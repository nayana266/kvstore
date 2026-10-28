package raft

import (
	"bytes"
	"fmt"
	"testing"

	pb "kvstore/proto/raftpb"
)

// newUnstartedNode builds a node whose handlers can be called directly. It is
// never started, so no timers or RPCs run.
func newUnstartedNode(id string, st Storage) *Node {
	return NewNode(Config{
		ID:        id,
		Peers:     without([]string{"n1", "n2", "n3"}, id),
		Transport: NewMemNetwork().Transport(id),
		Storage:   st,
	})
}

func entry(index, term uint64, data string) *pb.LogEntry {
	return &pb.LogEntry{Index: index, Term: term, Type: pb.EntryType_ENTRY_TYPE_NORMAL, Data: []byte(data)}
}

// TestVoteSavedBeforeReply: a granted vote must be on storage by the time the
// reply is returned, and a restarted node must not vote again in that term
// (Figure 2, "Persistent state ... updated on stable storage before
// responding to RPCs").
func TestVoteSavedBeforeReply(t *testing.T) {
	st := NewMemStorage()
	n := newUnstartedNode("n1", st)

	resp := n.HandleRequestVote(&pb.RequestVoteRequest{Term: 5, CandidateId: "n2"})
	if !resp.VoteGranted {
		t.Fatalf("n1 should vote for n2 in term 5")
	}
	term, votedFor, _, _ := st.Load()
	if term != 5 || votedFor != "n2" {
		t.Fatalf("after replying, storage has term=%d votedFor=%q; want 5, \"n2\"", term, votedFor)
	}

	restarted := newUnstartedNode("n1", st.Copy())
	if got, _ := restarted.State(); got != 5 {
		t.Fatalf("restarted node has term %d; want 5", got)
	}
	resp = restarted.HandleRequestVote(&pb.RequestVoteRequest{Term: 5, CandidateId: "n3"})
	if resp.VoteGranted {
		t.Fatalf("restarted node voted for n3 in term 5 after already voting for n2")
	}
}

// TestLogSavedBeforeReply: entries accepted from AppendEntries, including a
// truncated conflict, must survive a restart (Figure 2, AppendEntries rules 3–4).
func TestLogSavedBeforeReply(t *testing.T) {
	st := NewMemStorage()
	n := newUnstartedNode("n1", st)

	resp := n.HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:     1,
		LeaderId: "n2",
		Entries:  []*pb.LogEntry{entry(1, 1, "a"), entry(2, 1, "b")},
	})
	if !resp.Success {
		t.Fatalf("first AppendEntries should succeed")
	}
	if _, _, log, _ := st.Load(); len(log) != 2 {
		t.Fatalf("after replying, storage has %d entries; want 2", len(log))
	}

	// A leader in term 2 replaces index 2.
	resp = n.HandleAppendEntries(&pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "n3",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*pb.LogEntry{entry(2, 2, "c")},
	})
	if !resp.Success {
		t.Fatalf("second AppendEntries should succeed")
	}

	restarted := newUnstartedNode("n1", st.Copy())
	if got, _ := restarted.State(); got != 2 {
		t.Fatalf("restarted node has term %d; want 2", got)
	}
	want := []LogEntryView{{1, 1, []byte("a")}, {2, 2, []byte("c")}}
	got := restarted.LogEntries()
	if len(got) != len(want) {
		t.Fatalf("restarted log %v; want %v", got, want)
	}
	for i := range want {
		if got[i].Index != want[i].Index || got[i].Term != want[i].Term || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("restarted log %v; want %v", got, want)
		}
	}
}

// TestRestartAll: every node crashes and restarts. Terms never go backwards,
// and all committed commands are still there.
func TestRestartAll(t *testing.T) {
	c := newTestCluster(t, 3)
	c.checkOneLeader(c.ids)
	for _, s := range []string{"a", "b", "c"} {
		c.propose(c.ids, cmd(s))
	}
	c.waitApplied(c.ids, 3)

	before := map[string]uint64{}
	for _, id := range c.ids {
		before[id], _ = c.node(id).State()
	}
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	for _, id := range c.ids {
		if term, _ := c.node(id).State(); term < before[id] {
			t.Errorf("%s restarted with term %d; it had term %d before crashing", id, term, before[id])
		}
	}

	// Old-term entries only commit once the new leader commits one of its own
	// (Figure 8), so propose one more.
	c.propose(c.ids, cmd("d"))
	c.waitApplied(c.ids, 4)
	c.checkSameApplied(c.ids, [][]byte{cmd("a"), cmd("b"), cmd("c"), cmd("d")})
}

// TestCommittedEntrySurvivesCrash: an entry committed while one follower was
// down must survive even after the leader dies. The follower that missed it
// can't win an election (§5.4.1), so the node holding it on disk must.
func TestCommittedEntrySurvivesCrash(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.checkOneLeader(c.ids)
	followers := without(c.ids, leader)
	behind, ahead := followers[0], followers[1]

	c.propose(c.ids, cmd("a"))
	c.waitApplied(c.ids, 1)

	c.crash(behind)
	c.propose([]string{leader, ahead}, cmd("b"))
	c.waitApplied([]string{leader, ahead}, 2)

	// Only leader and ahead have "b". Both die; ahead and behind come back.
	c.crash(leader)
	c.crash(ahead)
	c.restart(behind)
	c.restart(ahead)

	survivors := []string{behind, ahead}
	c.propose(survivors, cmd("c"))
	c.waitApplied(survivors, 3)
	c.checkSameApplied(survivors, [][]byte{cmd("a"), cmd("b"), cmd("c")})
}

// TestFollowerRestartRejoins: a follower crashes partway through a stream of
// proposals, restarts, and ends up with the same history as everyone else.
func TestFollowerRestartRejoins(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.checkOneLeader(c.ids)
	follower := without(c.ids, leader)[0]

	for i := 0; i < 5; i++ {
		c.propose(c.ids, cmd(fmt.Sprintf("x%d", i)))
		if i == 2 {
			c.crash(follower)
		}
	}
	c.restart(follower)
	c.propose(c.ids, cmd("after"))
	c.waitApplied(c.ids, 6)

	want := c.node(c.ids[0]).Applied()
	c.checkSameApplied(c.ids, want)
}
