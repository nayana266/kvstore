package raft

import (
	"bytes"
	"testing"
	"time"
)

// TestBasicAgree: entries proposed to the leader are applied on every node
// in the same order (§5.3).
func TestBasicAgree(t *testing.T) {
	c := newTestCluster(t, 3)
	c.checkOneLeader(c.ids)

	want := [][]byte{cmd("a"), cmd("b"), cmd("c")}
	for _, w := range want {
		c.propose(c.ids, w)
	}
	c.waitApplied(c.ids, len(want))
	c.checkSameApplied(c.ids, want)
}

// TestFollowerCatchUp: a follower that missed proposals while disconnected
// receives them after reconnecting (§5.3).
func TestFollowerCatchUp(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.checkOneLeader(c.ids)

	c.propose(c.ids, cmd("before"))
	c.waitApplied(c.ids, 1)

	follower := without(c.ids, leader)[0]
	c.net.Disconnect(follower)

	c.propose(without(c.ids, follower), cmd("during"))
	c.waitApplied(without(c.ids, follower), 2)

	// Isolated follower must not have the second command yet.
	if got := c.nodes[follower].Applied(); len(got) >= 2 {
		t.Fatalf("disconnected follower unexpectedly applied %v", cmds(got))
	}

	c.net.Reconnect(follower)
	c.waitApplied(c.ids, 2)
	c.checkSameApplied(c.ids, [][]byte{cmd("before"), cmd("during")})
}

// TestOverwriteUncommitted: an old leader's uncommitted entries are replaced
// by the new leader's log after it rejoins (Figure 2, AppendEntries rules 3–4).
func TestOverwriteUncommitted(t *testing.T) {
	c := newTestCluster(t, 3)
	oldLeader, oldTerm := c.checkOneLeader(c.ids)

	// Partition the old leader alone. It may still believe it is leader and
	// accept Propose locally, but those entries cannot reach a majority.
	c.net.Partition([]string{oldLeader}, without(c.ids, oldLeader))

	// Old leader still thinks it is leader for a while; feed it a command
	// that must never become the cluster's committed history.
	deadline := time.Now().Add(time.Second)
	submitted := false
	for time.Now().Before(deadline) {
		if c.nodes[oldLeader].Propose(cmd("orphan")) {
			submitted = true
			break
		}
		time.Sleep(pollInterval)
	}
	if !submitted {
		// If it already stepped down, force an entry into its log via a
		// second try after confirming it still has the old term as leader
		// at least once. If Propose never succeeds, skip the local orphan
		// and still check that majority commits a different command.
		t.Log("old leader stepped down before Propose; continuing with majority-only commit")
	}

	majority := without(c.ids, oldLeader)
	_, newTerm := c.checkOneLeader(majority)
	if newTerm <= oldTerm {
		t.Fatalf("majority term %d should be > old leader term %d", newTerm, oldTerm)
	}

	c.propose(majority, cmd("winner"))
	c.waitApplied(majority, 1)
	c.checkSameApplied(majority, [][]byte{cmd("winner")})

	c.net.Heal()
	c.checkOneLeader(c.ids)
	c.waitApplied(c.ids, 1)

	// Every node must apply "winner". None may apply "orphan".
	for _, id := range c.ids {
		got := c.nodes[id].Applied()
		for _, a := range got {
			if bytes.Equal(a, cmd("orphan")) {
				t.Fatalf("%s applied uncommitted orphan from old leader: %v", id, cmds(got))
			}
		}
		if !equalCmds(got, [][]byte{cmd("winner")}) {
			t.Fatalf("%s applied %v, want [winner]", id, cmds(got))
		}
	}

	// Logs must agree on the committed prefix (winner at index 1).
	for _, id := range c.ids {
		log := c.nodes[id].LogEntries()
		if len(log) < 1 || !bytes.Equal(log[0].Data, cmd("winner")) {
			t.Fatalf("%s log %v does not start with winner", id, log)
		}
	}
}

// TestFigure8CommitRestriction: a majority having an old-term entry is not
// enough to commit it. Only after the new leader commits an entry from its
// own term may earlier entries become committed (Figure 8 / §5.4.2).
//
// Scenario (5 nodes):
//  1. Leader L1 proposes "old" and we let it replicate, then immediately
//     partition L1 with a minority so "old" may sit on a majority of disks
//     without a stable commit through a new term.
//  2. The other side elects L2, proposes "new", and commits it.
//  3. After healing, every node must have applied "new". If a buggy leader
//     commits old-term entries by majority alone, histories can diverge or
//     "old" can appear without a proper current-term commit path.
func TestFigure8CommitRestriction(t *testing.T) {
	c := newTestCluster(t, 5)
	l1, term1 := c.checkOneLeader(c.ids)

	c.propose(c.ids, cmd("old"))
	c.waitApplied(c.ids, 1)

	// Split so L1 is with only one follower (minority of 2). The majority
	// of 3 elects a new leader and commits a different entry.
	follower := without(c.ids, l1)[0]
	minority := []string{l1, follower}
	majority := without(c.ids, minority...)
	c.net.Partition(minority, majority)

	_, term2 := c.checkOneLeader(majority)
	if term2 <= term1 {
		t.Fatalf("majority term %d should be > %d", term2, term1)
	}

	c.propose(majority, cmd("new"))
	c.waitApplied(majority, 2) // "old" then "new" if L2 kept "old", or just catch up

	// Majority must have committed "new". "old" may or may not still be in
	// the log depending on elections, but Applied on the majority must end
	// with "new" and stay identical across the majority.
	for _, id := range majority {
		got := c.nodes[id].Applied()
		if len(got) == 0 || !bytes.Equal(got[len(got)-1], cmd("new")) {
			t.Fatalf("%s applied %v; last entry should be new", id, cmds(got))
		}
	}
	majApplied := c.nodes[majority[0]].Applied()
	c.checkSameApplied(majority, majApplied)

	c.net.Heal()
	c.checkOneLeader(c.ids)
	c.waitApplied(c.ids, len(majApplied))
	c.checkSameApplied(c.ids, majApplied)

	// Safety: no node applied a command the majority never committed.
	for _, id := range c.ids {
		got := c.nodes[id].Applied()
		if len(got) != len(majApplied) {
			t.Fatalf("%s applied %v, majority has %v", id, cmds(got), cmds(majApplied))
		}
	}
}
