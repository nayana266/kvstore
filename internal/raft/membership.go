package raft

import (
	"encoding/json"
	"slices"
	"time"

	pb "kvstore/proto/raftpb"
)

// minElectionTimeout is the lower bound of our randomized election timeout.
// Dissertation §4.2.3: ignore RequestVote if a current leader was heard from
// within this window (keeps removed servers from disrupting elections).
const minElectionTimeout = 150 * time.Millisecond

// MembershipStatus is the result of AddServer / RemoveServer.
type MembershipStatus int

const (
	MembershipOK MembershipStatus = iota
	MembershipNotLeader
	MembershipInProgress // another config change not yet committed (§4.1)
	MembershipTimeout
)

// clusterConfig is the payload of an ENTRY_TYPE_CONFIG log entry (§4.1).
// Learners are leader-volatile only until promoted via a config entry.
type clusterConfig struct {
	Voters []string `json:"voters"`
}

func encodeConfig(voters []string) []byte {
	v := append([]string(nil), voters...)
	slices.Sort(v)
	b, err := json.Marshal(clusterConfig{Voters: v})
	if err != nil {
		panic("raft: encode config: " + err.Error())
	}
	return b
}

func decodeConfig(data []byte) (clusterConfig, error) {
	var c clusterConfig
	err := json.Unmarshal(data, &c)
	return c, err
}

// latestConfigFromLog returns the newest CONFIG entry's voter set, or nil if
// the log has none. Dissertation §4.1: use the latest config in the log
// whether or not it is committed. Caller must hold n.mu.
func (n *Node) latestConfigFromLog() []string {
	for i := len(n.log) - 1; i >= 0; i-- {
		e := n.log[i]
		if e.Type != pb.EntryType_ENTRY_TYPE_CONFIG {
			continue
		}
		c, err := decodeConfig(e.Data)
		if err != nil {
			continue
		}
		return append([]string(nil), c.Voters...)
	}
	return nil
}

// adoptLatestConfig sets n.voters from the log (or bootstrap). Caller holds n.mu.
func (n *Node) adoptLatestConfig() {
	if v := n.latestConfigFromLog(); v != nil {
		n.voters = v
	} else {
		n.voters = append([]string(nil), n.bootstrap...)
	}
	// Drop learners that are now voters or no longer relevant.
	n.learners = slices.DeleteFunc(n.learners, func(id string) bool {
		return n.isVoter(id)
	})
}

// isVoter reports whether id is in the current configuration. Caller holds n.mu.
func (n *Node) isVoter(id string) bool {
	return slices.Contains(n.voters, id)
}

// majority is floor(len(voters)/2)+1. Caller holds n.mu.
func (n *Node) majority() int {
	return len(n.voters)/2 + 1
}

// otherVoters returns voting members except self. Caller holds n.mu.
func (n *Node) otherVoters() []string {
	var out []string
	for _, id := range n.voters {
		if id != n.id {
			out = append(out, id)
		}
	}
	return out
}

// replicationTargets is everyone the leader should send AppendEntries to:
// other voters plus non-voting learners (§4.1). Caller holds n.mu.
func (n *Node) replicationTargets() []string {
	seen := map[string]bool{n.id: true}
	var out []string
	for _, id := range n.voters {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, id := range n.learners {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// hasUncommittedConfig is true if the newest CONFIG entry is not committed.
// Only one membership change may be in flight (§4.1). Caller holds n.mu.
func (n *Node) hasUncommittedConfig() bool {
	for i := len(n.log) - 1; i >= 0; i-- {
		e := n.log[i]
		if e.Type != pb.EntryType_ENTRY_TYPE_CONFIG {
			continue
		}
		return e.Index > n.commitIndex
	}
	return false
}

// ensurePeerProgress initializes nextIndex/matchIndex for a peer. Caller holds n.mu.
func (n *Node) ensurePeerProgress(peer string) {
	if n.nextIndex == nil {
		n.nextIndex = make(map[string]uint64)
		n.matchIndex = make(map[string]uint64)
	}
	if _, ok := n.nextIndex[peer]; !ok {
		lastIndex, _ := n.lastLogInfo()
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}
}

// appendConfigEntry writes a CONFIG entry and adopts it immediately (§4.1).
// Caller must be leader and hold n.mu. Returns the new entry's index.
func (n *Node) appendConfigEntry(voters []string) uint64 {
	lastIndex, _ := n.lastLogInfo()
	index := lastIndex + 1
	entry := &pb.LogEntry{
		Term:  n.currentTerm,
		Index: index,
		Type:  pb.EntryType_ENTRY_TYPE_CONFIG,
		Data:  encodeConfig(voters),
	}
	n.log = append(n.log, entry)
	n.persistAppend(entry)
	n.adoptLatestConfig()
	for _, peer := range n.replicationTargets() {
		n.ensurePeerProgress(peer)
	}
	n.sendHeartbeats()
	return index
}

// waitForCommit blocks until commitIndex >= index or timeout. Must NOT hold n.mu.
func (n *Node) waitForCommit(index uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		ok := n.commitIndex >= index
		// Leader that removed itself steps down once Cnew commits (§4.1).
		if ok && n.role == Leader && !n.isVoter(n.id) {
			n.role = Follower
			n.resetElectionTimer()
		}
		n.mu.Unlock()
		if ok {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// AddServer adds id as a non-voting learner, waits until it has caught up, then
// appends a config making it a voter (dissertation §4.1).
func (n *Node) AddServer(id string) MembershipStatus {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return MembershipNotLeader
	}
	if id == "" || id == n.id {
		n.mu.Unlock()
		return MembershipOK
	}
	if n.isVoter(id) {
		n.mu.Unlock()
		return MembershipOK
	}
	if n.hasUncommittedConfig() {
		n.mu.Unlock()
		return MembershipInProgress
	}
	if !slices.Contains(n.learners, id) {
		n.learners = append(n.learners, id)
	}
	n.ensurePeerProgress(id)
	target, _ := n.lastLogInfo() // catch up to this index; later entries replicate as a voter
	n.sendAppendEntries(id)
	n.mu.Unlock()

	// Dissertation §4.1: wait until the new server is caught up before promoting.
	catchDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(catchDeadline) {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return MembershipNotLeader
		}
		// Require at least one successful AppendEntries so an unreachable
		// server is not promoted (even when the leader log is still empty).
		caught := n.aeOK[id] && (target == 0 || n.matchIndex[id] >= target)
		if caught {
			if n.hasUncommittedConfig() {
				n.mu.Unlock()
				return MembershipInProgress
			}
			if n.isVoter(id) {
				n.mu.Unlock()
				return MembershipOK
			}
			newVoters := append(append([]string(nil), n.voters...), id)
			index := n.appendConfigEntry(newVoters)
			n.mu.Unlock()
			if !n.waitForCommit(index, 5*time.Second) {
				return MembershipTimeout
			}
			return MembershipOK
		}
		n.sendAppendEntries(id)
		n.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	return MembershipTimeout
}

// RemoveServer appends a config without id. If the leader removes itself, it
// steps down after the entry commits (§4.1).
func (n *Node) RemoveServer(id string) MembershipStatus {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return MembershipNotLeader
	}
	if n.hasUncommittedConfig() {
		n.mu.Unlock()
		return MembershipInProgress
	}
	if !n.isVoter(id) {
		// Also drop learner if present.
		n.learners = slices.DeleteFunc(n.learners, func(x string) bool { return x == id })
		n.mu.Unlock()
		return MembershipOK
	}
	if len(n.voters) <= 1 {
		n.mu.Unlock()
		return MembershipOK // refuse to dissolve the last voter silently as OK no-op
	}
	newVoters := slices.DeleteFunc(append([]string(nil), n.voters...), func(x string) bool {
		return x == id
	})
	index := n.appendConfigEntry(newVoters)
	n.mu.Unlock()

	if !n.waitForCommit(index, 5*time.Second) {
		return MembershipTimeout
	}
	return MembershipOK
}

// Voters returns a copy of the current voter set (for tests).
func (n *Node) Voters() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.voters...)
}
