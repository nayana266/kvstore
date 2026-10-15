package raft

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

const (
	// leaderWait is how long to wait for an election to settle. Election
	// timeouts are 150–300ms, so this allows for a few split votes.
	leaderWait   = 2 * time.Second
	applyWait    = 3 * time.Second
	pollInterval = 10 * time.Millisecond
)

// testCluster runs n nodes on an in-memory network. While it runs, a
// background monitor fails the test if two nodes ever claim to be leader in
// the same term.
type testCluster struct {
	t     *testing.T
	net   *MemNetwork
	ids   []string
	nodes map[string]*Node

	stopMonitor chan struct{}
	monitorDone sync.WaitGroup
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{
		t:           t,
		net:         NewMemNetwork(),
		nodes:       map[string]*Node{},
		stopMonitor: make(chan struct{}),
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		node := NewNode(Config{
			ID:        id,
			Peers:     without(c.ids, id),
			Transport: c.net.Transport(id),
		})
		c.nodes[id] = node
		c.net.Register(id, node)
	}
	for _, id := range c.ids {
		c.nodes[id].Start()
	}

	c.monitorDone.Add(1)
	go c.monitorLeaders()

	t.Cleanup(func() {
		close(c.stopMonitor)
		c.monitorDone.Wait()
		for _, node := range c.nodes {
			node.Stop()
		}
	})
	return c
}

func (c *testCluster) monitorLeaders() {
	defer c.monitorDone.Done()
	leaderOf := map[uint64]string{}
	reported := map[uint64]bool{}
	ticker := time.NewTicker(pollInterval / 2)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopMonitor:
			return
		case <-ticker.C:
			for _, id := range c.ids {
				term, isLeader := c.nodes[id].State()
				if !isLeader {
					continue
				}
				prev, seen := leaderOf[term]
				if !seen {
					leaderOf[term] = id
				} else if prev != id && !reported[term] {
					reported[term] = true
					c.t.Errorf("two leaders in term %d: %s and %s", term, prev, id)
				}
			}
		}
	}
}

// checkOneLeader waits until exactly one of ids claims to be leader and
// returns it with its term.
func (c *testCluster) checkOneLeader(ids []string) (string, uint64) {
	c.t.Helper()
	var leaders []string
	deadline := time.Now().Add(leaderWait)
	for time.Now().Before(deadline) {
		leaders = leaders[:0]
		var term uint64
		for _, id := range ids {
			if t, isLeader := c.nodes[id].State(); isLeader {
				leaders = append(leaders, id)
				term = t
			}
		}
		if len(leaders) == 1 {
			return leaders[0], term
		}
		time.Sleep(pollInterval)
	}
	c.t.Fatalf("expected exactly one leader among %v within %v; last saw leaders %v", ids, leaderWait, leaders)
	return "", 0
}

// checkNoLeader fails if any of ids claims to be leader at any point during d.
func (c *testCluster) checkNoLeader(ids []string, d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			if term, isLeader := c.nodes[id].State(); isLeader {
				c.t.Fatalf("%s became leader in term %d, but %v should not be able to elect one", id, term, ids)
			}
		}
		time.Sleep(pollInterval)
	}
}

// without returns ids minus the excluded ones.
func without(ids []string, exclude ...string) []string {
	var out []string
	for _, id := range ids {
		if !slices.Contains(exclude, id) {
			out = append(out, id)
		}
	}
	return out
}

// propose asks the current leader among ids to append cmd, retrying across
// leader changes until someone accepts it.
func (c *testCluster) propose(ids []string, cmd []byte) {
	c.t.Helper()
	deadline := time.Now().Add(applyWait)
	for time.Now().Before(deadline) {
		leader, _ := c.checkOneLeader(ids)
		if c.nodes[leader].Propose(cmd) {
			return
		}
		time.Sleep(pollInterval)
	}
	c.t.Fatalf("could not propose %q to a leader among %v within %v", cmd, ids, applyWait)
}

// waitApplied waits until every id has at least n applied commands.
func (c *testCluster) waitApplied(ids []string, n int) {
	c.t.Helper()
	deadline := time.Now().Add(applyWait)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			if len(c.nodes[id].Applied()) < n {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(pollInterval)
	}
	for _, id := range ids {
		c.t.Logf("%s applied=%v commit=%d log=%v",
			id, cmds(c.nodes[id].Applied()), c.nodes[id].CommitIndex(), c.nodes[id].LogEntries())
	}
	c.t.Fatalf("timed out waiting for %d applied entries on %v", n, ids)
}

// checkSameApplied requires every id to have exactly the same applied list.
func (c *testCluster) checkSameApplied(ids []string, want [][]byte) {
	c.t.Helper()
	for _, id := range ids {
		got := c.nodes[id].Applied()
		if !equalCmds(got, want) {
			c.t.Fatalf("%s applied %v, want %v", id, cmds(got), cmds(want))
		}
	}
}

func equalCmds(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func cmds(in [][]byte) []string {
	out := make([]string, len(in))
	for i, c := range in {
		out[i] = string(c)
	}
	return out
}

func cmd(s string) []byte { return []byte(s) }
