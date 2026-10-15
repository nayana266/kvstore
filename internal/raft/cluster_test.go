package raft

import (
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
