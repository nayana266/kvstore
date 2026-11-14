package raft

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// startJoiner adds a non-voting node that waits for AddServer (§4.1).
func (c *testCluster) startJoiner(id string) {
	c.t.Helper()
	c.mu.Lock()
	c.ids = append(c.ids, id)
	c.storages[id] = NewMemStorage()
	node := NewNode(Config{
		ID:        id,
		Join:      true,
		Transport: c.net.Transport(id),
		Storage:   c.storages[id],
	})
	c.nodes[id] = node
	c.crashed[id] = false
	c.mu.Unlock()

	c.net.Register(id, node)
	node.Start()
}

func (c *testCluster) waitVoters(ids []string, want []string) {
	c.t.Helper()
	sortedWant := append([]string(nil), want...)
	slices.Sort(sortedWant)
	deadline := time.Now().Add(applyWait)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			got := c.node(id).Voters()
			slices.Sort(got)
			if !slices.Equal(got, sortedWant) {
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
		c.t.Logf("%s voters=%v", id, c.node(id).Voters())
	}
	c.t.Fatalf("timed out waiting for voters %v on %v", want, ids)
}

func TestAddServerGrow3to4(t *testing.T) {
	c := newTestCluster(t, 3)
	base := []string{"n1", "n2", "n3"}
	c.checkOneLeader(base)

	c.startJoiner("n4")
	leader, _ := c.checkOneLeader(base)
	if st := c.node(leader).AddServer("n4"); st != MembershipOK {
		t.Fatalf("AddServer: %v", st)
	}
	c.waitVoters([]string{"n1", "n2", "n3", "n4"}, []string{"n1", "n2", "n3", "n4"})

	c.propose([]string{"n1", "n2", "n3", "n4"}, cmd("after-add"))
	c.waitApplied([]string{"n1", "n2", "n3", "n4"}, 1)
	c.checkSameApplied([]string{"n1", "n2", "n3", "n4"}, [][]byte{cmd("after-add")})
}

func TestAddServerUnderLoad(t *testing.T) {
	c := newTestCluster(t, 3)
	base := []string{"n1", "n2", "n3"}
	c.checkOneLeader(base)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				c.propose(base, cmd(fmt.Sprintf("load-%d", i)))
				i++
				time.Sleep(30 * time.Millisecond)
			}
		}
	}()

	c.startJoiner("n4")
	c.startJoiner("n5")
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range []string{"n4", "n5"} {
		for {
			leader, _ := c.checkOneLeader(base)
			st := c.node(leader).AddServer(id)
			if st == MembershipOK {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("AddServer(%s): %v", id, st)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	close(stop)
	<-done

	c.waitVoters([]string{"n1", "n2", "n3", "n4", "n5"}, []string{"n1", "n2", "n3", "n4", "n5"})
}

func TestRemoveServerShrink(t *testing.T) {
	c := newTestCluster(t, 3)
	base := []string{"n1", "n2", "n3"}
	c.checkOneLeader(base)
	c.startJoiner("n4")
	c.startJoiner("n5")
	for _, id := range []string{"n4", "n5"} {
		leader, _ := c.checkOneLeader(base)
		if st := c.node(leader).AddServer(id); st != MembershipOK {
			t.Fatalf("AddServer(%s): %v", id, st)
		}
	}
	c.waitVoters([]string{"n1", "n2", "n3", "n4", "n5"}, []string{"n1", "n2", "n3", "n4", "n5"})

	all := []string{"n1", "n2", "n3", "n4", "n5"}
	leader, _ := c.checkOneLeader(all)
	remove := "n5"
	if leader == remove {
		remove = "n4"
	}
	if st := c.node(leader).RemoveServer(remove); st != MembershipOK {
		t.Fatalf("RemoveServer: %v", st)
	}
	remaining := without(all, remove)
	c.waitVoters(remaining, remaining)

	c.propose(remaining, cmd("after-remove"))
	c.waitApplied(remaining, 1)
}

func TestRemoveLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	base := []string{"n1", "n2", "n3"}
	c.checkOneLeader(base)
	c.startJoiner("n4")
	leader, _ := c.checkOneLeader(base)
	if st := c.node(leader).AddServer("n4"); st != MembershipOK {
		t.Fatalf("AddServer: %v", st)
	}
	c.waitVoters([]string{"n1", "n2", "n3", "n4"}, []string{"n1", "n2", "n3", "n4"})

	leader, _ = c.checkOneLeader([]string{"n1", "n2", "n3", "n4"})
	if st := c.node(leader).RemoveServer(leader); st != MembershipOK {
		t.Fatalf("RemoveServer(leader): %v", st)
	}
	remaining := without([]string{"n1", "n2", "n3", "n4"}, leader)
	c.waitVoters(remaining, remaining)

	// Old leader must not stay leader; a new one among remaining.
	deadline := time.Now().Add(leaderWait)
	for time.Now().Before(deadline) {
		_, isLeader := c.node(leader).State()
		if !isLeader {
			break
		}
		time.Sleep(pollInterval)
	}
	if _, isLeader := c.node(leader).State(); isLeader {
		t.Fatalf("removed leader %s still claims leadership", leader)
	}
	c.checkOneLeader(remaining)
	c.propose(remaining, cmd("post-leader-remove"))
	c.waitApplied(remaining, 1)
}

func TestAddOfflineThenOnline(t *testing.T) {
	c := newTestCluster(t, 3)
	base := []string{"n1", "n2", "n3"}
	c.checkOneLeader(base)

	// Node stays registered but disconnected the whole catch-up window first.
	c.startJoiner("n4")
	c.net.Disconnect("n4")

	leader, _ := c.checkOneLeader(base)
	st := c.node(leader).AddServer("n4")
	if st != MembershipTimeout {
		t.Fatalf("AddServer while offline: got %v, want Timeout", st)
	}

	c.net.Reconnect("n4")
	deadline := time.Now().Add(10 * time.Second)
	for {
		leader, _ = c.checkOneLeader(base)
		st = c.node(leader).AddServer("n4")
		if st == MembershipOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("AddServer after reconnect: %v", st)
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.waitVoters([]string{"n1", "n2", "n3", "n4"}, []string{"n1", "n2", "n3", "n4"})
	c.propose([]string{"n1", "n2", "n3", "n4"}, cmd("caught-up"))
	c.waitApplied([]string{"n1", "n2", "n3", "n4"}, 1)
}

func TestConfigChangeInProgress(t *testing.T) {
	c := newTestCluster(t, 3)
	base := []string{"n1", "n2", "n3"}
	leader, _ := c.checkOneLeader(base)
	c.startJoiner("n4")

	if st := c.node(leader).AddServer("n4"); st != MembershipOK {
		t.Fatalf("AddServer n4: %v", st)
	}
	follower := without(base, leader)[0]
	if st := c.node(follower).RemoveServer("n3"); st != MembershipNotLeader {
		t.Fatalf("follower RemoveServer: got %v, want NotLeader", st)
	}
}
