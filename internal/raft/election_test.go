package raft

import (
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newTestCluster(t, 3)

	leader, term := c.checkOneLeader(c.ids)
	if term < 1 {
		t.Fatalf("leader %s has term %d; the first election should use term 1 or higher", leader, term)
	}

	// With no failures, the leader's heartbeats should prevent new elections.
	time.Sleep(time.Second)
	leader2, term2 := c.checkOneLeader(c.ids)
	if leader2 != leader || term2 != term {
		t.Errorf("leadership changed with no failures: %s (term %d) -> %s (term %d)", leader, term, leader2, term2)
	}
}

func TestReElection(t *testing.T) {
	c := newTestCluster(t, 3)

	// The leader fails; the other two must elect a new one in a higher term.
	leader1, term1 := c.checkOneLeader(c.ids)
	c.net.Disconnect(leader1)
	_, term2 := c.checkOneLeader(without(c.ids, leader1))
	if term2 <= term1 {
		t.Errorf("new leader's term %d should be greater than old leader's term %d", term2, term1)
	}

	// The old leader comes back. It must step down when it sees the higher term.
	c.net.Reconnect(leader1)
	leader3, _ := c.checkOneLeader(c.ids)

	// Two of three nodes fail. The survivor has no majority and can't become leader.
	other := without(c.ids, leader3)[0]
	survivor := without(c.ids, leader3, other)
	c.net.Disconnect(leader3)
	c.net.Disconnect(other)
	c.checkNoLeader(survivor, time.Second)

	// One node returns, restoring a majority of two.
	c.net.Reconnect(other)
	c.checkOneLeader(append(survivor, other))

	// Everyone is back.
	c.net.Reconnect(leader3)
	c.checkOneLeader(c.ids)
}

func TestMinorityPartition(t *testing.T) {
	c := newTestCluster(t, 5)

	// Two followers are cut off. They can't get 3 votes, so they never elect
	// a leader, while the majority side keeps one.
	leader1, _ := c.checkOneLeader(c.ids)
	followers := without(c.ids, leader1)
	minority := followers[:2]
	c.net.Partition(minority, without(c.ids, minority...))
	c.checkNoLeader(minority, time.Second)
	c.checkOneLeader(without(c.ids, minority...))

	c.net.Heal()
	leader2, term2 := c.checkOneLeader(c.ids)

	// The leader is cut off with one follower. The other three elect a new
	// leader in a higher term; the follower stuck with the old leader can't.
	follower := without(c.ids, leader2)[0]
	minority = []string{leader2, follower}
	majority := without(c.ids, minority...)
	c.net.Partition(minority, majority)
	_, term3 := c.checkOneLeader(majority)
	if term3 <= term2 {
		t.Errorf("majority's new leader has term %d; should be greater than old leader's term %d", term3, term2)
	}
	c.checkNoLeader([]string{follower}, time.Second)

	// After healing, the old leader must step down, leaving exactly one leader.
	c.net.Heal()
	c.checkOneLeader(c.ids)
}
