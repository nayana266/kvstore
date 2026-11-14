package kv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"kvstore/internal/raft"
	pb "kvstore/proto/kvpb"
)

// startKVCluster runs n Raft nodes with KV services on an in-memory network.
func startKVCluster(t *testing.T, n int) (services []*Service, cleanup func()) {
	t.Helper()
	net := raft.NewMemNetwork()
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	addrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = id // hints are ids in this test
	}

	nodes := make([]*raft.Node, n)
	services = make([]*Service, n)
	for i, id := range ids {
		peers := append([]string{}, ids[:i]...)
		peers = append(peers, ids[i+1:]...)
		applyCh := make(chan raft.ApplyMsg, 10_000)
		node := raft.NewNode(raft.Config{
			ID:        id,
			Peers:     peers,
			Transport: net.Transport(id),
			Storage:   raft.NewMemStorage(),
			ApplyCh:   applyCh,
		})
		net.Register(id, node)
		store := NewStore()
		services[i] = NewService(node, store, applyCh, addrs)
		nodes[i] = node
	}
	for _, node := range nodes {
		node.Start()
	}
	cleanup = func() {
		for _, node := range nodes {
			node.Stop()
		}
	}
	return services, cleanup
}

func waitLeader(t *testing.T, services []*Service) *Service {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range services {
			if _, isLeader := s.raft.State(); isLeader {
				return s
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func TestServicePutGet(t *testing.T) {
	services, cleanup := startKVCluster(t, 3)
	defer cleanup()
	leader := waitLeader(t, services)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	put, err := leader.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("v"), ClientId: "c", Seq: 1})
	if err != nil {
		t.Fatal(err)
	}
	if put.Status != pb.Status_STATUS_OK {
		t.Fatalf("Put status %v hint %q", put.Status, put.LeaderHint)
	}

	get, err := leader.Get(ctx, &pb.GetRequest{Key: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if get.Status != pb.Status_STATUS_OK || !get.Found || string(get.Value) != "v" {
		t.Fatalf("Get status=%v found=%v value=%q", get.Status, get.Found, get.Value)
	}
}

func TestServiceNotLeaderRedirect(t *testing.T) {
	services, cleanup := startKVCluster(t, 3)
	defer cleanup()
	leader := waitLeader(t, services)

	var follower *Service
	for _, s := range services {
		if s != leader {
			follower = s
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Wait until a heartbeat has taught this follower who the leader is.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := follower.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("v"), ClientId: "c", Seq: 1})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Status == pb.Status_STATUS_NOT_LEADER && resp.LeaderHint != "" {
			return
		}
		if resp.Status == pb.Status_STATUS_OK {
			t.Fatal("follower unexpectedly accepted Put as leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for NOT_LEADER with leader hint")
}
