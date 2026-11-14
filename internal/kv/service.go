package kv

import (
	"context"
	"sync"
	"time"

	"kvstore/internal/raft"
	pb "kvstore/proto/kvpb"
)

// addrOf maps Raft node id → client-facing gRPC address (for leader hints).
type addrOf map[string]string

// Service is the gRPC KV API backed by Raft + Store.
type Service struct {
	pb.UnimplementedKVServer

	raft    *raft.Node
	store   *Store
	applyCh <-chan raft.ApplyMsg
	addrs   addrOf

	mu      sync.Mutex
	waiters map[uint64]chan ApplyResult // index → waiter
	done    map[uint64]ApplyResult      // already-applied results (race with Propose)
}

// NewService wires Raft apply notifications into the store.
// applyCh must be the same channel passed to raft.Config.ApplyCh.
func NewService(n *raft.Node, store *Store, applyCh <-chan raft.ApplyMsg, addrs map[string]string) *Service {
	s := &Service{
		raft:    n,
		store:   store,
		applyCh: applyCh,
		addrs:   addrs,
		waiters: map[uint64]chan ApplyResult{},
		done:    map[uint64]ApplyResult{},
	}
	go s.applyLoop()
	return s
}

// applyLoop reads committed commands and applies them to the store.
func (s *Service) applyLoop() {
	for msg := range s.applyCh {
		cmd, err := Decode(msg.Data)
		var res ApplyResult
		if err == nil {
			res = s.store.Apply(cmd)
		}
		s.mu.Lock()
		s.done[msg.Index] = res
		if ch, ok := s.waiters[msg.Index]; ok {
			ch <- res
			delete(s.waiters, msg.Index)
		}
		// Bound memory: drop old results (waiters for ancient indexes already timed out).
		for i := range s.done {
			if i+256 < msg.Index {
				delete(s.done, i)
			}
		}
		s.mu.Unlock()
	}
}

func (s *Service) leaderHint() string {
	id := s.raft.LeaderID()
	if id == "" {
		return ""
	}
	return s.addrs[id]
}

// proposeAndWait appends cmd and waits until that index is applied (or timeout).
func (s *Service) proposeAndWait(ctx context.Context, cmd Command) (ApplyResult, pb.Status) {
	data, err := Encode(cmd)
	if err != nil {
		return ApplyResult{}, pb.Status_STATUS_TIMEOUT
	}
	index, ok := s.raft.Propose(data)
	if !ok {
		return ApplyResult{}, pb.Status_STATUS_NOT_LEADER
	}

	s.mu.Lock()
	if res, ready := s.done[index]; ready {
		s.mu.Unlock()
		return res, pb.Status_STATUS_OK
	}
	ch := make(chan ApplyResult, 1)
	s.waiters[index] = ch
	s.mu.Unlock()

	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		return res, pb.Status_STATUS_OK
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return ApplyResult{}, pb.Status_STATUS_TIMEOUT
	case <-timer.C:
		s.mu.Lock()
		delete(s.waiters, index)
		// Apply may have landed during delete; check once more.
		if res, ready := s.done[index]; ready {
			s.mu.Unlock()
			return res, pb.Status_STATUS_OK
		}
		s.mu.Unlock()
		return ApplyResult{}, pb.Status_STATUS_TIMEOUT
	}
}

func (s *Service) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	_, st := s.proposeAndWait(ctx, Command{
		Type:     OpPut,
		Key:      req.Key,
		Value:    req.Value,
		ClientID: req.ClientId,
		Seq:      req.Seq,
	})
	return &pb.PutResponse{Status: st, LeaderHint: s.leaderHint()}, nil
}

func (s *Service) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	res, st := s.proposeAndWait(ctx, Command{Type: OpGet, Key: req.Key})
	resp := &pb.GetResponse{Status: st, LeaderHint: s.leaderHint()}
	if st == pb.Status_STATUS_OK {
		resp.Found = res.Found
		resp.Value = res.Value
	}
	return resp, nil
}

func (s *Service) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	_, st := s.proposeAndWait(ctx, Command{
		Type:     OpDelete,
		Key:      req.Key,
		ClientID: req.ClientId,
		Seq:      req.Seq,
	})
	return &pb.DeleteResponse{Status: st, LeaderHint: s.leaderHint()}, nil
}

func (s *Service) AddServer(ctx context.Context, req *pb.AddServerRequest) (*pb.AddServerResponse, error) {
	_ = ctx
	_ = req
	return &pb.AddServerResponse{Status: pb.Status_STATUS_NOT_LEADER, LeaderHint: s.leaderHint()}, nil
}

func (s *Service) RemoveServer(ctx context.Context, req *pb.RemoveServerRequest) (*pb.RemoveServerResponse, error) {
	_ = ctx
	_ = req
	return &pb.RemoveServerResponse{Status: pb.Status_STATUS_NOT_LEADER, LeaderHint: s.leaderHint()}, nil
}

const waitTimeout = 2 * time.Second
