package raft

import (
	"sync"

	"google.golang.org/protobuf/proto"

	pb "kvstore/proto/raftpb"
)

// MemStorage is an in-memory Storage for tests. Copy simulates a crash: the
// restarted node gets exactly what was saved at that moment, and anything the
// old node writes afterward is lost.
type MemStorage struct {
	mu       sync.Mutex
	term     uint64
	votedFor string
	log      []*pb.LogEntry
}

var _ Storage = (*MemStorage)(nil)

func NewMemStorage() *MemStorage {
	return &MemStorage{}
}

func (s *MemStorage) SaveState(term uint64, votedFor string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.votedFor = votedFor
	return nil
}

func (s *MemStorage) Append(entries []*pb.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		s.log = append(s.log, proto.Clone(e).(*pb.LogEntry))
	}
	return nil
}

func (s *MemStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.log[:0]
	for _, e := range s.log {
		if e.Index < index {
			kept = append(kept, e)
		}
	}
	s.log = kept
	return nil
}

func (s *MemStorage) Load() (uint64, string, []*pb.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.votedFor, cloneEntries(s.log), nil
}

// Copy returns an independent snapshot of what has been saved.
func (s *MemStorage) Copy() *MemStorage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &MemStorage{term: s.term, votedFor: s.votedFor, log: cloneEntries(s.log)}
}

func cloneEntries(in []*pb.LogEntry) []*pb.LogEntry {
	out := make([]*pb.LogEntry, len(in))
	for i, e := range in {
		out[i] = proto.Clone(e).(*pb.LogEntry)
	}
	return out
}
