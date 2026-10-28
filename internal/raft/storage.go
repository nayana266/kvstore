package raft

import (
	pb "kvstore/proto/raftpb"
)

// Storage holds the persistent state from the "State" box in Figure 2:
// currentTerm, votedFor, and log[]. Every method must not return until the
// change is on stable storage, because Raft replies to RPCs right after.
type Storage interface {
	// SaveState records currentTerm and votedFor ("" means no vote).
	SaveState(term uint64, votedFor string) error
	// Append adds entries to the end of the stored log.
	Append(entries []*pb.LogEntry) error
	// TruncateFrom deletes every stored entry with Index >= index.
	TruncateFrom(index uint64) error
	// Load returns everything saved so far. A brand-new storage returns
	// term 0, no vote, and an empty log.
	Load() (term uint64, votedFor string, log []*pb.LogEntry, err error)
}
