// Package storage keeps Raft's persistent state on disk.
package storage

import (
	"errors"

	pb "kvstore/proto/raftpb"
)

const (
	stateFile = "state" // currentTerm and votedFor
	logFile   = "log"   // append-only log entries
)

var errNotImplemented = errors.New("storage: not implemented")

// FileStorage stores Raft's persistent state in a directory:
//   - "state" holds currentTerm and votedFor. It is replaced atomically:
//     write a temp file, fsync it, then rename it over the old one.
//   - "log" holds length-prefixed protobuf LogEntry records, fsynced on every
//     append. On Open, a torn record at the end (from a crash mid-write) is
//     detected and discarded.
//
// It satisfies raft.Storage.
type FileStorage struct {
	dir string
	// TODO(Phase 3): open file handles and anything else you need.
}

// Open opens (or creates) the storage in dir.
func Open(dir string) (*FileStorage, error) {
	// TODO(Phase 3)
	return nil, errNotImplemented
}

// SaveState durably records currentTerm and votedFor.
func (s *FileStorage) SaveState(term uint64, votedFor string) error {
	// TODO(Phase 3)
	return errNotImplemented
}

// Append durably adds entries to the end of the log.
func (s *FileStorage) Append(entries []*pb.LogEntry) error {
	// TODO(Phase 3)
	return errNotImplemented
}

// TruncateFrom durably deletes every entry with Index >= index.
func (s *FileStorage) TruncateFrom(index uint64) error {
	// TODO(Phase 3)
	return errNotImplemented
}

// Load returns the saved term, vote, and log.
func (s *FileStorage) Load() (uint64, string, []*pb.LogEntry, error) {
	// TODO(Phase 3)
	return 0, "", nil, errNotImplemented
}

// Close releases open files.
func (s *FileStorage) Close() error {
	// TODO(Phase 3)
	return nil
}
