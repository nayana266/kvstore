package storage

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "kvstore/proto/raftpb"
)

func ent(index, term uint64, data string) *pb.LogEntry {
	return &pb.LogEntry{Index: index, Term: term, Type: pb.EntryType_ENTRY_TYPE_NORMAL, Data: []byte(data)}
}

func open(t *testing.T, dir string) *FileStorage {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return s
}

func reopen(t *testing.T, s *FileStorage, dir string) *FileStorage {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return open(t, dir)
}

func load(t *testing.T, s *FileStorage) (uint64, string, []*pb.LogEntry) {
	t.Helper()
	term, votedFor, log, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return term, votedFor, log
}

func checkLog(t *testing.T, got []*pb.LogEntry, want ...*pb.LogEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("log has %d entries %v; want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("entry %d = %v; want %v", i, got[i], want[i])
		}
	}
}

func mustAppend(t *testing.T, s *FileStorage, entries ...*pb.LogEntry) {
	t.Helper()
	if err := s.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestEmptyStorage(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	defer s.Close()

	term, votedFor, log := load(t, s)
	if term != 0 || votedFor != "" || len(log) != 0 {
		t.Fatalf("new storage = term %d, vote %q, %d entries; want 0, \"\", 0", term, votedFor, len(log))
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if err := s.SaveState(1, "n1"); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := s.SaveState(3, "n2"); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	s = reopen(t, s, dir)
	defer s.Close()

	term, votedFor, _ := load(t, s)
	if term != 3 || votedFor != "n2" {
		t.Fatalf("after reopen: term %d, vote %q; want 3, \"n2\"", term, votedFor)
	}
}

func TestLogSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	mustAppend(t, s, ent(1, 1, "a"), ent(2, 1, "b"))
	mustAppend(t, s, ent(3, 2, "c"))
	s = reopen(t, s, dir)
	defer s.Close()

	_, _, log := load(t, s)
	checkLog(t, log, ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 2, "c"))
}

func TestTruncateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	mustAppend(t, s, ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c"), ent(4, 1, "d"))
	if err := s.TruncateFrom(3); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	mustAppend(t, s, ent(3, 2, "x"))
	s = reopen(t, s, dir)
	defer s.Close()

	_, _, log := load(t, s)
	checkLog(t, log, ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 2, "x"))
}

// TestTornRecordDiscarded simulates a crash halfway through writing the last
// record: the final bytes are missing.
func TestTornRecordDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	mustAppend(t, s, ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 1, "c"))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(dir, logFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat log file: %v", err)
	}
	if err := os.Truncate(path, info.Size()-2); err != nil {
		t.Fatalf("truncate log file: %v", err)
	}

	s = open(t, dir)
	_, _, log := load(t, s)
	checkLog(t, log, ent(1, 1, "a"), ent(2, 1, "b"))

	// The torn bytes must be gone, or the next record would be unreadable.
	mustAppend(t, s, ent(3, 2, "z"))
	s = reopen(t, s, dir)
	defer s.Close()
	_, _, log = load(t, s)
	checkLog(t, log, ent(1, 1, "a"), ent(2, 1, "b"), ent(3, 2, "z"))
}

// TestPartialHeaderDiscarded simulates a crash right after writing one byte
// of the next record's length prefix.
func TestPartialHeaderDiscarded(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	mustAppend(t, s, ent(1, 1, "a"), ent(2, 1, "b"))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(filepath.Join(dir, logFile), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open log file: %v", err)
	}
	if _, err := f.Write([]byte{0xFF}); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close log file: %v", err)
	}

	s = open(t, dir)
	defer s.Close()
	_, _, log := load(t, s)
	checkLog(t, log, ent(1, 1, "a"), ent(2, 1, "b"))
}
