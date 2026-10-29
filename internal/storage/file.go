// Package storage keeps Raft's persistent state on disk.
package storage

import (
	"errors"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	"encoding/json"
	"encoding/binary"
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
	dir     string
	logFile *os.File // the log file, opened for appending

	// In-memory copy of what's on disk. Every change is written to disk
	// before the method returns.
	term     uint64
	votedFor string
	log      []*pb.LogEntry
}
// stateRecord is the contents of the state file.
type stateRecord struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"votedFor"`
}
// Open opens (or creates) the storage in dir.
func Open(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{dir: dir}

	// TODO(step 2): read the state file.
	data, err := os.ReadFile(filepath.Join(dir, stateFile))
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Brand-new storage: term 0, no vote.
		s.term = 0
		s.votedFor = ""
	case err != nil:
		return nil, err
	default:
		var st stateRecord
		if err := json.Unmarshal(data, &st); err != nil {
			return nil, err
		}
		s.term = st.Term
		s.votedFor = st.VotedFor
	}
	
	raw, err := os.ReadFile(filepath.Join(dir, logFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	entries, good, err := readLog(raw)
	if err != nil {
		return nil, err
	}
	// A crash mid-write can leave a torn record at the end. Cut the file back
	// to the last complete record so new appends land in a readable spot.
	if good < len(raw) {
		if err := os.Truncate(filepath.Join(dir, logFile), int64(good)); err != nil {
			return nil, err
		}
	}
	s.log = entries

	f, err := os.OpenFile(filepath.Join(dir, logFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.logFile = f
	return s, nil
}

// Load returns the saved term, vote, and log.
func (s *FileStorage) Load() (uint64, string, []*pb.LogEntry, error) {
	out := make([]*pb.LogEntry, len(s.log))
	for i, e := range s.log {
		out[i] = proto.Clone(e).(*pb.LogEntry)
	}
	return s.term, s.votedFor, out, nil
}

// Close releases open files.
func (s *FileStorage) Close() error {
	return s.logFile.Close()
}



// SaveState durably records currentTerm and votedFor.
// SaveState durably records currentTerm and votedFor.
func (s *FileStorage) SaveState(term uint64, votedFor string) error {
	data, err := json.Marshal(stateRecord{Term: term, VotedFor: votedFor})
	if err != nil {
		return err
	}

	// Write a temp file first, so a crash mid-write can't damage the real one.
	tmp := filepath.Join(s.dir, stateFile+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	// fsync: force the bytes out of the operating system's memory onto the disk.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Rename swaps the new file in all at once: after a crash you find
	// either the old file or the new one, never half of each.
	if err := os.Rename(tmp, filepath.Join(s.dir, stateFile)); err != nil {
		return err
	}

	s.term = term
	s.votedFor = votedFor
	return nil
}

// Append durably adds entries to the end of the log.
// Append durably adds entries to the end of the log.
func (s *FileStorage) Append(entries []*pb.LogEntry) error {
	var buf []byte
	for _, e := range entries {
		data, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(data))) // the 4-byte length
		buf = append(buf, data...)                                  // the entry itself
	}
	if _, err := s.logFile.Write(buf); err != nil {
		return err
	}
	// fsync before returning, so the entries are on disk when Raft replies.
	if err := s.logFile.Sync(); err != nil {
		return err
	}
	for _, e := range entries {
		s.log = append(s.log, proto.Clone(e).(*pb.LogEntry))
	}
	return nil
}

// readLog parses length-prefixed records. It stops at the first incomplete
// record and returns how many bytes were complete records.
func readLog(data []byte) ([]*pb.LogEntry, int, error) {
	var entries []*pb.LogEntry
	pos := 0
	for {
		if len(data)-pos < 4 {
			break // not enough bytes left for a length
		}
		n := int(binary.BigEndian.Uint32(data[pos:]))
		if len(data)-pos-4 < n {
			break // the entry itself is cut off
		}
		e := &pb.LogEntry{}
		if err := proto.Unmarshal(data[pos+4:pos+4+n], e); err != nil {
			return nil, 0, err
		}
		entries = append(entries, e)
		pos += 4 + n
	}
	return entries, pos, nil
}

// encodeRecords turns entries into length-prefixed records.
func encodeRecords(entries []*pb.LogEntry) ([]byte, error) {
	var buf []byte
	for _, e := range entries {
		data, err := proto.Marshal(e)
		if err != nil {
			return nil, err
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(data)))
		buf = append(buf, data...)
	}
	return buf, nil
}

// writeFileSync writes data to path and fsyncs it.
func writeFileSync(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// TruncateFrom durably deletes every entry with Index >= index.
// TruncateFrom durably deletes every entry with Index >= index. It rewrites
// the whole log file; conflicts are rare, so simple beats fast.
func (s *FileStorage) TruncateFrom(index uint64) error {
	var kept []*pb.LogEntry
	for _, e := range s.log {
		if e.Index < index {
			kept = append(kept, e)
		}
	}

	buf, err := encodeRecords(kept)
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, logFile+".tmp")
	if err := writeFileSync(tmp, buf); err != nil {
		return err
	}

	// The open handle points at the old file, so close it, swap the new file
	// in, and reopen.
	if err := s.logFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, logFile)); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(s.dir, logFile), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.logFile = f
	s.log = kept
	return nil
}

