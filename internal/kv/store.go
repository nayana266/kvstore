package kv

import "sync"

// ApplyResult is what the state machine returns after applying one command.
type ApplyResult struct {
	// For Get: Found/Value. For Put/Delete: unused (OK is implied if applied).
	Found bool
	Value []byte
	// Dup is true if this command was a duplicate seq (already applied).
	Dup bool
}

// Store is the Raft state machine: a key-value map plus client dedup table.
type Store struct {
	mu sync.Mutex

	data map[string][]byte

	// lastSeq is the highest seq applied per clientId.
	// lastResult caches the ApplyResult for that seq so retries are idempotent.
	lastSeq    map[string]uint64
	lastResult map[string]ApplyResult
}

func NewStore() *Store {
	return &Store{
		data:       map[string][]byte{},
		lastSeq:    map[string]uint64{},
		lastResult: map[string]ApplyResult{},
	}
}

// Apply runs one log command against the state machine.
func (s *Store) Apply(cmd Command) ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Type {
	case OpGet:
		v, ok := s.data[cmd.Key]
		if !ok {
			return ApplyResult{Found: false}
		}
		return ApplyResult{Found: true, Value: append([]byte(nil), v...)}

	case OpPut, OpDelete:
		if prev, ok := s.lastSeq[cmd.ClientID]; ok && cmd.Seq <= prev {
			r := s.lastResult[cmd.ClientID]
			r.Dup = true
			return r
		}
		var r ApplyResult
		if cmd.Type == OpPut {
			s.data[cmd.Key] = append([]byte(nil), cmd.Value...)
		} else {
			delete(s.data, cmd.Key)
		}
		s.lastSeq[cmd.ClientID] = cmd.Seq
		s.lastResult[cmd.ClientID] = r
		return r

	default:
		return ApplyResult{}
	}
}

// GetLocal is a non-linearizable local read for debugging only.
func (s *Store) GetLocal(key string) (value []byte, found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}
