package kv

import (
	"bytes"
	"testing"
)

func TestApplyPutGet(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Type: OpPut, Key: "a", Value: []byte("1"), ClientID: "c1", Seq: 1})
	got := s.Apply(Command{Type: OpGet, Key: "a"})
	if !got.Found || !bytes.Equal(got.Value, []byte("1")) {
		t.Fatalf("Get after Put: found=%v value=%q; want true, \"1\"", got.Found, got.Value)
	}
}

func TestApplyDelete(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Type: OpPut, Key: "a", Value: []byte("1"), ClientID: "c1", Seq: 1})
	s.Apply(Command{Type: OpDelete, Key: "a", ClientID: "c1", Seq: 2})
	got := s.Apply(Command{Type: OpGet, Key: "a"})
	if got.Found {
		t.Fatalf("Get after Delete found value %q; want not found", got.Value)
	}
}

func TestClientDedup(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Type: OpPut, Key: "a", Value: []byte("1"), ClientID: "c1", Seq: 1})
	// Same seq again = retry; must not change state, Dup should be true.
	r := s.Apply(Command{Type: OpPut, Key: "a", Value: []byte("WRONG"), ClientID: "c1", Seq: 1})
	if !r.Dup {
		t.Fatalf("duplicate seq should set Dup=true")
	}
	got := s.Apply(Command{Type: OpGet, Key: "a"})
	if !bytes.Equal(got.Value, []byte("1")) {
		t.Fatalf("dedup failed: value=%q; want \"1\"", got.Value)
	}

	// Older seq must also be ignored.
	s.Apply(Command{Type: OpPut, Key: "a", Value: []byte("OLD"), ClientID: "c1", Seq: 1})
	got = s.Apply(Command{Type: OpGet, Key: "a"})
	if !bytes.Equal(got.Value, []byte("1")) {
		t.Fatalf("stale seq applied: value=%q; want \"1\"", got.Value)
	}

	// New seq updates.
	s.Apply(Command{Type: OpPut, Key: "a", Value: []byte("2"), ClientID: "c1", Seq: 2})
	got = s.Apply(Command{Type: OpGet, Key: "a"})
	if !bytes.Equal(got.Value, []byte("2")) {
		t.Fatalf("new seq: value=%q; want \"2\"", got.Value)
	}
}

func TestDedupPerClient(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Type: OpPut, Key: "a", Value: []byte("c1"), ClientID: "c1", Seq: 1})
	s.Apply(Command{Type: OpPut, Key: "a", Value: []byte("c2"), ClientID: "c2", Seq: 1})
	got := s.Apply(Command{Type: OpGet, Key: "a"})
	if !bytes.Equal(got.Value, []byte("c2")) {
		t.Fatalf("value=%q; want \"c2\" (second client)", got.Value)
	}
}
