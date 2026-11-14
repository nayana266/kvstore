package kv

import (
	"encoding/json"
)

// Op is a command stored in a Raft log entry's Data field.
type OpType string

const (
	OpPut    OpType = "Put"
	OpDelete OpType = "Delete"
	OpGet    OpType = "Get" // through-the-log linearizable read (Phase 4)
)

// Command is encoded into log entry bytes.
type Command struct {
	Type     OpType `json:"type"`
	Key      string `json:"key"`
	Value    []byte `json:"value,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

func Encode(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func Decode(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
