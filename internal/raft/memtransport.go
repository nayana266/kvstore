package raft

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "kvstore/proto/raftpb"
)

// ErrUnreachable is returned when the in-memory network drops a message.
var ErrUnreachable = errors.New("raft: peer unreachable")

// MemNetwork connects nodes running in the same process. Tests use it to
// simulate crashed or partitioned nodes by dropping messages.
type MemNetwork struct {
	mu           sync.Mutex
	handlers     map[string]Handler
	disconnected map[string]bool
	group        map[string]int // partition group per node; nil means no partition
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{
		handlers:     map[string]Handler{},
		disconnected: map[string]bool{},
	}
}

// Register makes h reachable at id.
func (n *MemNetwork) Register(id string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Transport returns the endpoint the node with the given id uses to send RPCs.
func (n *MemNetwork) Transport(from string) Transport {
	return &memTransport{net: n, from: from}
}

// Disconnect drops all messages to and from id, as if the node crashed or
// lost its network connection.
func (n *MemNetwork) Disconnect(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disconnected[id] = true
}

// Reconnect undoes Disconnect.
func (n *MemNetwork) Reconnect(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.disconnected, id)
}

// Partition splits the network so nodes can only reach nodes in the same
// group. Nodes not listed in any group can only reach each other.
func (n *MemNetwork) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = map[string]int{}
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i + 1
		}
	}
}

// Heal removes all partitions and reconnects every node.
func (n *MemNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = nil
	n.disconnected = map[string]bool{}
}

func (n *MemNetwork) route(from, to string) (Handler, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.disconnected[from] || n.disconnected[to] {
		return nil, false
	}
	if n.group != nil && n.group[from] != n.group[to] {
		return nil, false
	}
	h, ok := n.handlers[to]
	return h, ok
}

type memTransport struct {
	net  *MemNetwork
	from string
}

func (t *memTransport) RequestVote(ctx context.Context, to string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return deliver(ctx, t, to, req, Handler.HandleRequestVote)
}

func (t *memTransport) AppendEntries(ctx context.Context, to string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return deliver(ctx, t, to, req, Handler.HandleAppendEntries)
}

// deliver calls the receiver's handler directly. Requests and replies are
// cloned so sender and receiver never share memory, as with a real network.
// The reply is dropped if the network changed while the handler ran.
func deliver[Req, Resp proto.Message](ctx context.Context, t *memTransport, to string, req Req, handle func(Handler, Req) Resp) (Resp, error) {
	var zero Resp
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	h, ok := t.net.route(t.from, to)
	if !ok {
		return zero, ErrUnreachable
	}
	resp := handle(h, proto.Clone(req).(Req))
	if _, ok := t.net.route(to, t.from); !ok {
		return zero, ErrUnreachable
	}
	return proto.Clone(resp).(Resp), nil
}
