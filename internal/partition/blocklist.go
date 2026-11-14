package partition

import (
	"context"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MetadataPeerID is attached by Raft clients so servers can apply partitions.
const MetadataPeerID = "raft-peer-id"

// BlockList is the set of peer ids this node refuses to talk to.
// Safe for concurrent use; used by gRPC interceptors and GRPCTransport.
type BlockList struct {
	mu   sync.RWMutex
	ids  map[string]bool
	self string
}

func NewBlockList(self string) *BlockList {
	return &BlockList{self: self, ids: map[string]bool{}}
}

// Set replaces the block list (empty = heal this node's filters).
func (b *BlockList) Set(peers []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ids = map[string]bool{}
	for _, id := range peers {
		if id != "" && id != b.self {
			b.ids[id] = true
		}
	}
}

// Get returns a copy of blocked peer ids.
func (b *BlockList) Get() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.ids))
	for id := range b.ids {
		out = append(out, id)
	}
	return out
}

// Blocked reports whether traffic to/from peer should be dropped.
func (b *BlockList) Blocked(peer string) bool {
	if b == nil || peer == "" {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.ids[peer]
}

// UnaryServerInterceptor drops inbound RPCs from blocked peers (Raft metadata).
// Admin methods are never blocked so the harness can heal a node.
func (b *BlockList) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if isAdminMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		if from := peerFromCtx(ctx); b.Blocked(from) {
			return nil, status.Errorf(codes.Unavailable, "chaos: blocked inbound from %s", from)
		}
		return handler(ctx, req)
	}
}

func peerFromCtx(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(MetadataPeerID)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func isAdminMethod(fullMethod string) bool {
	return strings.Contains(fullMethod, "Admin/")
}

// ContextWithPeer returns ctx carrying the local Raft id for outbound RPCs.
func ContextWithPeer(ctx context.Context, peerID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, MetadataPeerID, peerID)
}
