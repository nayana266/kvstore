package raft

import (
	"context"

	pb "kvstore/proto/raftpb"
)

// Transport is how a node sends RPCs to its peers. The raft package only talks
// to the network through this interface. Implementations must be safe to call
// from multiple goroutines at once.
type Transport interface {
	RequestVote(ctx context.Context, to string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
}

// Handler receives RPCs from peers. Node implements it; a transport delivers
// incoming requests to it.
type Handler interface {
	HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse
	HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse
}
