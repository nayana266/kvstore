package raft

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kvstore/proto/raftpb"
)

// GRPCTransport is a Raft Transport over gRPC. Peer ids map to dial addresses.
type GRPCTransport struct {
	mu      sync.Mutex
	localID string
	peers   map[string]string // id → "host:port"
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.RaftClient
}

// NewGRPCTransport builds a client-side transport. Call Close when done.
func NewGRPCTransport(localID string, peers map[string]string) *GRPCTransport {
	return &GRPCTransport{
		localID: localID,
		peers:   peers,
		conns:   map[string]*grpc.ClientConn{},
		clients: map[string]pb.RaftClient{},
	}
}

func (t *GRPCTransport) client(to string) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[to]; ok {
		return c, nil
	}
	addr, ok := t.peers[to]
	if !ok {
		return nil, fmt.Errorf("raft grpc: unknown peer %q", to)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.conns[to] = conn
	cl := pb.NewRaftClient(conn)
	t.clients[to] = cl
	return cl, nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, to string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	cl, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return cl.RequestVote(ctx, req)
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, to string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	cl, err := t.client(to)
	if err != nil {
		return nil, err
	}
	return cl.AppendEntries(ctx, req)
}

func (t *GRPCTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var first error
	for id, c := range t.conns {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
		delete(t.conns, id)
		delete(t.clients, id)
	}
	return first
}

// raftServer adapts Handler to the generated gRPC RaftServer.
type raftServer struct {
	pb.UnimplementedRaftServer
	h Handler
}

func (s *raftServer) RequestVote(_ context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.h.HandleRequestVote(req), nil
}

func (s *raftServer) AppendEntries(_ context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.h.HandleAppendEntries(req), nil
}

// ServeRaft registers the Raft gRPC service on srv.
func ServeRaft(srv *grpc.Server, h Handler) {
	pb.RegisterRaftServer(srv, &raftServer{h: h})
}

// Listen starts a gRPC server on addr that serves Raft (and any other services
// already registered on the returned server before Serve is called).
// Prefer registering KV on the same server in cmd/server.
func Listen(addr string) (*grpc.Server, net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	srv := grpc.NewServer()
	return srv, ln, nil
}
