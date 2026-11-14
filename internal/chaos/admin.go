package chaos

import (
	"context"

	"kvstore/internal/partition"
	adminpb "kvstore/proto/adminpb"
)

// AdminServer exposes a partition.BlockList over gRPC for the chaos harness.
type AdminServer struct {
	adminpb.UnimplementedAdminServer
	blocks *partition.BlockList
}

func NewAdminServer(blocks *partition.BlockList) *AdminServer {
	return &AdminServer{blocks: blocks}
}

func (s *AdminServer) SetBlockList(_ context.Context, req *adminpb.SetBlockListRequest) (*adminpb.SetBlockListResponse, error) {
	s.blocks.Set(req.PeerIds)
	return &adminpb.SetBlockListResponse{}, nil
}

func (s *AdminServer) GetBlockList(_ context.Context, _ *adminpb.GetBlockListRequest) (*adminpb.GetBlockListResponse, error) {
	return &adminpb.GetBlockListResponse{PeerIds: s.blocks.Get()}, nil
}
