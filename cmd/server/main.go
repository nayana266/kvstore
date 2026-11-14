package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	"kvstore/internal/chaos"
	"kvstore/internal/kv"
	"kvstore/internal/partition"
	"kvstore/internal/raft"
	"kvstore/internal/storage"
	adminpb "kvstore/proto/adminpb"
	pb "kvstore/proto/kvpb"
)

func main() {
	id := flag.String("id", "", "this node's id (required)")
	addr := flag.String("addr", "", "gRPC listen address, e.g. 127.0.0.1:5001 (required)")
	peers := flag.String("peers", "", "other peers as id=addr,id=addr")
	dataDir := flag.String("data-dir", "", "directory for Raft persistent state (required)")
	flag.Parse()

	if *id == "" || *addr == "" || *dataDir == "" {
		flag.Usage()
		os.Exit(2)
	}

	peerAddrs := map[string]string{}
	var peerIDs []string
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			idAddr := strings.SplitN(p, "=", 2)
			if len(idAddr) != 2 {
				log.Fatalf("bad --peers entry %q (want id=addr)", p)
			}
			peerAddrs[idAddr[0]] = idAddr[1]
			peerIDs = append(peerIDs, idAddr[0])
		}
	}
	// Include ourselves for leader hints from any node.
	allAddrs := map[string]string{*id: *addr}
	for k, v := range peerAddrs {
		allAddrs[k] = v
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatal(err)
	}
	st, err := storage.Open(filepath.Join(*dataDir))
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	blocks := partition.NewBlockList(*id)
	applyCh := make(chan raft.ApplyMsg, 10_000)
	tr := raft.NewGRPCTransportWithBlocks(*id, peerAddrs, blocks)
	defer tr.Close()

	node := raft.NewNode(raft.Config{
		ID:        *id,
		Peers:     peerIDs,
		Transport: tr,
		Storage:   st,
		ApplyCh:   applyCh,
	})

	srv, ln, err := raft.Listen(*addr, grpc.UnaryInterceptor(blocks.UnaryServerInterceptor()))
	if err != nil {
		log.Fatal(err)
	}
	raft.ServeRaft(srv, node)

	store := kv.NewStore()
	kvSvc := kv.NewServiceWithPeers(node, store, applyCh, allAddrs, tr)
	pb.RegisterKVServer(srv, kvSvc)
	adminpb.RegisterAdminServer(srv, chaos.NewAdminServer(blocks))

	node.Start()
	defer node.Stop()

	go func() {
		log.Printf("node %s listening on %s (data %s)", *id, *addr, *dataDir)
		if err := srv.Serve(ln); err != nil {
			log.Printf("serve: %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	fmt.Println("shutting down")
	srv.GracefulStop()
}
