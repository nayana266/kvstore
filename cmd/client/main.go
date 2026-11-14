package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kvstore/proto/kvpb"
)

func main() {
	addrs := flag.String("addrs", "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003", "comma-separated KV server addresses")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: client [--addrs a,b,c] put KEY VALUE | get KEY | delete KEY\n")
		os.Exit(2)
	}

	servers := strings.Split(*addrs, ",")
	clientID := fmt.Sprintf("cli-%d", time.Now().UnixNano())
	var seq uint64

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch args[0] {
	case "put":
		if len(args) != 3 {
			fatal("put KEY VALUE")
		}
		seq++
		err := withRetry(ctx, servers, func(ctx context.Context, cl pb.KVClient, hint *string) error {
			resp, err := cl.Put(ctx, &pb.PutRequest{
				Key: args[1], Value: []byte(args[2]), ClientId: clientID, Seq: seq,
			})
			if err != nil {
				return err
			}
			return mapStatus(resp.Status, resp.LeaderHint, hint)
		})
		if err != nil {
			fatal(err.Error())
		}
		fmt.Println("ok")
	case "get":
		if len(args) != 2 {
			fatal("get KEY")
		}
		var value []byte
		var found bool
		err := withRetry(ctx, servers, func(ctx context.Context, cl pb.KVClient, hint *string) error {
			resp, err := cl.Get(ctx, &pb.GetRequest{Key: args[1]})
			if err != nil {
				return err
			}
			if resp.Status == pb.Status_STATUS_OK {
				value, found = resp.Value, resp.Found
				return nil
			}
			return mapStatus(resp.Status, resp.LeaderHint, hint)
		})
		if err != nil {
			fatal(err.Error())
		}
		if !found {
			fmt.Println("(missing)")
			return
		}
		fmt.Printf("%s\n", value)
	case "delete":
		if len(args) != 2 {
			fatal("delete KEY")
		}
		seq++
		err := withRetry(ctx, servers, func(ctx context.Context, cl pb.KVClient, hint *string) error {
			resp, err := cl.Delete(ctx, &pb.DeleteRequest{Key: args[1], ClientId: clientID, Seq: seq})
			if err != nil {
				return err
			}
			return mapStatus(resp.Status, resp.LeaderHint, hint)
		})
		if err != nil {
			fatal(err.Error())
		}
		fmt.Println("ok")
	default:
		fatal("unknown command " + args[0])
	}
}

func mapStatus(st pb.Status, leaderHint string, hint *string) error {
	switch st {
	case pb.Status_STATUS_OK:
		return nil
	case pb.Status_STATUS_NOT_LEADER:
		*hint = leaderHint
		return errNotLeader
	case pb.Status_STATUS_TIMEOUT:
		return fmt.Errorf("timeout (safe to retry with same seq)")
	default:
		return fmt.Errorf("status %v", st)
	}
}

var errNotLeader = fmt.Errorf("not leader")

func withRetry(ctx context.Context, servers []string, fn func(context.Context, pb.KVClient, *string) error) error {
	i := 0
	for try := 0; try < 20; try++ {
		addr := servers[i%len(servers)]
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			i++
			continue
		}
		cl := pb.NewKVClient(conn)
		var hint string
		err = fn(ctx, cl, &hint)
		conn.Close()
		if err == nil {
			return nil
		}
		if err == errNotLeader {
			if hint != "" {
				// Prefer the hinted leader on the next try.
				for j, s := range servers {
					if s == hint {
						i = j
						break
					}
				}
				// If hint isn't in our list, try it anyway by rewriting slot 0 temporarily.
				servers = append([]string{hint}, servers...)
				i = 0
			} else {
				i++
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return err
	}
	return fmt.Errorf("exhausted retries")
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
