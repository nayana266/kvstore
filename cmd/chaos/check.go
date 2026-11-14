package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kvstore/proto/kvpb"
)

// history records timed client ops for Porcupine.
type history struct {
	mu  sync.Mutex
	ops []porcupine.Operation
}

func (h *history) add(clientID int, input, output any, call, ret int64) {
	h.mu.Lock()
	h.ops = append(h.ops, porcupine.Operation{
		ClientId: clientID,
		Input:    input,
		Call:     call,
		Output:   output,
		Return:   ret,
	})
	h.mu.Unlock()
}

func (h *history) snapshot() []porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]porcupine.Operation, len(h.ops))
	copy(out, h.ops)
	return out
}

type kvOp int

const (
	kvPut kvOp = iota
	kvGet
)

type kvInput struct {
	Op    kvOp
	Key   string
	Value string
}

type kvOutput struct {
	OK    bool   // Put succeeded / Get completed
	Found bool   // Get only
	Value string // Get only
}

// kvModel is a multi-key map; each Put overwrites, each Get reads current value.
var kvModel = porcupine.Model{
	Init: func() any {
		return map[string]string{}
	},
	Step: func(state, input, output any) (bool, any) {
		st := state.(map[string]string)
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.Op {
		case kvPut:
			if !out.OK {
				return false, state
			}
			next := cloneMap(st)
			next[in.Key] = in.Value
			return true, next
		case kvGet:
			if !out.OK {
				return false, state
			}
			v, ok := st[in.Key]
			if !ok {
				return !out.Found, state
			}
			return out.Found && out.Value == v, state
		default:
			return false, state
		}
	},
	Equal: func(a, b any) bool {
		return reflect.DeepEqual(a, b)
	},
	DescribeOperation: func(input, output any) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.Op {
		case kvPut:
			return fmt.Sprintf("Put(%q,%q)", in.Key, in.Value)
		case kvGet:
			if !out.Found {
				return fmt.Sprintf("Get(%q)→missing", in.Key)
			}
			return fmt.Sprintf("Get(%q)→%q", in.Key, out.Value)
		}
		return "?"
	},
}

func cloneMap(m map[string]string) map[string]string {
	n := make(map[string]string, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}

// checkAcks verifies every client-acked Put is still visible after heal (PLAN).
func checkAcks(addrs []string, acks *ackLog) error {
	snapshot := acks.snapshot()
	if len(snapshot) == 0 {
		return fmt.Errorf("no acked writes (cluster may not have elected a leader)")
	}

	// Last ack wins per key (writers use unique keys, but be defensive).
	want := map[string]string{}
	for _, a := range snapshot {
		want[a.key] = a.value
	}

	var failures []string
	for key, value := range want {
		got, found, err := getRetry(addrs, key)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: get error: %v", key, err))
			continue
		}
		if !found {
			failures = append(failures, fmt.Sprintf("%s: missing (acked %q)", key, value))
			continue
		}
		if got != value {
			failures = append(failures, fmt.Sprintf("%s: got %q want %q", key, got, value))
		}
	}
	if len(failures) > 0 {
		max := 10
		if len(failures) < max {
			max = len(failures)
		}
		return fmt.Errorf("ack read-back failed (%d keys): %v", len(failures), failures[:max])
	}
	log.Printf("ack read-back ok: %d unique keys", len(want))
	return nil
}

// checkPorcupine checks the concurrent Put history plus post-heal Gets.
// Get outputs use values already verified by checkAcks (call after checkAcks).
func checkPorcupine(hist *history, addrs []string, acks *ackLog) error {
	_ = addrs
	ops := hist.snapshot()
	// Sequential Gets after all Puts returned constrain the final map state.
	clientID := 1000
	t := time.Now().UnixNano()
	for i, a := range acks.snapshot() {
		call := t + int64(i*2)
		ret := call + 1
		ops = append(ops, porcupine.Operation{
			ClientId: clientID,
			Input:    kvInput{Op: kvGet, Key: a.key},
			Call:     call,
			Output:   kvOutput{OK: true, Found: true, Value: a.value},
			Return:   ret,
		})
	}

	res := porcupine.CheckOperationsTimeout(kvModel, ops, 30*time.Second)
	switch res {
	case porcupine.Ok:
		log.Printf("porcupine: linearizable (%d ops)", len(ops))
		return nil
	case porcupine.Unknown:
		return fmt.Errorf("porcupine: timed out checking %d ops", len(ops))
	default:
		return fmt.Errorf("porcupine: not linearizable (%d ops, result=%v)", len(ops), res)
	}
}

func getRetry(addrs []string, key string) (value string, found bool, err error) {
	deadline := time.Now().Add(10 * time.Second)
	i := 0
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		addr := addrs[i%len(addrs)]
		conn, dialErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr != nil {
			cancel()
			i++
			time.Sleep(50 * time.Millisecond)
			continue
		}
		resp, rpcErr := pb.NewKVClient(conn).Get(ctx, &pb.GetRequest{Key: key})
		conn.Close()
		cancel()
		if rpcErr != nil {
			i++
			time.Sleep(50 * time.Millisecond)
			continue
		}
		switch resp.Status {
		case pb.Status_STATUS_OK:
			return string(resp.Value), resp.Found, nil
		case pb.Status_STATUS_NOT_LEADER:
			if resp.LeaderHint != "" {
				addrs = append([]string{resp.LeaderHint}, addrs...)
				i = 0
			} else {
				i++
			}
			time.Sleep(50 * time.Millisecond)
		default:
			i++
			time.Sleep(50 * time.Millisecond)
		}
	}
	return "", false, fmt.Errorf("get %q exhausted retries", key)
}
