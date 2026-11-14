package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kvstore/proto/kvpb"
)

func main() {
	addrs := flag.String("addrs", "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003", "comma-separated KV server addresses")
	clients := flag.Int("clients", 16, "number of concurrent client goroutines")
	duration := flag.Duration("duration", 60*time.Second, "how long to run")
	valueSize := flag.Int("value-size", 128, "value payload size in bytes")
	flag.Parse()

	if *clients < 1 || *valueSize < 1 {
		fmt.Fprintln(os.Stderr, "clients and value-size must be >= 1")
		os.Exit(2)
	}

	servers := strings.Split(*addrs, ",")
	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	var (
		okCount   atomic.Uint64
		failCount atomic.Uint64
		latMu     sync.Mutex
		latencies []time.Duration
	)

	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	start := time.Now()

	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(clientIdx int) {
			defer wg.Done()
			clientID := fmt.Sprintf("bench-%d-%d", os.Getpid(), clientIdx)
			var seq uint64
			keyN := 0
			for time.Now().Before(deadline) {
				seq++
				keyN++
				key := fmt.Sprintf("b/%d/%d", clientIdx, keyN)
				t0 := time.Now()
				err := putWithRetry(deadline, servers, clientID, seq, key, value)
				elapsed := time.Since(t0)
				if err != nil {
					failCount.Add(1)
					continue
				}
				okCount.Add(1)
				latMu.Lock()
				latencies = append(latencies, elapsed)
				latMu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	wall := time.Since(start)

	ok := okCount.Load()
	fail := failCount.Load()
	var p50, p95, p99 time.Duration
	latMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if n := len(latencies); n > 0 {
		p50 = latencies[percentileIndex(n, 50)]
		p95 = latencies[percentileIndex(n, 95)]
		p99 = latencies[percentileIndex(n, 99)]
	}
	latMu.Unlock()

	rate := float64(ok) / wall.Seconds()
	fmt.Printf("clients=%d duration=%s value-size=%d addrs=%s\n", *clients, *duration, *valueSize, *addrs)
	fmt.Printf("ok=%d fail=%d wall=%s\n", ok, fail, wall.Round(time.Millisecond))
	fmt.Printf("throughput=%.1f committed writes/sec\n", rate)
	fmt.Printf("latency p50=%s p95=%s p99=%s\n", p50, p95, p99)
}

func percentileIndex(n, p int) int {
	if n == 0 {
		return 0
	}
	i := (p*n + 99) / 100 // ceil(p/100 * n), then clamp
	if i < 1 {
		i = 1
	}
	if i > n {
		i = n
	}
	return i - 1
}

func putWithRetry(deadline time.Time, servers []string, clientID string, seq uint64, key string, value []byte) error {
	i := 0
	for try := 0; try < 30; try++ {
		if time.Now().After(deadline) {
			return fmt.Errorf("deadline")
		}
		addr := servers[i%len(servers)]
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cancel()
			i++
			continue
		}
		cl := pb.NewKVClient(conn)
		resp, err := cl.Put(ctx, &pb.PutRequest{
			Key: key, Value: value, ClientId: clientID, Seq: seq,
		})
		conn.Close()
		cancel()
		if err != nil {
			i++
			time.Sleep(20 * time.Millisecond)
			continue
		}
		switch resp.Status {
		case pb.Status_STATUS_OK:
			return nil
		case pb.Status_STATUS_NOT_LEADER:
			if resp.LeaderHint != "" {
				servers = append([]string{resp.LeaderHint}, servers...)
				i = 0
			} else {
				i++
			}
			time.Sleep(20 * time.Millisecond)
		default:
			return fmt.Errorf("status %v", resp.Status)
		}
	}
	return fmt.Errorf("exhausted retries")
}
