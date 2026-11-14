// Command chaos runs a real multi-process Raft cluster, injects faults while
// clients write, then heals and checks correctness (ack read-back + Porcupine).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminpb "kvstore/proto/adminpb"
	pb "kvstore/proto/kvpb"
)

func main() {
	scenario := flag.String("scenario", "crash", "crash|restart|isolate|partition")
	nodes := flag.Int("nodes", 3, "cluster size")
	duration := flag.Duration("duration", 15*time.Second, "how long clients write")
	seed := flag.Int64("seed", 0, "rng seed (0 = use time)")
	dataRoot := flag.String("data", "", "data dir (default: .data/chaos-<pid>)")
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(*seed))
	log.Printf("chaos seed=%d scenario=%s nodes=%d duration=%s", *seed, *scenario, *nodes, *duration)

	root, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	bin := filepath.Join(root, "bin")
	if err := buildBins(root, bin); err != nil {
		log.Fatal(err)
	}

	data := *dataRoot
	if data == "" {
		data = filepath.Join(root, ".data", fmt.Sprintf("chaos-%d", os.Getpid()))
	}
	_ = os.RemoveAll(data)
	if err := os.MkdirAll(data, 0o755); err != nil {
		log.Fatal(err)
	}

	c := newCluster(bin, data, *nodes)
	if err := c.startAll(); err != nil {
		log.Fatal(err)
	}
	defer c.stopAll()

	time.Sleep(2 * time.Second) // election

	acks := &ackLog{}
	hist := &history{}
	stopClients := make(chan struct{})
	var clientWG sync.WaitGroup
	var putsOK atomic.Uint64
	for i := 0; i < 4; i++ {
		clientWG.Add(1)
		go func(id int) {
			defer clientWG.Done()
			runWriter(c.addrs(), id, fmt.Sprintf("chaos-cli-%d", id), stopClients, acks, hist, &putsOK)
		}(i)
	}

	faultDone := time.After(*duration / 3)
	healAt := time.After(2 * *duration / 3)
	endAt := time.After(*duration)
	faultInjected := false
	healed := false

	for {
		select {
		case <-faultDone:
			if faultInjected {
				continue
			}
			faultInjected = true
			if err := c.inject(*scenario, rng); err != nil {
				log.Printf("inject: %v", err)
			}
		case <-healAt:
			if healed {
				continue
			}
			healed = true
			if err := c.heal(); err != nil {
				log.Printf("heal: %v", err)
			}
		case <-endAt:
			close(stopClients)
			clientWG.Wait()
			if !healed {
				_ = c.heal()
			}
			time.Sleep(2 * time.Second) // let commits catch up

			log.Printf("writes ok=%d acked=%d", putsOK.Load(), acks.len())
			addrs := c.addrs()
			if err := checkAcks(addrs, acks); err != nil {
				log.Printf("CORRECTNESS: %v", err)
				os.Exit(1)
			}
			if err := checkPorcupine(hist, addrs, acks); err != nil {
				log.Printf("CORRECTNESS: %v", err)
				os.Exit(1)
			}
			log.Printf("PASS chaos scenario=%s seed=%d", *scenario, *seed)
			return
		}
	}
}

func buildBins(root, bin string) error {
	if err := os.MkdirAll(bin, 0o755); err != nil {
		return err
	}
	for _, pair := range [][2]string{
		{"server", "./cmd/server"},
		{"client", "./cmd/client"},
	} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(bin, pair[0]), pair[1])
		cmd.Dir = root
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("build %s: %w", pair[0], err)
		}
	}
	return nil
}

type cluster struct {
	bin   string
	data  string
	n     int
	procs map[string]*os.Process
	mu    sync.Mutex
}

func newCluster(bin, data string, n int) *cluster {
	return &cluster{bin: bin, data: data, n: n, procs: map[string]*os.Process{}}
}

func (c *cluster) id(i int) string   { return fmt.Sprintf("n%d", i) }
func (c *cluster) addr(i int) string { return fmt.Sprintf("127.0.0.1:%d", 6000+i) }

func (c *cluster) addrs() []string {
	out := make([]string, c.n)
	for i := 1; i <= c.n; i++ {
		out[i-1] = c.addr(i)
	}
	return out
}

func (c *cluster) peersFlag(self int) string {
	var parts []string
	for i := 1; i <= c.n; i++ {
		if i == self {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", c.id(i), c.addr(i)))
	}
	return strings.Join(parts, ",")
}

func (c *cluster) startAll() error {
	for i := 1; i <= c.n; i++ {
		if err := c.start(i); err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) start(i int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.id(i)
	dir := filepath.Join(c.data, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	cmd := exec.Command(filepath.Join(c.bin, "server"),
		"-id="+id,
		"-addr="+c.addr(i),
		"-peers="+c.peersFlag(i),
		"-data-dir="+dir,
	)
	logf, err := os.Create(filepath.Join(c.data, id+".log"))
	if err != nil {
		return err
	}
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		return err
	}
	c.procs[id] = cmd.Process
	log.Printf("started %s pid=%d", id, cmd.Process.Pid)
	return nil
}

func (c *cluster) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, p := range c.procs {
		_ = p.Kill()
		_, _ = p.Wait()
		delete(c.procs, id)
	}
}

func (c *cluster) kill9(i int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.id(i)
	p := c.procs[id]
	if p == nil {
		return fmt.Errorf("%s not running", id)
	}
	if err := p.Signal(syscall.SIGKILL); err != nil {
		return err
	}
	_, _ = p.Wait()
	delete(c.procs, id)
	log.Printf("kill -9 %s", id)
	return nil
}

func (c *cluster) setBlock(i int, peers []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(c.addr(i), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = adminpb.NewAdminClient(conn).SetBlockList(ctx, &adminpb.SetBlockListRequest{PeerIds: peers})
	return err
}

func (c *cluster) inject(scenario string, rng *rand.Rand) error {
	victim := 1 + rng.Intn(c.n)
	switch scenario {
	case "crash":
		return c.kill9(victim)
	case "restart":
		if err := c.kill9(victim); err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)
		return c.start(victim)
	case "isolate":
		log.Printf("isolate %s", c.id(victim))
		var others []string
		for i := 1; i <= c.n; i++ {
			if i != victim {
				others = append(others, c.id(i))
			}
		}
		if err := c.setBlock(victim, others); err != nil {
			return err
		}
		for i := 1; i <= c.n; i++ {
			if i == victim {
				continue
			}
			if err := c.setBlock(i, []string{c.id(victim)}); err != nil {
				return err
			}
		}
		return nil
	case "partition":
		// Pairwise: cut victim from one other node only.
		other := victim
		for other == victim {
			other = 1 + rng.Intn(c.n)
		}
		log.Printf("partition %s <-> %s", c.id(victim), c.id(other))
		if err := c.setBlock(victim, []string{c.id(other)}); err != nil {
			return err
		}
		return c.setBlock(other, []string{c.id(victim)})
	default:
		return fmt.Errorf("unknown scenario %q", scenario)
	}
}

func (c *cluster) heal() error {
	log.Printf("heal: clear block lists and ensure all nodes up")
	for i := 1; i <= c.n; i++ {
		c.mu.Lock()
		running := c.procs[c.id(i)] != nil
		c.mu.Unlock()
		if !running {
			if err := c.start(i); err != nil {
				return err
			}
			time.Sleep(300 * time.Millisecond)
		}
		if err := c.setBlock(i, nil); err != nil {
			log.Printf("heal %s block clear: %v (may still be starting)", c.id(i), err)
		}
	}
	return nil
}

type ack struct {
	key, value string
}

type ackLog struct {
	mu   sync.Mutex
	acks []ack
}

func (a *ackLog) add(key, value string) {
	a.mu.Lock()
	a.acks = append(a.acks, ack{key, value})
	a.mu.Unlock()
}

func (a *ackLog) len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.acks)
}

func (a *ackLog) snapshot() []ack {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]ack, len(a.acks))
	copy(out, a.acks)
	return out
}

func runWriter(addrs []string, clientIdx int, clientID string, stop <-chan struct{}, acks *ackLog, hist *history, putsOK *atomic.Uint64) {
	var seq uint64
	n := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		seq++
		n++
		key := fmt.Sprintf("%s/%d", clientID, n)
		val := fmt.Sprintf("v-%d", seq)
		call := time.Now().UnixNano()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := putRetry(ctx, addrs, clientID, seq, key, []byte(val))
		cancel()
		ret := time.Now().UnixNano()
		if err != nil {
			continue
		}
		if ret <= call {
			ret = call + 1
		}
		putsOK.Add(1)
		acks.add(key, val)
		hist.add(clientIdx, kvInput{Op: kvPut, Key: key, Value: val}, kvOutput{OK: true}, call, ret)
	}
}

func putRetry(ctx context.Context, addrs []string, clientID string, seq uint64, key string, value []byte) error {
	i := 0
	for try := 0; try < 15; try++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		addr := addrs[i%len(addrs)]
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			i++
			continue
		}
		resp, err := pb.NewKVClient(conn).Put(ctx, &pb.PutRequest{
			Key: key, Value: value, ClientId: clientID, Seq: seq,
		})
		conn.Close()
		if err != nil {
			i++
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if resp.Status == pb.Status_STATUS_OK {
			return nil
		}
		if resp.Status == pb.Status_STATUS_NOT_LEADER && resp.LeaderHint != "" {
			addrs = append([]string{resp.LeaderHint}, addrs...)
			i = 0
			time.Sleep(20 * time.Millisecond)
			continue
		}
		i++
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("put failed")
}
