# Benchmark & chaos results

Fill this in after each meaningful run. Throughput is usually dominated by
fsync of the Raft log; numbers without batching are expected to be low.

## Machine

| Field | Value |
|---|---|
| Date | 2026-11-14 |
| OS / kernel | Darwin 25.6.0 arm64 |
| CPU | Apple M5 |
| RAM | 16 GB |
| Disk | local SSD (laptop) |
| Go version | go1.27.2 darwin/arm64 |

## Cluster

| Field | Value |
|---|---|
| Nodes | 3 (same machine) |
| Persistence | FileStorage, fsync on every Append |
| Batching | yes (leader unstable queue, flush ≤10ms or 64 entries; follower multi-entry Append) |
| Network | loopback |

## Benchmark command

```bash
./scripts/cluster.sh 3
# wait for election (~1s)
./bin/bench -addrs 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 \
  -clients 16 -duration 60s -value-size 128
```

## Results

| clients | duration | value-size | writes/sec | fail | p50 | p95 | p99 | notes |
|---|---|---|---|---|---|---|---|---|
| 8 | 5s | 128 | 98.2 | 0 | 66ms | 132ms | 145ms | before batching |
| 8 | 5s | 128 | 632.6 | 0 | 11ms | 21ms | 30ms | after batching (~6.4×) |
| 16 | 60s | 128 |  |  |  |  |  | TODO: full run |

## Chaos (Phase 7)

Harness: `go run ./cmd/chaos -scenario crash|restart|isolate|partition -duration 15s -seed N`

Correctness: ack read-back of every successful Put, then Porcupine on Put history + post-heal Gets.

| Scenario | seed | passes | notes |
|---|---|---|---|
| crash | 1 | yes | 719 acks; porcupine 1438 ops |
| restart | 4 | yes | 1357 acks; porcupine 2714 ops |
| isolate | 2 | yes | 2249 acks; porcupine 4498 ops |
| partition | 3 | yes | 2580 acks; porcupine 5160 ops |

All runs: 3 nodes, same machine, `-duration 9s`, 2026-11-14.
