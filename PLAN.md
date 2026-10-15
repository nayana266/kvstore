# Distributed Key-Value Store — Build Plan

Raft-based KV store in Go with gRPC, persistence, dynamic membership, a benchmark, and a chaos-testing harness.

**Primary reference:** the Raft paper (Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm"), especially **Figure 2**, and §4 of Ongaro's dissertation for membership changes. Keep Figure 2 open the whole time. Most Raft bugs come from deviating from it.

---

## Learning track (do this alongside the build)

**Before you start (about 1 week):**
- **Go concurrency warm-up.** Do the Go Tour sections on goroutines, channels, `select`, and `sync.Mutex`. Write a few tiny programs and run them with `go run -race`.
- **Read the Raft paper** end to end once. Don't worry about understanding all of it yet.
- **Play with the visualizations** at raft.github.io and "The Secret Lives of Data." Kill the leader and partition nodes, and predict the outcome before clicking.
- **Watch the MIT 6.5840 Raft lectures** (free on YouTube).
- **Read the "Students' Guide to Raft"** (Jon Gjengset) and keep it bookmarked.
- *Optional:* do MIT 6.5840 Lab 3 (Raft) + Lab 4 (KV) privately for their test suites. Don't publish those solutions; this repo should be your own code.

**For every phase:**
1. **Read:** reread the paper sections for that phase.
2. **Explain:** write a short explanation in `NOTES.md` in your own words, e.g. "A follower rejects AppendEntries when..." If you can't write it, you aren't ready to code it.
3. **Write tests:** you can ask Cursor to help write them, as long as you understand what each one checks.
4. **Implement it yourself:** use Cursor in Ask mode for hints only.
5. **Review:** ask Cursor, "Review this against Figure 2. What rule am I missing or violating?"
6. **Debug:** when a test fails, add logging and try on your own for 20–30 minutes before asking for help.
7. **Quiz:** ask Cursor, "Ask me 5 hard interview questions about this phase, one at a time, and correct me." Log any questions you missed in `NOTES.md`.

**What Cursor can write freely:** protobuf definitions, gRPC server/client wiring, CLI flag parsing, shell scripts, benchmark stats output, and test boilerplate.

**What you write (with hints):** elections, replication, the commit rule, persistence ordering, client dedup, membership changes, and the chaos correctness checker.

---

## Repo layout

```
kvstore/
├── cmd/
│   ├── server/        # node binary: --id, --addr, --peers, --data-dir
│   ├── client/        # CLI: put/get/delete
│   ├── bench/         # load generator
│   └── chaos/         # fault-injection harness
├── proto/
│   ├── raft.proto     # RequestVote, AppendEntries, InstallSnapshot (optional)
│   └── kv.proto       # Put, Get, Delete, AddServer, RemoveServer
├── internal/
│   ├── raft/          # consensus core (no KV knowledge)
│   │   ├── raft.go        # state, main loop
│   │   ├── election.go
│   │   ├── replication.go
│   │   ├── membership.go
│   │   └── transport.go   # interface; gRPC + in-memory impls
│   ├── storage/       # durable log + hard state (term, votedFor)
│   └── kv/            # state machine, client dedup, apply loop
├── test/              # integration tests using in-memory transport
├── scripts/           # start a local 3/5-node cluster
├── RESULTS.md         # benchmark + chaos results with machine specs
└── PLAN.md
```

**Key design rule:** `internal/raft` talks to the network only through a `Transport` interface. Build an **in-memory transport** first. It makes tests fast and deterministic and lets you simulate partitions by just dropping messages. The gRPC transport comes later and is thin.

---

## Phase 0 — Setup (½ day)

- `go mod init`, install `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (or use `buf`).
- Define `raft.proto` messages straight from Figure 2's RPC argument/result fields.
- Define `kv.proto`: `Put(key, value, clientId, seq)`, `Get(key)`, `Delete(key, clientId, seq)`. Responses include `leaderHint` for redirects.

**Done when:** `make proto` generates code and `go build ./...` passes.

---

## Phase 1 — Leader election (2–3 days)

- Node state: `currentTerm`, `votedFor`, `role` (follower/candidate/leader), `log[]`.
- Randomized election timeout (e.g. 150–300ms); leader heartbeats (~50ms).
- RequestVote handler with the **log up-to-date check** (last term, then last index).
- Any RPC with a higher term → step down to follower, update term, clear vote.

**Tests (in-memory transport):**
- 3 nodes elect exactly one leader.
- Kill the leader → new leader elected within ~1s.
- Partition a minority → majority still has a leader; minority can't elect one.
- No two leaders in the same term, ever (assert this in every test).

---

## Phase 2 — Log replication (3–4 days)

- Leader tracks `nextIndex[]`, `matchIndex[]`.
- AppendEntries consistency check (`prevLogIndex`, `prevLogTerm`), truncate conflicting entries.
- **Fast backtracking:** follower returns `conflictTerm`/`conflictIndex` so the leader skips back a whole term at a time.
- Commit rule: advance `commitIndex` to N only if a majority has `matchIndex >= N` **and** `log[N].term == currentTerm` (Figure 8 in the paper, the classic bug).
- Apply loop sends committed entries to the state machine via a channel, in order.

**Tests:**
- Entries submitted to the leader get applied on all nodes in the same order.
- Follower that was disconnected catches up after reconnecting.
- Old leader with uncommitted entries rejoins → its conflicting entries are overwritten.
- The Figure 8 scenario.

---

## Phase 3 — Persistence & crash recovery (2 days)

- Persist `currentTerm`, `votedFor`, and log entries **before** responding to any RPC that changed them.
- Simple format: append-only log file of length-prefixed protobuf records + a small `state` file for term/vote. Write to temp file + `fsync` + rename for the state file; `fsync` the log on append.
- On startup: load state + log, replay committed entries into the KV map.
- Optional but good: snapshots + log compaction (`InstallSnapshot` RPC) so restarts don't replay forever.

**Tests:**
- Kill all nodes, restart → same leader-term invariants hold, all committed data present.
- Crash a node mid-replication, restart → it rejoins cleanly.
- Corrupt/truncated tail record on disk is detected and discarded (simulate a crash mid-write).

---

## Phase 4 — KV state machine + gRPC client API (2 days)

- `Put`/`Delete` → leader appends to log, waits until applied, then replies.
- **Client dedup:** each client has `clientId` + monotonically increasing `seq`; state machine stores last `seq` per client and its result, so retries are idempotent. This is what makes "zero loss / zero duplicates" true under retries.
- `Get` must be linearizable: either route reads through the log (simple) or implement ReadIndex (leader confirms it's still leader with a heartbeat round before serving). Start with through-the-log.
- Non-leader → return `NOT_LEADER` + `leaderHint`; client library retries against the hint.
- Swap in the gRPC transport for inter-node RPC; `cmd/server` runs a real node.

**Done when:** `scripts/cluster.sh 3` starts 3 processes and the CLI can put/get through leader failover.

---

## Phase 5 — Dynamic membership changes (3–4 days)

Use **single-server changes** (dissertation §4.1), simpler than joint consensus:
- Config is stored as a special log entry; a node uses the **latest config in its log**, committed or not.
- Only one config change in flight at a time; leader rejects a second until the first commits.
- `AddServer`: first add the node as a **non-voting learner**, replicate until it's caught up (e.g. within one election timeout of the leader), then append the config entry that makes it a voter.
- `RemoveServer`: if the leader removes itself, it steps down after the entry commits.
- Removed servers can disrupt elections with higher terms. Handle with the "ignore RequestVote if you've heard from a current leader within the minimum election timeout" rule.

**Tests:** grow 3→5 under load, shrink 5→3, remove the leader, add a node that was offline the whole time.

---

## Phase 6 — Benchmark (1 day)

`cmd/bench`:
- Flags: `--clients 16 --duration 60s --value-size 128`.
- Each client loops `Put` with unique keys, counts successes/failures, records latency.
- Report: **committed writes/sec**, failed requests, p50/p95/p99 latency.
- Record machine specs, cluster size, fsync on/off, and whether nodes were on one box or several in `RESULTS.md`.

Throughput is dominated by fsync. **Batch** multiple client entries into one disk write + one AppendEntries round to get real numbers.

---

## Phase 7 — Chaos-testing harness (3–4 days)

`cmd/chaos` runs a real multi-process cluster and injects faults while clients write:

| Fault | How |
|---|---|
| Node crash | `kill -9` the process |
| Disk restart | kill, then restart with the same `--data-dir` |
| Isolation | node drops all inbound/outbound RPCs |
| Pairwise partition | node A drops RPCs to/from node B only |

**Partitions:** add a gRPC interceptor on each node that checks a **block list** (settable via an admin RPC or a control file). The harness flips entries on/off. Works without root, unlike iptables.

**Correctness check:**
- Every client logs each write it got an **ack** for (key, value).
- After faults stop and the cluster heals, read every key back; any acked write missing or wrong is a failure.
- Stronger (recommended): record full operation histories and check them with **Porcupine** (Go linearizability checker). That's a much more impressive claim than "no lost writes."

Run each scenario many times with random seeds; log the seed so failures are reproducible.

---

## Working with Cursor

- **One phase per session.** Reference this file with `@PLAN.md` and tell Cursor which phase you're on.
- **Use Ask mode by default.** Switch to Agent mode only for the boilerplate listed in the learning track.
- **Tests first:** "Help me write failing tests for Phase 2 using the in-memory transport." Then you implement until they pass.
- Interviewers will ask things like "what happens if the leader crashes after replicating to a minority?" You should be able to answer from memory, so practice with the quiz step.
- Run tests with `-race` and in a loop (`go test -race -count=50 ./internal/raft/...`). Raft bugs are timing-dependent and often show up only 1 run in 30.
- Keep a mutex-per-node + single goroutine owning state. Don't hold the lock while sending RPCs.

### Suggested `.cursorrules`

```
I'm building this project to LEARN Raft and distributed systems.
- Do NOT write implementation code for internal/raft, internal/storage, internal/kv,
  or the chaos correctness checker unless I explicitly say "write it."
- Instead: give hints, ask me guiding questions, and point to the relevant rule in
  Figure 2 or section of the Raft paper.
- When I share code, review it against Figure 2 and name any missing or violated rule.
- When I'm debugging, help me figure out what to log and how to reason about it
  before suggesting a fix.
- You MAY write boilerplate freely: protobufs, gRPC wiring, CLI flags, scripts,
  benchmark output formatting, and test scaffolding.

This is a Raft-based distributed KV store in Go. Follow Figure 2 of the Raft paper exactly.
- internal/raft must not import gRPC directly; use the Transport interface.
- Never hold the node mutex while making an RPC or blocking on a channel.
- Persist term/votedFor/log before replying to any RPC that changed them.
- Every new behavior needs a test using the in-memory transport.
- Run tests with -race. Prefer simple, explicit code over clever abstractions.
- When implementing a Raft rule, add a comment citing the paper section.
```

---

## Updating the resume

After Phases 6–7, put your **measured** numbers into the bullets (from `RESULTS.md`), even if they're lower or higher than 650 writes/sec. Link the repo with `RESULTS.md` front and center.
