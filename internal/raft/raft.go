package raft

import (
	"context"
	"fmt"
	pb "kvstore/proto/raftpb"
	"math/rand"
	"sync"
	"time"
)

// Config holds what a Node needs to start.
type Config struct {
	ID        string    // this server's ID
	Peers     []string  // IDs of all other servers in the cluster
	Transport Transport // used to send RPCs to peers
	Storage   Storage   // durable home for currentTerm, votedFor, and log[]
}

// Role is a server's current role.
type Role int

const (
	Follower  Role = iota // 0
	Candidate             // 1
	Leader                // 2
)

// Node is one Raft server.
type Node struct {
	id        string
	done      chan struct{}
	peers     []string
	transport Transport
	storage   Storage

	mu               sync.Mutex
	votes            int
	electionDeadline time.Time
	lastHeartbeat    time.Time
	currentTerm      uint64
	votedFor         string
	log              []*pb.LogEntry
	role             Role

	// Phase 2: Figure 2 volatile state + leader replication progress.
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64 // per-peer; only meaningful when leader
	matchIndex  map[string]uint64 // per-peer; only meaningful when leader
	applied     [][]byte          // command payloads applied in order (test-facing)
}

// LogEntryView is a read-only snapshot of one log entry for tests.
type LogEntryView struct {
	Index uint64
	Term  uint64
	Data  []byte
}

var _ Handler = (*Node)(nil)

func NewNode(cfg Config) *Node {
	n := &Node{
		id:        cfg.ID,
		peers:     cfg.Peers,
		transport: cfg.Transport,
		storage:   cfg.Storage,
		done:      make(chan struct{}),
	}

	// Figure 2, "Persistent state": restore what was saved before a restart.
	term, votedFor, log, err := cfg.Storage.Load()
	if err != nil {
		panic(fmt.Sprintf("raft: loading storage for %s: %v", cfg.ID, err))
	}
	n.currentTerm = term
	n.votedFor = votedFor
	n.log = log
	return n
}

// Start launches the node's background goroutines (election timer,
// heartbeats) and returns without blocking.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
	go n.run()
}

// Stop shuts the node down. Its goroutines should exit soon after.
func (n *Node) Stop() {
	close(n.done)
}

// run is the node's background loop. It exits when Stop closes n.done.
func (n *Node) run() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {

		case <-n.done:
			return

		case <-ticker.C:
			n.tick()
		}
	}
}

// tick runs every 10ms and decides whether to start an election or send heartbeats.
func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == Leader {
		// TODO(Phase 1): step 6, send heartbeats
		if time.Since(n.lastHeartbeat) >= 50*time.Millisecond {
			n.sendHeartbeats()
		}
		return
	}
	// Figure 2, Rules for Servers (followers and candidates): start an election
	// if the election timeout elapses without hearing from a leader (§5.2).
	if time.Now().After(n.electionDeadline) {
		n.startElection()
	}
}

// startElection makes this node a candidate and asks every peer for a vote.
// The caller must hold n.mu.
func (n *Node) startElection() {
	// Figure 2, Rules for Servers (candidates): increment currentTerm, vote for
	// self, reset the election timer, send RequestVote to all other servers.
	n.currentTerm = n.currentTerm + 1
	n.role = Candidate
	n.votedFor = n.id
	n.persistState()
	n.resetElectionTimer()

	n.votes = 1

	lastIndex, lastTerm := n.lastLogInfo()
	req := &pb.RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateId:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	for _, peer := range n.peers {
		go n.requestVote(peer, req)
	}
}

// sendHeartbeats / replicate: send AppendEntries to every peer (§5.3).
// The caller must hold n.mu.
func (n *Node) sendHeartbeats() {
	n.lastHeartbeat = time.Now()
	for _, peer := range n.peers {
		n.sendAppendEntries(peer)
	}
}

// sendAppendEntries builds a peer-specific AppendEntries and sends it.
// The caller must hold n.mu. Must not hold the lock during the RPC itself
// (go starts the send without waiting).
func (n *Node) sendAppendEntries(peer string) {
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}

	prevLogIndex := next - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	// Entries from Raft index `next` through the end of the log. Copied because
	// the RPC goroutine reads them without n.mu, and a later truncate+append
	// would overwrite the shared backing array.
	var entries []*pb.LogEntry
	if next <= uint64(len(n.log)) {
		entries = append([]*pb.LogEntry(nil), n.log[next-1:]...)
	}

	req := &pb.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	go n.appendEntries(peer, req)
}

// appendEntries sends one AppendEntries RPC and handles the reply.
func (n *Node) appendEntries(peer string, req *pb.AppendEntriesRequest) {
	resp, err := n.transport.AppendEntries(context.Background(), peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Figure 2, Rules for Servers (all servers): step down on a higher term.
	if resp.Term > n.currentTerm {
		n.currentTerm = resp.Term
		n.role = Follower
		n.votedFor = ""
		n.persistState()
		n.resetElectionTimer()
		return
	}

	// Ignore replies from an old leadership term.
	if n.role != Leader || req.Term != n.currentTerm {
		return
	}

	if resp.Success {
		// Figure 2, Rules for Servers (leaders): update matchIndex / nextIndex (§5.3).
		match := req.PrevLogIndex + uint64(len(req.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeCommit()
		n.applyCommitted()
		return
	}

	// AppendEntries failed: step nextIndex back and retry.
	// Fast backtracking using conflictTerm / conflictIndex (end of §5.3).
	if resp.ConflictIndex > 0 {
		next := resp.ConflictIndex
		if resp.ConflictTerm > 0 {
			last := uint64(0)
			for _, e := range n.log {
				if e.Term == resp.ConflictTerm {
					last = e.Index
				}
			}
			if last > 0 {
				next = last + 1
			}
		}
		if next < 1 {
			next = 1
		}
		n.nextIndex[peer] = next
	} else if n.nextIndex[peer] > 1 {
		n.nextIndex[peer]--
	}
	n.sendAppendEntries(peer)
}

// maybeCommit advances commitIndex if a majority has replicated an entry from
// the current term (Figure 2 leader rule; Figure 8 / §5.4.2).
// The caller must hold n.mu.
func (n *Node) maybeCommit() {
	majority := (len(n.peers)+1)/2 + 1
	lastIndex, _ := n.lastLogInfo()
	for N := lastIndex; N > n.commitIndex; N-- {
		// Figure 8: only commit entries from the current term this way.
		if n.log[N-1].Term != n.currentTerm {
			continue
		}
		count := 1 // the leader itself has the entry
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = N
			return
		}
	}
}

// applyCommitted applies log[lastApplied+1 .. commitIndex] to the state machine
// in order (Figure 2, Rules for Servers — all servers).
// The caller must hold n.mu.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		data := n.log[n.lastApplied-1].Data
		n.applied = append(n.applied, append([]byte(nil), data...))
	}
}

// requestVote sends one RequestVote RPC to peer and handles the reply.
func (n *Node) requestVote(peer string, req *pb.RequestVoteRequest) {
	// Never hold the lock while making an RPC.
	resp, err := n.transport.RequestVote(context.Background(), peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Figure 2, Rules for Servers (all servers): step down on a higher term.
	if resp.Term > n.currentTerm {
		n.currentTerm = resp.Term
		n.role = Follower
		n.votedFor = ""
		n.persistState()
		return
	}

	// Ignore stale replies from an old election.
	if n.role != Candidate || req.Term != n.currentTerm {
		return
	}

	if resp.VoteGranted {
		n.votes++
		// Majority of the full cluster (self + peers).
		majority := (len(n.peers)+1)/2 + 1
		if n.votes >= majority {
			n.role = Leader
			n.nextIndex = make(map[string]uint64)
			n.matchIndex = make(map[string]uint64)
			lastIndex, _ := n.lastLogInfo()
			for _, peer := range n.peers {
				n.nextIndex[peer] = lastIndex + 1 // optimistic (Figure 2)
				n.matchIndex[peer] = 0
			}
			n.lastHeartbeat = time.Time{} // send heartbeats immediately in tick()
			// TODO(Phase 1): step 6 will send heartbeats from tick()
		}
	}
}

// State reports the node's current term and whether it believes it is the leader.
func (n *Node) State() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

// Propose asks this node to append cmd to the Raft log.
// Returns false if this node is not the leader (caller should retry elsewhere).
func (n *Node) Propose(cmd []byte) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1) Only the leader accepts client commands (Figure 2 / §5.3).
	if n.role != Leader {
		return false
	}

	// 2) New entry goes at the end of the log.
	lastIndex, _ := n.lastLogInfo() // hint: last log index + 1. Use n.lastLogInfo()
	index := lastIndex + 1
	entry := &pb.LogEntry{
		Term:  n.currentTerm, // this leader's term
		Index: index,
		Type:  pb.EntryType_ENTRY_TYPE_NORMAL,
		Data:  cmd,
	}
	n.log = append(n.log, entry)
	n.persistAppend(entry)
	// 3) Optional for later: kick replication immediately so you don't wait
	//    for the next 50ms heartbeat. You can leave this out for now.
	n.sendHeartbeats()

	return true
}

// Applied returns a copy of command payloads applied to the state machine,
// in commit order (Figure 2, All Servers: apply when commitIndex > lastApplied).
func (n *Node) Applied() [][]byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([][]byte, len(n.applied))
	for i, cmd := range n.applied {
		out[i] = append([]byte(nil), cmd...)
	}
	return out
}

// LogEntries returns a snapshot of the Raft log for tests.
func (n *Node) LogEntries() []LogEntryView {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]LogEntryView, len(n.log))
	for i, e := range n.log {
		out[i] = LogEntryView{
			Index: e.Index,
			Term:  e.Term,
			Data:  append([]byte(nil), e.Data...),
		}
	}
	return out
}

// CommitIndex returns the highest log index known to be committed.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// HandleRequestVote is called when a candidate asks this node for its vote.
func (n *Node) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Figure 2, RequestVote receiver rule 1: reject a candidate from an older term.
	if req.Term < n.currentTerm {
		return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	// Figure 2, Rules for Servers (all servers): a higher term means this node
	// is out of date, so adopt the term and become a follower (§5.1).
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.role = Follower
		n.votedFor = ""
		n.persistState()
	}

	// §5.4.1: the candidate's log is at least as up-to-date if its last term is
	// higher, or the last terms match and its log is at least as long.
	lastIndex, lastTerm := n.lastLogInfo()
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)

	// Figure 2, RequestVote receiver rule 2: at most one vote per term (§5.2),
	// and only for an up-to-date candidate (§5.4.1).
	if (n.votedFor == "" || n.votedFor == req.CandidateId) && upToDate {
		n.votedFor = req.CandidateId
		n.persistState()
		n.resetElectionTimer()
		return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}
	}
	return &pb.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
}

// lastLogInfo returns the index and term of the last log entry, or 0, 0 if
// the log is empty. The caller must hold n.mu.
func (n *Node) lastLogInfo() (index, term uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// resetElectionTimer picks a new random election timeout (§5.2).
// The caller must hold n.mu.
func (n *Node) resetElectionTimer() {
	timeout := time.Duration(rand.Intn(150)+150) * time.Millisecond
	n.electionDeadline = time.Now().Add(timeout)
}

// persistState saves currentTerm and votedFor (Figure 2, "Persistent state").
// The caller must hold n.mu, and must call it before replying to or sending
// any RPC that depends on the new values.
func (n *Node) persistState() {
	if err := n.storage.SaveState(n.currentTerm, n.votedFor); err != nil {
		panic(fmt.Sprintf("raft: saving state for %s: %v", n.id, err))
	}
}

// persistTruncate deletes stored entries with Index >= index.
// The caller must hold n.mu.
func (n *Node) persistTruncate(index uint64) {
	if err := n.storage.TruncateFrom(index); err != nil {
		panic(fmt.Sprintf("raft: truncating log for %s: %v", n.id, err))
	}
}

// persistAppend adds entries to the end of the stored log.
// The caller must hold n.mu.
func (n *Node) persistAppend(entries ...*pb.LogEntry) {
	if err := n.storage.Append(entries); err != nil {
		panic(fmt.Sprintf("raft: appending to log for %s: %v", n.id, err))
	}
}

// HandleAppendEntries is called when a leader sends entries or a heartbeat.
func (n *Node) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Figure 2, AppendEntries receiver rule 1: reject a leader from an older term.
	if req.Term < n.currentTerm {
		return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}

	// Figure 2, Rules for Servers (all servers): adopt a higher term (§5.1).
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.persistState()
	}

	// §5.2: the sender is the leader for this term, so this node is a follower.
	// A candidate that hears from the leader steps down.
	n.role = Follower
	n.resetElectionTimer()

	// Figure 2, AppendEntries receiver rule 2: log consistency check.
	// On failure, return conflictTerm/conflictIndex for fast backtracking (§5.3).
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(n.log)) {
			return &pb.AppendEntriesResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictTerm:  0,
				ConflictIndex: uint64(len(n.log)) + 1,
			}
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			conflictTerm := n.log[req.PrevLogIndex-1].Term
			conflictIndex := req.PrevLogIndex
			for conflictIndex > 1 && n.log[conflictIndex-2].Term == conflictTerm {
				conflictIndex--
			}
			return &pb.AppendEntriesResponse{
				Term:          n.currentTerm,
				Success:       false,
				ConflictTerm:  conflictTerm,
				ConflictIndex: conflictIndex,
			}
		}
	}

	// Figure 2, AppendEntries receiver rules 3–4: truncate conflicts, then append.
	// Each change is saved before the reply below (Figure 2, "Persistent state").
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + uint64(i) + 1
		if idx <= uint64(len(n.log)) {
			if n.log[idx-1].Term != entry.Term {
				n.log = n.log[:idx-1] // delete conflicting entry and everything after
				n.persistTruncate(idx)
				n.log = append(n.log, entry)
				n.persistAppend(entry)
			}
			// else: identical entry already present; leave it
		} else {
			n.log = append(n.log, entry)
			n.persistAppend(entry)
		}
	}

	// Figure 2, AppendEntries receiver rule 5: adopt leader's commit index.
	if req.LeaderCommit > n.commitIndex {
		lastIndex, _ := n.lastLogInfo()
		n.commitIndex = min(req.LeaderCommit, lastIndex)
	}
	n.applyCommitted()

	return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: true}
}
