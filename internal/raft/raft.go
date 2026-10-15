package raft

import (
	"context"
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

	mu               sync.Mutex
	votes            int
	electionDeadline time.Time
	lastHeartbeat    time.Time
	currentTerm      uint64
	votedFor         string
	log              []*pb.LogEntry
	role             Role
}

var _ Handler = (*Node)(nil)

func NewNode(cfg Config) *Node {
	return &Node{
		id:        cfg.ID,
		peers:     cfg.Peers,
		transport: cfg.Transport,
		done:      make(chan struct{}),
	}
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

// sendHeartbeats sends empty AppendEntries to every peer.
// The caller must hold n.mu.
func (n *Node) sendHeartbeats() {
	n.lastHeartbeat = time.Now()
	lastIndex, lastTerm := n.lastLogInfo()
	req := &pb.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: lastIndex,
		PrevLogTerm:  lastTerm,
		Entries:      nil, // empty = heartbeat
		LeaderCommit: 0,   // Phase 2 will track commitIndex
	}
	for _, peer := range n.peers {
		go n.appendEntries(peer, req)
	}
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
		n.resetElectionTimer()
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
	}

	// §5.2: the sender is the leader for this term, so this node is a follower.
	// A candidate that hears from the leader steps down.
	n.role = Follower
	n.resetElectionTimer()

	// TODO(Phase 2): log consistency check and appending entries (Figure 2,
	// AppendEntries receiver rules 2–5).
	return &pb.AppendEntriesResponse{Term: n.currentTerm, Success: true}
}
