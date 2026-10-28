# Notes

## Phase 1: Leader Election

Each server has 3 different states: leader, follower, or candidate.
Followers don't issue any requests on their own but respond to requests from leaders and candidates.
The leader handles all client requests.
The candidate is used to elect a new leader.

Raft divides time into terms. Each term begins with an election, in which one or more candidates attempt to become leader. If candidate wins the election, they become leader for the rest of the term. 

Sometimes, an election can end up in a split. In this case the term will end with no leader, a new term (with a new election)

Raft servers communicate using remote procedure calls (RPCs). Basic consensus algorithm requires only two types of RPCs. RequestVote RPCs are initiated by candidates during election, and AppendEntries RPCs are initiated by leaders to replicate log entries and to provide a form of heartbeat

A candidate wins an election if it recieves votes from a majority of the servers in the full cluster for the same term. Each server will vote for at most one candidate in a given term. Majority rule ensures that at most one candidate can win the election for a particular term. 

While waiting for votes, a candidate may recieve an AppendEntries RPC from another server claiming to be leader. If the leader's term is at least as learge as the candidate's current term, then the candidate recognizes the leader as legitimate and returns to the follower state.

If many followers become candidates at the same time, votes could be split so that no candidate obtains a majority. When this happens, each candidate will time out and start a new election by incrementing its term and initiating another round of RequestVote RPCs. 

When does a node grant its vote?
If candidate's term is higher than the node's own, the node first switches to the new term: it updates currentTerm, becomes a follower, and clears votedFor. It says no if: the candidate's term is lower than the node's currentTerm, the node already voted for a different candidate in this term, the candidate's kog is less up-to-date than the node's. If none of those apply, the node grants the vote. It sets votedFor to the candidate and resets its election timer. 

## Phase 2 : Log Replication
A follower rejects AppendEntries if either the leader's term is old or the log does not match.
The leader may set commitIndex to N when a majority of the cluster has that entry and log[N].term == currentTerm.

## Phase 3:
currentTerm, votedFor, and the log go to disk. They record promises a node has made to the rest of the cluster: which term it's in, who it voted for, and which entries it has accepted. Forgetting any of them could let the node break a promise after a restart.
Everything else can be rebuilt. A restarted node starts as a follower. The leader tells it the commit index in the next AppendEntries, and it re-applies entries from its saved log. A new leader sets up nextIndex and matchIndex from scratch anyway.
If it replies first and crashes before saving, it restarts with no memory of the vote. It could then vote for a different candidate in the same term. Both candidates could reach a majority, giving two leaders in one term, which Raft must never allow. Saving first means a "yes" that left the node is always on disk.
The same idea applies to the log. A follower must save new entries before replying Success: true, because the leader counts that reply toward a majority when deciding what's committed.