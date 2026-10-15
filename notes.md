# Notes

## Phase 1: Leader Election

Each server has 3 different states: leader, follower, or candidate.
Followers are passive, they don't issue any requests on their own but respond to requests from leaders and candidates.
The leader handles all client requests.
The candidate is used to elect a new leader.

Raft divides time into terms. Each term begins with an election, in which one or more candidates attempt to become leader. If candidate wins the election, they become leader for the rest of the term. 

Sometimes, an election can end up in a split. In this case the term will end with no leader, a new term (with a new election)

Raft servers communicate using remote procedure calls (RPCs). Basic consensus algorithm requires only two types of RPCs. RequestVote RPCs are initiated by candidates during election, and AppendEntries RPCs are initiated by leaders to replicate log entries and to provide a form of heartbeat

A candidate wins an election if it recieves votes from a majority of the servers in the full cluster for the same term. Each server will vote for at most one candidate in a given term. Majority rule ensures that at most one candidate can win the election for a particular term. 

While waiting for votes, a candidate may recieve an AppendEntries RPC from another server claiming to be leader. If the leader's term is at least as learge as the candidate's current term, then the candidate recognizes the leader as legitimate and returns to the follower state.

If many followers become candidates at the same time, votes could be split so that no candidate obtains a majority. When this happens, each candidate will time out and start a new election by incrementing its term and initiating another round of RequestVote RPCs. 