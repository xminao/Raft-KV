package raft

// all index start from 1, term start from 1

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// Server role
//
type ServerRole int

const (
	ROLE_FOLLOWER     ServerRole = 1
	ROLE_CANDIDATE    ServerRole = 2
	ROLE_LEADER       ServerRole = 3
	HEARTBEAT_TIMEOUT            = 100 // 100ms, 10times per second.
)

//
// Log Entry
//
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer
	currentRole    ServerRole
	currentTerm    int
	votedCount     int
	votedFor       int

	log     []LogEntry
	applyCh chan ApplyMsg

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited.
	lastApplied int // index of highest log entry applied to state machine.

	// Volatile state on Leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server.
	matchIndex []int // for each server, index of highest log entry known to be replicated on server.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentRole == ROLE_LEADER
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// error
		panic("[readPersist] failed to decode persist data.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// Leader
// revoke by leader, for send append entries
//
func (rf *Raft) SendAppendEntries() {
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.SendAppendEntry(peer)
		}
	}
}

// SendAppendEntry rf: leader peer; server: follower peer
func (rf *Raft) SendAppendEntry(server int) {
	rf.mu.Lock()
	// possibly changed the identity
	if rf.currentRole != ROLE_LEADER {
		rf.mu.Unlock()
		return
	}

	// the log entries leader excepted follower server to replicate
	prevLogIndex := rf.nextIndex[server] - 1
	//fmt.Printf("[test] prevLogIndex=%d\n", prevLogIndex)

	// user deep copy to avoid race condition when override log in AppendEntires()
	entries := make([]LogEntry, len(rf.log[prevLogIndex+1:]))
	copy(entries, rf.log[prevLogIndex+1:])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[prevLogIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	//fmt.Printf("[SendAppendEntry] id=%d, commit index=%d, entries=%v, send entry to id=%d.\n", rf.me, rf.commitIndex, entries, server)
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		//fmt.Printf("[SendAppendEntry] Leader=%d send append entry to Follower=%d failed.\n", rf.me, server)
		return
	} else {
		//fmt.Printf("[SendAppendEntry] Leader=%d send append entry to Follower=%d success.\n", rf.me, server)
	}

	rf.mu.Lock()
	if rf.currentRole != ROLE_LEADER {
		rf.mu.Unlock()
		return
	}

	// handle reply
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// check for can commit
		// if there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
		// and log[N].term == currentTerm, set commitIndex = N
		for N := len(rf.log) - 1; N > rf.commitIndex; N -= 1 {
			count := 0
			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= N {
					count += 1
				}
			}

			if count*2 > len(rf.peers) {
				rf.setCommitIndex(N)
				break
			}
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.switchRole(ROLE_FOLLOWER)
			rf.persist() // currentTerm changed, need persist (4)
		} else {
			// decrease next index, retry in next rpc
			rf.nextIndex[server] -= 1
		}
	}
	rf.mu.Unlock()
}

//
// Leader
// send heartbeat (no log AppendEntries)
//
/*
func (rf *Raft) SendHeartbeat() {
	for server := range rf.peers {
		// send heartbeat rpc to every peer.
		if server == rf.me {
			continue
		}
		// new goroutine, prevent blocked.
		go func(server int) {
			// empty appendEntries, heartbeat
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}
			rf.mu.Lock()
			args.Term = rf.currentTerm
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				//fmt.Printf("[SendHeartbeat]  ID=%d, Role=%d, Term=%d send heartbeat to Server=%d failed.\n", rf.me, rf.currentRole, rf.currentTerm, server)
				return
			} else {
				//fmt.Printf("[SendHeartbeat]  ID=%d, Role=%d, Term=%d send heartbeat to Server=%d successed.\n", rf.me, rf.currentRole, rf.currentTerm, server)
			}

			// handle reply from peer which response.
			rf.mu.Lock()
			if reply.Term > args.Term {
				// switch to follower if new leader exist.
				rf.switchRole(ROLE_FOLLOWER)
				rf.currentTerm = reply.Term
			}
			rf.mu.Unlock()
		}(server)
	}
}
*/

//
// Candidate
// Request vote for other server to elect.
//
func (rf *Raft) StartElection() {
	// reset vote count and time out
	rf.votedFor = rf.me
	rf.votedCount = 1 // include self
	rf.currentTerm += 1
	rf.persist()                               // votedFor and currentTerm changed, need to persist (5)
	rf.electionTimer.Reset(getRandomTimeout()) // reset election timeout (3)

	//fmt.Printf("[Election] ID=%d, Term=%d start election.\n", rf.me, rf.currentTerm)
	// start collection votes
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		// start new goroutine to send request vote rpc
		go func(server int) {
			rf.mu.Lock()
			lastLogIndex := len(rf.log) - 1
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidatedID: rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  rf.log[lastLogIndex].Term,
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()

			//fmt.Printf("[StartElection] ID=%d, Role=%d, Term=%d send vote request to Server=%d.\n", rf.me, rf.currentRole, rf.currentTerm, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				//fmt.Printf("[StartElection] ID=%d, Role=%d, Term=%d request vote to Server=%d failed.\n", rf.me, rf.currentRole, rf.currentRole, server)
				return
			} else {
				//fmt.Printf("[StartElection] ID=%d, Role=%d, Term=%d request vote to Server=%d successed. vote count=%d\n", rf.me, rf.currentRole, rf.currentRole, server, rf.votedCount)
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentRole != ROLE_CANDIDATE || rf.currentTerm != args.Term {
				return
			}
			// if request server is newest
			if reply.Term > rf.currentTerm {
				rf.switchRole(ROLE_FOLLOWER)
				rf.currentTerm = reply.Term
				rf.persist() // currentTerm changed, need to persist (6)
			}
			if reply.VoteGranted && rf.currentRole == ROLE_CANDIDATE {
				rf.votedCount += 1
				if rf.votedCount*2 > len(rf.peers) {
					//fmt.Printf("[StartElection] ID=%d won the election.\n", rf.me)
					rf.switchRole(ROLE_LEADER)
				}
			}
		}(server)
	}
}

// RPC:
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidatedID int // Candidate's ID who requesting vote
	// for candidate restriction
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // term of peer who be request to vote, for candidate update itself
	VoteGranted bool // true: vote to candidate who request; false: disagree who request
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // rf.votedFor, need persist (1)
	//fmt.Printf("[RequestVote] ID=%d, Role=%d, Term=%d recived vote request.\n", rf.me, rf.currentRole, rf.currentTerm)

	// term --> identity --> log
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm == args.Term {
		switch rf.currentRole {
		case ROLE_FOLLOWER:
			// prevent the same peer request vote again.
			if rf.votedFor != -1 && rf.votedFor != args.CandidatedID {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
				return
			}
		case ROLE_CANDIDATE, ROLE_LEADER:
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.switchRole(ROLE_FOLLOWER)
	}

	// candidate' vote should be at least up-to-date as receiver's log
	// up-to-date:
	// a. the logs have last entries with different terms, the log with the later term is more up-to-date
	// b. the logs end with the same term, then whichever log is longer is more up-to-date

	lastLogIndex := len(rf.log) - 1
	// a.
	if lastLogIndex != -1 && args.LastLogTerm < rf.log[lastLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// b.
	if lastLogIndex != -1 && args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < lastLogIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidatedID
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	// reset timer after grant vote (1)
	rf.electionTimer.Reset(getRandomTimeout())
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.currentRole == ROLE_LEADER
	// only leader receive command from client.
	if isLeader {
		//fmt.Println("============================================")
		//fmt.Printf("[Start] ID=%d start command=%v.\n", rf.me, command)
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist() // log changed, need persist (3)
		//fmt.Printf("[Start] log=%v\n", rf.log)
		index = len(rf.log) - 1
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.heartbeatTimer.C:
			// only leader heartbeat timeout
			// empty log: heartbeat
			rf.mu.Lock()
			if rf.currentRole == ROLE_LEADER {
				//fmt.Printf("[Ticker] ID=%d Heartbeat time out.\n", rf.me)
				rf.SendAppendEntries()
				rf.heartbeatTimer.Reset(HEARTBEAT_TIMEOUT * time.Millisecond)
			}
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			// election timeout
			// candidate: start election; follower: become candidate and start election.
			rf.mu.Lock()
			//fmt.Printf("[Ticker] ID=%d Election time out.\n", rf.me)
			switch rf.currentRole {
			case ROLE_CANDIDATE:
				rf.StartElection()
			case ROLE_FOLLOWER:
				rf.switchRole(ROLE_CANDIDATE)
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.mu.Lock()
	rf.heartbeatTimer = time.NewTimer(HEARTBEAT_TIMEOUT * time.Millisecond)
	rf.electionTimer = time.NewTimer(getRandomTimeout())
	rf.currentRole = ROLE_FOLLOWER
	rf.currentTerm = 0 // initialize with 0, the start initial leader will become 1
	rf.votedFor = -1   // peer id, -1: not vote for any peer.

	rf.log = make([]LogEntry, 1) // log index start from 1
	rf.applyCh = applyCh

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.mu.Unlock()
	//fmt.Printf("[Raft] Peer=%d start...\n", me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

//
// RPC: AppendEntries args and reply
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // AppendEntries consistency check
	PrevLogTerm  int // AppendEntries consistency check
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

//
// handle AppendEntries
// rf: peer who reviced appendEntries
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 0: args.term > currentTerm, switch role to follower and update current term to args.term
	// 1: candidate, args.term = current term, switch to follower
	// 2: follower, just update election time out
	// 3: leader, do nothing
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // rf.currentTerm, need persist (2)
	//fmt.Printf("[AppendEntries] ID=%d send AppendEntries to ID=%d, args=%v.\n", args.LeaderId, rf.me, args)
	reply.Success = true

	// 1. Reply false if args term < current term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2. switch to follower if args term > current term, and reset election time out
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchRole(ROLE_FOLLOWER)
	}
	// reset election when received rpc except args term < current term (2)
	// (rpc send from leader)
	rf.electionTimer.Reset(getRandomTimeout())

	// 2. Log consistency check
	// current peer's last log's index or term need to = leader's one

	// a. current peers' last log's index < leader's one
	lastLogIndex := len(rf.log) - 1 // index start with 1
	// 1	2	3	4	5
	// 1	1	2	2	[3]	leader
	// 1	1	2	2		follower
	// 1	1	2	_		follower
	if lastLogIndex < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// b. current peer's last log's index >= leader's one, but term in args prevLogIndex are not
	// 1	2	3	4	5
	// 1	1	2	2	[3]	leader
	// 1	1	2	3		follower
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 3. Log consistency check pass.
	//  Append any new entries not already in the index from args.PrevLogIndex + 1
	//  unmatchIndex: index of args entries, which not match in current peers.

	//init
	// 0	1	2	3	4	5	6	7
	// 0	1	1	2	2	[3]	[3]	[3]  - leader
	// 0	1	1	2	2		follower
	// 0	1	1	2	2	3 - follower
	// above example, args entries: [3, 3, 3], unmatchIndex is 5
	//fmt.Printf("[AppendEntries] Leader ID=%d, Log=%v, Follower=%d, Log=%v. \n", args.LeaderId, args.Entries, rf.me, rf.log)
	unmatchIndex := -1
	for entryIndex := range args.Entries {
		if len(rf.log)-1 < args.PrevLogIndex+entryIndex+1 {
			unmatchIndex = entryIndex
			break
		}
		if rf.log[args.PrevLogIndex+entryIndex+1].Term != args.Entries[entryIndex].Term {
			unmatchIndex = entryIndex
			break
		}
	}

	if unmatchIndex != -1 {
		// there are unmatche entries
		// truncate unmatch follower entries, and cover leader entries
		rf.log = rf.log[:(args.PrevLogIndex + unmatchIndex + 1)] // till to unmatchIndex
		rf.log = append(rf.log, args.Entries[unmatchIndex:]...)
	}

	// 4. if leader's commit > commitIndex, set commitIndex = min(leader's commit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(min(args.LeaderCommit, len(rf.log)-1))
	}

	reply.Success = true

	//fmt.Printf("[AppendEntries] Follower ID=%d, Log=%v.\n", rf.me, rf.log)
}

//
// Switch role
// revoker must hold lock
//
func (rf *Raft) switchRole(role ServerRole) {
	if role == rf.currentRole {
		return
	}
	//fmt.Printf("[SwitchRole] ID=%d, Role=%d Term=%d ===> Role=%d.\n", rf.me, rf.currentRole, rf.currentTerm, role)
	rf.currentRole = role
	switch role {
	case ROLE_FOLLOWER:
		//rf.electionTimer.Reset(getRandomTimeout()) // reset election timeout
		rf.votedFor = -1
	case ROLE_CANDIDATE:
		rf.StartElection()
	case ROLE_LEADER:
		// initialize nextIndex with leader last log index + 1
		// initialize matchIndex with 0
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.SendAppendEntries()
		rf.heartbeatTimer.Reset(HEARTBEAT_TIMEOUT * time.Millisecond)
	}
}

//
// Get random time.
//
func getRandomTimeout() time.Duration {
	return time.Duration(250+rand.Intn(150)) * time.Millisecond
}

//
// Get current time
//
func getCurrentTime() int64 {
	return time.Now().UnixNano()
}

//
// set commit index and update apply
//
func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	// apply entries between lastApplied and commmit
	// should be revoked after current commitIndex updated.
	if rf.commitIndex > rf.lastApplied {
		//fmt.Printf("[Apply] ID=%d apply between index=%d and index=%d.\n", rf.me, rf.lastApplied+1, rf.commitIndex)
		entriesToApply := append([]LogEntry{}, rf.log[(rf.lastApplied+1):(rf.commitIndex+1)]...)

		// new goroutine to apply entries
		go func(startIndex int, entriesToApply []LogEntry) {
			for index, entry := range entriesToApply {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: startIndex + index,
				}
				rf.applyCh <- msg
				// update lastApplied
				// another goroutine, protect it with lock
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					//fmt.Printf("[SetCommitIndex] ID=%d, term=%d current log commited entries length=%d, log length=%d, last log index=%d, term=%d.\n", rf.me, rf.currentTerm, len(rf.log[:rf.commitIndex]), len(rf.log)-1, len(rf.log)-1, rf.log[rf.commitIndex].Term)
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}
}

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}
