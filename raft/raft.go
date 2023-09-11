package raft

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
	"math/rand"

	"fmt"
	"sync"
	"sync/atomic"
	"time"

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

	commitIndex int // index of highest log entry known to be commited.
	lastApplied int // index of highest log entry applied to state machine.
	log         []LogEntry
	applyCh     chan ApplyMsg

	// leader
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	// send append entries to all peers.
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			// append entries args by leader
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}

			// check if need replicate log
			rf.mu.Lock()
			if rf.currentRole != ROLE_LEADER {
				rf.mu.Unlock()
				return
			}
			// initialize appendEntries which need send
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			// PrevLogIndex: index of log entry immediately preceding new ones.
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.LeaderCommit = rf.commitIndex
			// exist log not commit in leader peer, need replicate to followers.
			// rf.log: log entries in leader peer.
			// rf.matchIndex[server]: the highest commit log index in leader peer.
			// log need to replicate: [matchIndex[server] + 1, len[rf.log]]
			if len(rf.log) != rf.matchIndex[server] {
				for i := rf.nextIndex[server]; i <= len(rf.log); i += 1 {
					args.Entries = append(args.Entries, rf.log[i])
				}
			}
			rf.mu.Unlock()

			// send appendEntries
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				fmt.Printf("[SendAppendEntries] ID=%d send AppendEntries to ID=%d failed.", rf.me, server)
			} else {
				fmt.Printf("[SendAppendEntries] ID=%d send AppendEntries to ID=%d success.", rf.me, server)
			}

			// handle reply
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > args.Term {
				rf.switchRole(ROLE_FOLLOWER)
				rf.currentTerm = reply.Term
				return
			}

			if rf.currentRole != ROLE_LEADER || rf.currentTerm != args.Term {
				return
			}

			if reply.Success {
				// replication success, increase nextIndex and matchIndex
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				fmt.Printf("[SendAppendEntries] Leader ID=%d replicate log to ID=%d success, nextIndex=%v, matchIndex=%v.",
					rf.me, server, rf.nextIndex, rf.matchIndex)

				// check if can commit
				for N := len(rf.log); N > rf.commitIndex; N -= 1 {
					if rf.log[N].Term != rf.currentTerm {
						continue
					}

					// count for peer who replicate success. (initialize with 1, include leader self)
					matchCount := 1
					for j := 0; j < len(rf.matchIndex); j += 1 {
						if rf.matchIndex[j] >= N {
							matchCount += 1
						}
					}

					// count * 2 > rf.peers, commit.
					if matchCount*2 > len(rf.peers) {
						rf.setCommitIndex(N)
						break
					}
				}
			} else {
				// send append entries failed.
				// set server nextIndex - 1, try again in next rpc.
				rf.nextIndex[server] = reply.ConflictIndex

				if reply.ConflictTerm != -1 {
					for i := args.PrevLogIndex; i >= 1; i -= 1 {
						if rf.log[i].Term == reply.ConflictTerm {
							rf.nextIndex[server] = i
							break
						}
					}
				}

			}
		}(server)
	}
}

//
// Leader
// send heartbeat (no log AppendEntries)
//
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

//
// Candidate
// Request vote for other server to elect.
//
func (rf *Raft) StartElection() {
	// reset vote count and time out
	rf.votedFor = rf.me
	rf.votedCount = 1 // include self
	rf.currentTerm += 1
	rf.electionTimer.Reset(getRandomTimeout()) // reset election timeout

	//fmt.Printf("[Election] ID=%d start election.", rf.me)
	// start collection votes
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		// start new goroutine to send request vote rpc
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidatedID: rf.me,
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
			// if request server is newest
			if reply.Term > rf.currentTerm {
				rf.switchRole(ROLE_FOLLOWER)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			} else if reply.VoteGranted && rf.currentRole == ROLE_CANDIDATE {
				rf.votedCount += 1
				if rf.votedCount*2 > len(rf.peers) {
					rf.switchRole(ROLE_LEADER)
				}
			}
			rf.mu.Unlock()
		}(server)
	}
}

// RPC:
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatedID int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//fmt.Printf("[RequestVote] ID=%d, Role=%d, Term=%d recived vote request.\n", rf.me, rf.currentRole, rf.currentTerm)

	// if request's term is new, switch to Follower and reset vote and term
	if rf.currentTerm < args.Term {
		rf.switchRole(ROLE_FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	switch rf.currentRole {
	case ROLE_FOLLOWER:
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidatedID
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	case ROLE_CANDIDATE, ROLE_LEADER:
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
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
			// heartbeat timeout
			// leader send heartbeat to followers.
			//fmt.Printf("[Ticker] ID=%d Heartbeat time out.\n", rf.me)
			rf.mu.Lock()
			if rf.currentRole == ROLE_LEADER {
				rf.SendHeartbeat()
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
	elecT := getRandomTimeout()
	rf.electionTimer = time.NewTimer(elecT)
	//fmt.Printf("[Make] ID=%d Election Timer=%dms\n", rf.me, elecT.Milliseconds())
	rf.currentRole = ROLE_FOLLOWER
	rf.currentTerm = 1 // initialize with 1.
	rf.votedFor = -1   // peer id, -1: not vote for any peer.

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.log = make([]LogEntry, 0)
	rf.applyCh = applyCh
	for peer := range rf.peers {
		rf.nextIndex[peer] = 0
		rf.matchIndex[peer] = -1
	}

	rf.mu.Unlock()
	fmt.Printf("[Raft] Peer=%d start...\n", me)

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
// Request AppendEntries
//
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 0: args.term > currentTerm, switch role to follower and update current term to args.term
	// 1: candidate, args.term = current term, switch to follower
	// 2: follower, just update election time out
	// 3: leader, do nothing
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term // update term with new term and switch to follower.
		rf.switchRole(ROLE_FOLLOWER)
	} else if rf.currentTerm == args.Term && rf.currentRole == ROLE_CANDIDATE { // a leader won, switch to follower
		rf.switchRole(ROLE_FOLLOWER)
	}
	// !!! reset electiontimer when recived rpc.
	rf.electionTimer.Reset(getRandomTimeout())
	reply.Term = rf.currentTerm
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
		rf.electionTimer.Reset(getRandomTimeout()) // reset election timeout
		rf.votedFor = -1
	case ROLE_CANDIDATE:
		rf.StartElection()
	case ROLE_LEADER:
		rf.SendHeartbeat()
		rf.heartbeatTimer.Reset(HEARTBEAT_TIMEOUT * time.Millisecond)
	}
}

//
// Get random time.
//
func getRandomTimeout() time.Duration {
	return time.Duration(HEARTBEAT_TIMEOUT*3+rand.Intn(HEARTBEAT_TIMEOUT)) * time.Millisecond
}

//
// Get current time
//
func getCurrentTime() int64 {
	return time.Now().UnixNano()
}

//
// set commit index
//
func (rf *Raft) setCommitIndex(index int) {

}
