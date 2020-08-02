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
	"sync"
	"sync/atomic"
	"time"

	"bytes"

	"../labgob"
	"../labrpc"
)

type serverState int

const (
	follower  serverState = 0
	candidate serverState = 1
	leader    serverState = 2
)

// election timeout
const electionTimeout int = 300
const heartbeatsFreq int = 100

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

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
	state            serverState
	voteGrantedCount int
	logsCount        int
	clusterSize      int

	// Persistent state on all servers
	currentTerm int
	votedFor    int // -1 means null
	logs        []LogEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// channel for signal
	applyCh       chan ApplyMsg
	heartbeatCh   chan bool
	grantVoteCh   chan bool
	electionWinCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, (rf.state == leader)
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		DPrintf("Server %d ----- Term %d: server apply log with index: %d, term: %d, command: %v", rf.me, rf.currentTerm, i, rf.logs[i].Term, rf.logs[i].Command)
		rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: i, CommandTerm: rf.logs[i].Term, Command: rf.logs[i].Command}
	}
	rf.lastApplied = rf.commitIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("Server %d ----- Term %d: server save presist state", rf.me, rf.currentTerm)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		DPrintf("Server %d ----- Term %d: no previous persist ", rf.me, rf.currentTerm)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("Server %d ----- Term %d: server restore from persist fail", rf.me, rf.currentTerm)
	} else {
		DPrintf("Server %d ----- Term %d: server restore from persist ", rf.me, rf.currentTerm)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.logsCount = len(logs)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %d ----- Term %d: recieved vote request with args: %+v", rf.me, rf.currentTerm, *args)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		DPrintf("Server %d ----- Term %d: reply vote request with: %+v", rf.me, rf.currentTerm, *reply)
		return
	} else if args.Term > rf.currentTerm {
		rf.setToFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	// need send grant vote signal even not vote to request candidate to reset election timeout
	// otherwise some case the server would not have chance to promote to candidate
	rf.nonBlockChSend(rf.grantVoteCh, true)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && isCandidateUptoDate(args.LastLogIndex, args.LastLogTerm, rf) {
		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		rf.votedFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
	}
	DPrintf("Server %d ----- Term %d: reply vote request with: %+v", rf.me, rf.currentTerm, *reply)
}

// send signal to channel without block, so that extra signal would not casuse dead lock
func (rf *Raft) nonBlockChSend(ch chan bool, val bool) {
	select {
	case ch <- val:
		DPrintf("Server %d ----- Term %d: signal send", rf.me, rf.currentTerm)
	default:
		DPrintf("Server %d ----- Term %d: signal not send", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) getLastLogInfo() (int, int) {
	lastLogIndex := rf.logsCount - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	return lastLogIndex, lastLogTerm
}

// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
// If the logs end with the same term, then whichever log is longer is more up-to-date.
func isCandidateUptoDate(cLastLogIndex int, cLastLogTerm int, rf *Raft) bool {
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	return (cLastLogTerm > lastLogTerm) || (cLastLogTerm == lastLogTerm && cLastLogIndex >= lastLogIndex)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %d ----- Term %d: recieved append request with arg: %+v", rf.me, rf.currentTerm, *args)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.NextTryIndex = args.PrevLogIndex

	if args.Term < rf.currentTerm {
		DPrintf("Server %d ----- Term %d: request from previouse term, do nothing. reply %+v", rf.me, rf.currentTerm, *reply)
		return
	}

	if args.Term > rf.currentTerm {
		rf.setToFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	rf.nonBlockChSend(rf.heartbeatCh, true)

	// consistency check
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < rf.logsCount && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
		DPrintf("Server %d ----- Term %d: pass consistency check.", rf.me, rf.currentTerm)
		if len(args.Entries) != 0 {
			DPrintf("Server %d ----- Term %d: append Entries.", rf.me, rf.currentTerm)
			rf.logs = rf.logs[:(args.PrevLogIndex + 1)]
			rf.logs = append(rf.logs, args.Entries...)
			rf.logsCount = len(rf.logs)
			rf.persist()
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
			DPrintf("Server %d ----- Term %d: update commit index: %d.", rf.me, rf.currentTerm, rf.commitIndex)
			go rf.applyLogs()
		}

		reply.Success = true
	} else {
		reply.NextTryIndex = rf.findNextRetryIndex(args.PrevLogIndex, args.PrevLogTerm)
	}

	DPrintf("Server %d ----- Term %d: reply append request with: %+v", rf.me, rf.currentTerm, *reply)
}

func (rf *Raft) findNextRetryIndex(prevTryIndex int, prevTryTerm int) int {
	if prevTryIndex >= rf.logsCount {
		return rf.logsCount
	} else if prevTryIndex <= 0 {
		return 1
	} else {
		term := rf.logs[prevTryIndex].Term
		nextryIndex := prevTryIndex - 1
		for ; nextryIndex > 0 && rf.logs[nextryIndex].Term == term; nextryIndex-- {
		}

		return (nextryIndex + 1)
	}
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
	DPrintf("Server %d ----- Term %d: send vote request to server %d with args %+v.", rf.me, args.Term, server, *args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) getVoteFromPeer(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		// DPrintf("Server %d ----- Term %d: no vote reply from server %d for term %d.", rf.me, rf.currentTerm, server, args.Term)
		return
	}

	DPrintf("Server %d ----- Term %d: recieved vote reply from server %d with reply %+v", rf.me, rf.currentTerm, server, *reply)
	if reply.Term < rf.currentTerm {
		// invalid reply: reply to old or have enough vote
		DPrintf("Server %d ----- Term %d: reply invalid", rf.me, rf.currentTerm)
		return
	} else if reply.Term > rf.currentTerm {
		// discover higher term
		rf.setToFollower(reply.Term)
		return
	} else {
		if rf.state == candidate && reply.VoteGranted {
			rf.voteGrantedCount++
			if rf.voteGrantedCount > (rf.clusterSize / 2) {
				rf.nonBlockChSend(rf.electionWinCh, true)
				DPrintf("Server %d ----- Term %d: wins election in term %d.", rf.me, rf.currentTerm, args.Term)
				return
			}
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("Server %d ----- Term %d: send AppendEntries request to server %d with args %+v.", rf.me, args.Term, server, *args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) ackAppendEntrie(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		// DPrintf("Server %d ----- Term %d: no append reply from server %d for term %d.", rf.me, rf.currentTerm, server, args.Term)
		return
	}

	DPrintf("Server %d ----- Term %d: recieved AppendEntries reply from server %d with reply %+v", rf.me, rf.currentTerm, server, *reply)

	if reply.Term < rf.currentTerm {
		// invalid reply: reply to old or have enough vote
		DPrintf("Server %d ----- Term %d: reply invalid", rf.me, rf.currentTerm)
	} else if reply.Term > rf.currentTerm {
		// discover higher term
		rf.setToFollower(reply.Term)
	} else {
		if rf.state == leader {
			if reply.Success {
				nextMatched := args.PrevLogIndex + len(args.Entries)

				if nextMatched <= rf.matchIndex[server] {
					return
				}

				rf.matchIndex[server] = nextMatched
				rf.nextIndex[server] = nextMatched + 1
				DPrintf("Server %d ----- Term %d: append to server %d success, update nextIndex: %d, matchIndex: %d.", rf.me, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])

				possibleCommited := rf.matchIndex[server]
				if possibleCommited > rf.commitIndex && rf.logs[possibleCommited].Term == rf.currentTerm {
					count := 0
					DPrintf("Server %d ----- Term %d: Checkeing possible Commited Index: %d.", rf.me, rf.currentTerm, possibleCommited)
					for _, index := range rf.matchIndex {
						if index >= possibleCommited {
							count++
						}
					}
					if count > (rf.clusterSize / 2) {
						DPrintf("Server %d ----- Term %d: Possible Commited Index in majority: %d.", rf.me, rf.currentTerm, possibleCommited)
						rf.commitIndex = possibleCommited
						go rf.applyLogs()
					}
				}

			} else {
				DPrintf("Server %d ----- Term %d: append to server %d fail, update next try: %d", rf.me, rf.currentTerm, server, reply.NextTryIndex)
				rf.nextIndex[server] = reply.NextTryIndex
			}

		} else {
			DPrintf("Server %d ----- Term %d: not leader in term %d", rf.me, rf.currentTerm, reply.Term)
		}
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := (rf.state == leader)

	if isLeader {
		index = rf.logsCount
		term = rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{term, command})
		rf.persist()
		rf.logsCount++
		rf.matchIndex[rf.me] = index
		DPrintf("Server %d ----- Term %d: add new log with index %d and term %d.", rf.me, rf.currentTerm, index, term)
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != candidate {
		return
	}

	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	DPrintf("Server %d ----- Term %d: start election.", rf.me, rf.currentTerm)

	for server, _ := range rf.peers {
		if server != rf.me {
			reply := RequestVoteReply{}
			go rf.getVoteFromPeer(server, &args, &reply)
		}
	}
}

func (rf *Raft) broadcastAppendRequests() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return
	}
	DPrintf("Server %d ----- Term %d: broadcast Append request.", rf.me, rf.currentTerm)
	for server, _ := range rf.peers {
		if server != rf.me {
			// build AppendEntriesArgs for each peers
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, LeaderCommit: rf.commitIndex}
			nextIndex := rf.nextIndex[server]
			if nextIndex >= 0 && nextIndex <= rf.logsCount {
				args.Entries = rf.logs[rf.nextIndex[server]:]
			}

			args.PrevLogIndex = nextIndex - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}

			reply := AppendEntriesReply{}
			go rf.ackAppendEntrie(server, &args, &reply)
		}
	}

}

func (rf *Raft) setToFollower(term int) {
	DPrintf("Server %d ----- Term %d: set server as follower in term %d", rf.me, rf.currentTerm, term)
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
	rf.voteGrantedCount = 0
	rf.persist()
}

func (rf *Raft) setToCandidate() {
	DPrintf("Server %d ----- Term %d: set as candidate, and increase term by 1", rf.me, rf.currentTerm)
	rf.state = candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist()
	rf.voteGrantedCount = 1
	go rf.startElection()
}

func (rf *Raft) setToLeader() {
	DPrintf("Server %d ----- Term %d: set server as leader in term %d", rf.me, rf.currentTerm, rf.currentTerm)
	rf.state = leader

	initialNextIndex := rf.logsCount
	rf.nextIndex = make([]int, rf.clusterSize)
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = initialNextIndex
	}
	rf.matchIndex = make([]int, rf.clusterSize)
	go rf.broadcastAppendRequests()
}

func (rf *Raft) run() {
	for !rf.killed() {
		rf.mu.Lock()
		term := rf.currentTerm
		DPrintf("Server %d ----- Term %d: run.", rf.me, term)
		switch rf.state {
		case follower:
			DPrintf("Server %d ----- Term %d: run as follower.", rf.me, term)
			rf.mu.Unlock()
			// • If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(electionTimeout)+electionTimeout)):
				DPrintf("Server %d ----- Term %d: timeout in run.", rf.me, term)
				// promote to candidate and start election
				rf.mu.Lock()
				if rf.currentTerm == term {
					DPrintf("Server %d ----- Term %d: promote to candidate.", rf.me, rf.currentTerm)
					rf.state = candidate
				}
				rf.mu.Unlock()
			}

		case candidate:
			rf.setToCandidate()
			DPrintf("Server %d ----- Term %d: run as candidate.", rf.me, rf.currentTerm)
			term = rf.currentTerm
			rf.mu.Unlock()
			// On conversion to candidate, start election:
			// 		• Increment currentTerm
			// 		• Vote for self
			// 		• Reset election timer
			// 		• Send RequestVote RPCs to all other servers
			// • If votes received from majority of servers: become leader
			// • If AppendEntries RPC received from new leader: convert to follower
			// • If election timeout elapses: start new election
			select {
			case <-rf.electionWinCh:
				rf.mu.Lock()
				if rf.currentTerm == term {
					DPrintf("Server %d ----- Term %d: win election, set to leader.", rf.me, rf.currentTerm)
					rf.setToLeader()
				}
				rf.mu.Unlock()
			case <-rf.heartbeatCh:
				rf.mu.Lock()
				DPrintf("Server %d ----- Term %d: back to follower.", rf.me, rf.currentTerm)
				rf.state = follower
				rf.mu.Unlock()
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(electionTimeout)+electionTimeout)):
				DPrintf("Server %d ----- Term %d: timeout in Candiate run.", rf.me, term)
				rf.mu.Lock()
				if rf.currentTerm == term {
					DPrintf("Server %d ----- Term %d: timeout reset to candidate.", rf.me, rf.currentTerm)
					rf.state = candidate
				}
				rf.mu.Unlock()
			}

		case leader:
			DPrintf("Server %d ----- Term %d: run as leader.", rf.me, term)
			rf.mu.Unlock()
			// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; repeat during idle periods to prevent election timeouts
			go rf.broadcastAppendRequests()
			time.Sleep(time.Millisecond * time.Duration(heartbeatsFreq))
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	DPrintf("Server %d: initial new server", me)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = follower
	rf.voteGrantedCount = 0
	rf.clusterSize = len(peers)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	rf.logsCount = len(rf.logs)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.heartbeatCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.electionWinCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	DPrintf("Server %d: initial finished with state: currentTerm: %d.", me, rf.currentTerm)
	go rf.run()

	return rf
}
