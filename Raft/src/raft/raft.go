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
	"Raft/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"Raft/labgob"
	"Raft/labrpc"
)

const (
	Follower  = iota // 0
	Candidate        // 1
	Leader           // 2
)

const (
	MinElectionTimeout = 300 * time.Millisecond
	MaxElectionTimeout = 600 * time.Millisecond
	HeartbeatInterval  = 100 * time.Millisecond
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entries struct {
	Term    int
	Command interface{}
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyChan chan ApplyMsg

	state         int       // Whether Leader, Candidate, or Follower (initial value is Follower)
	lastHeartbeat time.Time // When the last heart bet was
	votesReceived int       // Number of votes received during the current election. (initial value is 0)
	majority      int       // Number of necessary votes for becoming Leader

	// Persistent state on all servers:
	currentTerm int       // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int       // candidateId that received vote in current term (or null if none)
	log         []Entries // each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//	Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Snapshot raw bytes of the snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
	stateMachineState []byte
}

type AppendEntriesArgs struct {
	Term         int       // leader’s term
	LeaderId     int       // so follower can redirect clients
	PrevLogIndex int       // index of log entry immediately preceding new ones
	PrevLogTerm  int       // term of prevLogIndex entry
	Entries      []Entries // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int       // leader’s commitIndex
	FollowerId   int
}

type AppendEntriesReply struct {
	Term                  int  // currentTerm, for leader to update itself
	Success               bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm          int  // term of the conflicting entry
	ConflictIndex         int  // first index it stores for that term
	AppendedEntriesLength int  // The length of appended entries
}

type InstallSnapshotArgs struct {
	Term              int // leader’s Term
	LeaderId          int //so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of last Included Index

	Data []byte // raw bytes of the snapshot
	//offset byte offset where chunk is positioned in the snapshot file
	//Done bool  // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // current Term, for leader to update itself
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)

	// Save snapshot
	_ = e.Encode(rf.lastIncludedIndex)
	_ = e.Encode(rf.lastIncludedTerm)

	raftState := w.Bytes()
	var data []byte

	if len(rf.stateMachineState) != 0 {
		data = rf.stateMachineState
	} else {
		data = nil
	}
	rf.persister.Save(raftState, data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var curTerm int
	var votedFor int
	var log []Entries

	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&curTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		return
	} else {

		rf.log = log
		rf.currentTerm = curTerm
		rf.votedFor = votedFor

		// read snapshot
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.stateMachineState = rf.persister.ReadSnapshot()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.debugPrint("Snapshot: ")
	if index < rf.lastIncludedIndex {
		return
	}

	DPrintf("Before Snapshotiing server(%v) %v: index = %v, rf.lastIncludedIndex = %v, "+
		"rf.lastIncludedTerm = %v, logLen = %v, log = %v\n ",
		rf.StatusName(), rf.me, index, rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log), rf.log)

	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex].Term
	rf.stateMachineState = snapshot

	newLogStart := index - rf.lastIncludedIndex
	newLog := make([]Entries, len(rf.log)-newLogStart)
	copy(newLog, rf.log[newLogStart:])
	rf.log = newLog

	rf.lastIncludedIndex = index
	// append dummy element as it was before.
	DPrintf("rf.log = %v", rf.log)
	rf.log[0].Term = 0
	rf.log[0].Command = nil

	DPrintf("Snapshotiing server(%v) %v: rf.lastIncludedIndex = %v, rf.lastIncludedTerm = %v,"+
		" logLen = %v, log = %v\n ",
		rf.StatusName(), rf.me, index, rf.lastIncludedTerm, len(rf.log), rf.log)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Your code here (3A, 3B).

	rf.debugPrint("RequestVote: ")
	// 1.Reply false if currentTerm > vote request term
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		DPrintf("me = %v, status = %v, term = %v election request term %v from server %v \n",
			rf.me, rf.StatusName(), rf.currentTerm, args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 2. If ((votedFor is -1) or (votedFor is candidateId)), and
	// 	  candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	// should we update time for VoteGranted = false ?? <- looks like yes
	if reply.VoteGranted {
		rf.lastHeartbeat = time.Now()
	}
	reply.Term = rf.currentTerm
	return
}

// 5.4.1 (Election restriction) how to determine which of two logs is more up-to-date.
func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	rf.debugPrint("isUpToDate: ")
	// Get the term of the last log entry in the server's own log
	myLastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	myLastLogTerm := rf.log[len(rf.log)-1].Term
	if myLastLogTerm == 0 {
		myLastLogTerm = rf.lastIncludedTerm
	}

	// Rule 1: Compare the terms
	if lastLogTerm > myLastLogTerm {
		return true
	} else if lastLogTerm < myLastLogTerm {
		return false
	}

	// Rule 2: If terms are the same, compare the log indices (lengths)
	return lastLogIndex >= myLastLogIndex
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := false
	for !ok && !rf.killed() {
		//rf.mu.Lock()
		//DPrintf("Server %d sending RequestVote to %d in term %d", rf.me, server, rf.currentTerm)
		//rf.mu.Unlock()
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	index := rf.lastIncludedIndex + len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (3B).

	// if this Raft isn't leader return immediately.
	if !isLeader {
		return index, term, isLeader
	}
	rf.debugPrint("Start: ")
	currentLogEntre := Entries{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, currentLogEntre)

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	DPrintf("%v %v: new entrie - (%v): term = %v, matchIndex[] = %v, nextIndex[] = %v, commitIndex = %v, "+
		"rf.lastIncludedIndex = %v, logLen = %v, index = %v\n",
		rf.StatusName(), rf.me, command, rf.currentTerm, rf.matchIndex, rf.nextIndex, rf.commitIndex,
		rf.lastIncludedIndex, len(rf.log), index)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func heartBeatTimeHasPassed(lastHeartbeat time.Time) bool {
	elapsed := time.Since(lastHeartbeat)
	electionTimeout := MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout)))
	return elapsed >= electionTimeout
}

func (rf *Raft) sendIntoChannel() {
	// this is for the first time, 0 logs are commited.
	for rf.killed() == false {

		rf.mu.Lock()
		rf.debugPrint("sendIntoChannel: ")
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		log := rf.log
		lastIncludedIndex := rf.lastIncludedIndex
		stateMachineState := rf.stateMachineState
		lastIncludedTerm := rf.lastIncludedTerm
		rf.mu.Unlock()

		if lastApplied-lastIncludedIndex < 0 {
			rf.applyChan <- ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      stateMachineState,
				SnapshotIndex: lastIncludedIndex,
				SnapshotTerm:  lastIncludedTerm,
			}
			lastApplied = lastIncludedIndex
			rf.mu.Lock()
			DPrintf("sending into channel(Snapshot) <- logindex[%v], loglen = %v, (lastApplied = %v, "+
				"lastIncludedIndex = %v),  %v = %v: matchIndex[] = %v\n",
				lastApplied+1-lastIncludedIndex, len(rf.log), lastApplied, lastIncludedIndex,
				rf.StatusName(), rf.me, rf.matchIndex)
			rf.mu.Unlock()
		}

		if commitIndex > lastApplied {
			for i := lastApplied + 1; i <= commitIndex; i++ {
				var command interface{}

				rf.mu.Lock()
				DPrintf("sending into channel(index = 0) <- logindex[%v], loglen = %v, (i = %v, "+
					"lastIncludedIndex = %v),  %v = %v: matchIndex[] = %v\n",
					i-lastIncludedIndex, len(rf.log), i, lastIncludedIndex, rf.StatusName(), rf.me, rf.matchIndex)
				rf.mu.Unlock()

				command = log[i-lastIncludedIndex].Command

				if command != nil {
					rf.applyChan <- ApplyMsg{
						SnapshotValid: false,
						CommandValid:  true,
						Command:       command,
						CommandIndex:  i,
					}
					rf.mu.Lock()
					DPrintf("sending into channel(Log) <- logindex[%v], loglen = %v, (i = %v, "+
						"lastIncludedIndex = %v),  %v = %v: matchIndex[] = %v\n",
						i-lastIncludedIndex, len(rf.log), i, lastIncludedIndex, rf.StatusName(), rf.me, rf.matchIndex)
					rf.mu.Unlock()
				}
			}

			// TODO es lock arasworad aris?
			rf.mu.Lock()
			rf.lastApplied = commitIndex
			DPrintf("%v - %v : logs are sent into channel. lastApplied = %v. commitIndex = %v, len(rf.log) = %v",
				rf.StatusName(), rf.me, rf.lastApplied, rf.commitIndex, len(rf.log))
			rf.mu.Unlock()
		}
		time.Sleep(HeartbeatInterval / 2)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		state := rf.state
		lastHeartbeat := rf.lastHeartbeat
		rf.mu.Unlock()

		if state != Leader {
			// Follower or Candidate: check if election timeout occurred
			if heartBeatTimeHasPassed(lastHeartbeat) {
				rf.startElection()
			}
		}
		ms := (time.Duration(rand.Int63() % 50)) * time.Millisecond
		time.Sleep(ms)
	}
}

func (rf *Raft) leaderTicker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Leader sends heartbeats

		if rf.isLeader() {
			// Leader: send heartbeat (AppendEntries) to all followers
			rf.sendHeartbeats()
		}
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return false
	}
	return true
}

func (rf *Raft) sendHeartbeats() {
	rf.debugPrint("sendHeartbeats: ")
	if !rf.isLeader() {
		return
	}
	rf.mu.Lock()
	term := rf.currentTerm
	me := rf.me
	peers := rf.peers

	nextIndex := make([]int, len(rf.nextIndex))
	copy(nextIndex, rf.nextIndex)

	logLength := len(rf.log)
	log := make([]Entries, logLength)
	copy(log, rf.log)
	commitIndex := rf.commitIndex
	lastIncludedIndex := rf.lastIncludedIndex
	lastIncludedTerm := rf.lastIncludedTerm
	stateMachineState := rf.stateMachineState
	rf.mu.Unlock()

	for i := range peers {
		if i != me {
			go func(server int) {
				// check leader
				if !rf.isLeader() {
					DPrintf("Server %v is no longer leader. It was tring to send heartbeat to server %v\n",
						me, server)
					return
				}
				var prevLogTerm int
				if nextIndex[server]-1 <= lastIncludedIndex {
					prevLogTerm = lastIncludedTerm
				} else {
					prevLogTerm = log[nextIndex[server]-lastIncludedIndex-1].Term
				}

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     me,
					PrevLogIndex: nextIndex[server] - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      nil, // empty for heartbeat
					LeaderCommit: commitIndex,
					FollowerId:   server,
				}

				if nextIndex[server] < logLength+lastIncludedIndex {
					// Send snapshot if logIdx is negative or zero
					logIdx := nextIndex[server] - lastIncludedIndex
					if logIdx <= 0 {
						DPrintf("leader %d should send snapshot to server %d", me, server)
						snapshotArgs := InstallSnapshotArgs{
							Term:              term,
							LeaderId:          me,
							LastIncludedIndex: lastIncludedIndex,
							LastIncludedTerm:  lastIncludedTerm,
							Data:              stateMachineState,
						}
						snapshotReply := InstallSnapshotReply{}
						if rf.sendSnapshot(server, &snapshotArgs, &snapshotReply) {
							rf.handleSnapshotReply(server, &snapshotArgs, &snapshotReply)
						}
						return
					}
					// Todo copy??
					args.Entries = log[logIdx:]
				}

				reply := AppendEntriesReply{}
				rf.mu.Lock()
				DPrintf("Server %v (status - '%v') sending append entrie to %v", me, rf.StatusName(), server)
				rf.mu.Unlock()
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.handleAppendEntriesReply(server, &args, &reply)
				}
			}(i)
		}
	}
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debugPrint("InstallSnapshot: ")

	DPrintf("%v %v InstallSnapshot() reply from server %v: LastIncludedIndex = %v, "+
		"LastIncludedIndex = %v, Term = %v",
		rf.StatusName(), rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedIndex, args.Term)

	//1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%v %v InstallSnapshot() - args.Term < rf.currentTerm - reply from server %v: "+
			"LastIncludedIndex = %v, LastIncludedIndex = %v, Term = %v",
			rf.StatusName(), rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedIndex, args.Term)

		reply.Term = rf.currentTerm
		return
	}

	// 2. Create new snapshot file if first chunk(offset is 0)

	// 3. Write data into snapshot file at given offset

	// 4. Reply and wait for more data chunks if done is false

	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	rf.stateMachineState = args.Data

	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry,retain log entries following it and reply

	// 7. Discard the entire log
	rf.log = append(make([]Entries, 0), Entries{Term: 0, Command: nil})

	// 8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	reply.Term = rf.currentTerm
	rf.persist()
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) handleSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.debugPrint("handleSnapshotReply: ")
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.lastHeartbeat = time.Now()
		return
	}
	rf.matchIndex[server] = max(rf.lastIncludedIndex, rf.matchIndex[server])
	rf.nextIndex[server] = rf.matchIndex[server] + 1
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.debugPrint("handleAppendEntriesReply: ")
	DPrintf("%v %d processing appendEntries reply from server %d: term=%d, prevLogIndex=%d, nextIndex = %v, matchIndex = %v, log=%v",
		rf.StatusName(), rf.me, args.FollowerId, args.Term, args.PrevLogIndex, rf.nextIndex, rf.matchIndex, rf.log)

	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.lastHeartbeat = time.Now()
		return
	}

	if reply.Success {
		addedLogLength := len(args.Entries)
		if reply.AppendedEntriesLength != -1 {
			addedLogLength = reply.AppendedEntriesLength
		}
		rf.nextIndex[server] = args.PrevLogIndex + addedLogLength + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.updateCommitIndex()
	} else {
		if reply.ConflictTerm != -1 {
			conflictIndex := reply.ConflictIndex
			if conflictIndex > len(rf.log)+rf.lastIncludedIndex {
				print("line 734: aq ar unda modiodes wesit")
				conflictIndex = len(rf.log) + rf.lastIncludedIndex
			}

			for conflictIndex > 0 &&
				conflictIndex > rf.lastIncludedIndex &&
				rf.log[conflictIndex-rf.lastIncludedIndex-1].Term != reply.ConflictTerm {

				conflictIndex--
			}

			if conflictIndex == 1 {
				rf.nextIndex[server] = reply.ConflictIndex
			} else {
				rf.nextIndex[server] = max(rf.lastIncludedIndex+1, conflictIndex)
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.debugPrint("updateCommitIndex: ")
	matchIndices := make([]int, len(rf.matchIndex))
	copy(matchIndices, rf.matchIndex)
	sort.Ints(matchIndices)

	// Find the majority index (in sorted array it is median) e.g. 1 1 1 2 4 4 4 -> index = 3
	N := matchIndices[(len(matchIndices) / 2)]

	// Check if log on this index is in same term
	if N-rf.lastIncludedIndex >= 0 && rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm {
		rf.commitIndex = N
		DPrintf("Server %d's matchIndex[] = %v, commit index is - %v\n", rf.me, rf.matchIndex, rf.commitIndex)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.debugPrint("startElection: ")

	DPrintf("Server %d starting election in term %d", rf.me, rf.currentTerm)
	// aq ar vici unda tu ara es killed
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate // I am candidate leader
	rf.currentTerm += 1  // incr Term
	rf.votedFor = rf.me  // Vote for myself
	rf.votesReceived = 1 // Vote for self
	rf.persist()

	term := rf.currentTerm
	me := rf.me
	peers := rf.peers
	logLength := len(rf.log)
	var lastLogTerm int
	if logLength == 1 {
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.log[logLength-1].Term
	}
	lastIncludedIndex := rf.lastIncludedIndex

	// Reset election timer
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()

	// Send RequestVote RPCs to all other servers
	for i := range peers {
		if i != me {
			go func(server int) {
				args := RequestVoteArgs{
					Term:         term,
					CandidateId:  me,
					LastLogIndex: logLength + lastIncludedIndex - 1,
					LastLogTerm:  lastLogTerm, // term = 0 if lastLogIndex is 0 (it is first log with Entre {0, nil})
				}
				reply := RequestVoteReply{}

				// TODO es sheidzleba rom race condition iyos?
				if rf.sendRequestVote(server, &args, &reply) {
					rf.handleVoteReply(reply)
				}
			}(i)
		}
	}
}

func (rf *Raft) handleVoteReply(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.debugPrint("handleVoteReply: ")

	// TODO es vafshe rashi mchirdeba???
	//if rf.state != Candidate || rf.currentTerm != reply.Term {
	//	DPrintf("Server %d (status - %v) currentTerm = %v, reply.Term = %v\n", rf.me, rf.StatusName(), rf.currentTerm, reply.Term)
	//	return
	//}

	// 1. Step down to follower if the reply's term is higher than the current term
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votesReceived = 0
		rf.votedFor = -1
		DPrintf("Server %d isn't leader anymore (1.Step down to follower if the reply's "+
			"term is higher than the current term)\n", rf.me)
		return
	}

	// 2. If the vote was granted, increment the votes received
	if reply.VoteGranted && reply.Term == rf.currentTerm {
		rf.votesReceived++
		DPrintf("Server :%d: recieved vote. total votes = %d (2. If the vote was granted, "+
			"increment the votes received)\n", rf.me, rf.votesReceived)

		// Check if I've become leader
		if rf.votesReceived >= rf.majority && rf.state == Candidate {
			rf.state = Leader
			DPrintf("Server :%d: became leader. log = %d, matchindex = %d \n", rf.me, rf.log, rf.matchIndex)
			rf.initializeLeader()
			return
		}
	}
}

func (rf *Raft) initializeLeader() {
	rf.debugPrint("initializeLeader: ")
	// Set nextIndex and matchIndex for all followers
	nextLogIndex := rf.lastIncludedIndex + len(rf.log)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = nextLogIndex // Initialize nextIndex to length of logs
		rf.matchIndex[i] = 0           // No log entries have been replicated yet
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	DPrintf("Leader %d (status - '%v') sending heartbeat to %d with commitIndex %d in term %v",
		rf.me, rf.StatusName(), server, rf.commitIndex, rf.currentTerm)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.debugPrint("AppendEntries: ")

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("%v %v rejected AppendEntries from %v: term %d < currentTerm %d",
			rf.StatusName(), rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// Update current term if the args term is greater than the current term
	if args.Term > rf.currentTerm {
		DPrintf("%v %d updating term from %d to %d and becoming follower",
			rf.StatusName(), rf.me, rf.currentTerm, args.Term)
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm(§5.3)
	if rf.lastIncludedIndex+len(rf.log) <= args.PrevLogIndex {
		DPrintf("%v %v : Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm",
			rf.StatusName(), rf.me)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.lastHeartbeat = time.Now()

		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + len(rf.log)
		return
	}

	// 3. If an existing entry conflicts with a new one(same index but different terms),
	// 		delete the existing entry and all that follow it(§5.3)

	getTerm := rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
	if getTerm == 0 {
		getTerm = rf.lastIncludedTerm
	}

	if getTerm != args.PrevLogTerm {
		//DPrintf("Server %v log mismatch: expected term %v at index %v, got term %v\n",
		//	rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogIndex, args.PrevLogTerm)
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term

		// Todo copy ??
		rf.log = rf.log[:args.PrevLogIndex-rf.lastIncludedIndex]
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.lastHeartbeat = time.Now()

		// Find the first index of the conflicting term
		conflictIndex := args.PrevLogIndex - rf.lastIncludedIndex
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex + rf.lastIncludedIndex

		DPrintf("%v %v's log = %v. conflictIndex = %v\n",
			rf.StatusName(), rf.me, rf.log, conflictIndex+rf.lastIncludedIndex)
		return
	}

	reply.AppendedEntriesLength = -1

	// 4.Append any new entries not already in the log
	//rf.log = append(rf.log[:args.PrevLogIndex-rf.lastIncludedIndex+1], args.Entries...)

	// 4. Append new entries only if they do not match existing terms
	for i, entry := range args.Entries {
		targetIndex := args.PrevLogIndex + 1 + i

		if targetIndex >= len(rf.log)+rf.lastIncludedIndex {
			// If the current log index is beyond the existing log, append the new entry
			rf.log = append(rf.log, args.Entries[i:]...)
			reply.AppendedEntriesLength = len(args.Entries) - i
			break
		} else if rf.log[targetIndex-rf.lastIncludedIndex].Term != entry.Term {
			// If the terms do not match, replace the existing entry with the new one
			rf.log = append(rf.log[:targetIndex-rf.lastIncludedIndex], args.Entries[i:]...)
			reply.AppendedEntriesLength = len(args.Entries) - i
			break
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)+rf.lastIncludedIndex-1)
	}

	rf.lastHeartbeat = time.Now()

	reply.Term = rf.currentTerm
	reply.Success = true

	DPrintf("AppendEntrie success. %v me = %v, my term = %v, leader term %v leader %v log = %v\n",
		rf.StatusName(), rf.me, rf.currentTerm, args.Term, args.LeaderId, rf.log)

	return
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyChan = applyCh
	// Initialize the node as a Follower at the beginning
	rf.state = Follower
	rf.lastHeartbeat = time.Now()
	rf.votesReceived = 0
	rf.majority = len(peers)/2 + 1

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(make([]Entries, 0), Entries{Term: 0, Command: nil})

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize snapshot
	rf.lastIncludedTerm = 0
	rf.lastIncludedIndex = 0
	rf.stateMachineState = make([]byte, 0)

	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	go rf.ticker()
	go rf.leaderTicker()
	go rf.sendIntoChannel()

	DPrintf("Server %v started\n", rf.me)

	return rf
}

func (rf *Raft) StatusName() string {
	if rf.state == Follower {
		return "Follower"
	}
	if rf.state == Candidate {
		return "Candidate"
	}
	return "Leader"
}

func (rf *Raft) debugPrint(f string) {
	DPrintf(f+" %v %v, term = %v, commitIndex = %v, lastIncludedIndex = %v, lastIncludedTerm = %v, nextIndex = %v, matchIndex = %v, log = %v",
		rf.StatusName(), rf.me, rf.currentTerm, rf.commitIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.nextIndex, rf.matchIndex, rf.log)
}
