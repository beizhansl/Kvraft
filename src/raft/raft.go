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
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

const (
	FOLLOWER  int = 0
	LEADER    int = 1
	CANDIDATE int = 2
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
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

	// Persistent state on all server
	currentTerm int
	votedFor    int
	log         []LogEntry //the first entries used to be lastsnapshotIndex and lastSnapshotTerm!!!

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//which state does server at
	currentState int

	//leader volatile state
	nextIndex  []int
	matchIndex []int

	// Timer
	timeElection  *time.Timer
	timeHeartbeat *time.Timer
	// ApplyLog
	applyCh chan ApplyMsg
	// ApplyIng
	applyIng chan int
}

// return real location in log
func (rf *Raft) getloca(index int) int {
	return index - rf.log[0].Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentState == LEADER
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
func (rf *Raft) CreState() []byte {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm int
	var voteFor int
	var logTemp []LogEntry
	if d.Decode(&currTerm) != nil ||
		d.Decode(&voteFor) != nil || d.Decode(&logTemp) != nil {
		log.Fatal("ReadPersist Fail!")
	} else {
		rf.currentTerm = currTerm
		rf.votedFor = voteFor
		rf.log = logTemp
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// no use to implement
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// Here the application has get mutex a, and wait for mutex r!
// But when we are applying log, we get mutex r and wait for mutex a!
// Binboo!!! DeadLock! And It's what confuses me!
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// when received a snapshot, cat log before index and persist snapshot
	// DeadLock here!
	// println(rf.me, rf.currentState, index, "---snapshot---")
	rf.mu.Lock()
	// println(rf.me, "++++++++++++++++")
	// 1. cat the log
	loca := index - rf.log[0].Index
	if loca < 1 || loca >= len(rf.log) {
		rf.mu.Unlock()
		return
	}
	Term := rf.log[loca].Term
	rf.log = rf.log[loca:]
	// 2. update log[0] to store lastSnapshotIndex and lastSnapshotTerm!
	rf.log[0].Index = index
	rf.log[0].Term = Term
	// 3. store snapshot,it should be save at last

	rf.persister.SaveStateAndSnapshot(rf.CreState(), snapshot)
	SnapshotLog(rf.me, index, rf.currentTerm)
	rf.mu.Unlock()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//first update currentTerm if lower and change to follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// after receive AppendEntries, reset election timer
	rf.resetElectionTime()
	reply.Success = false

	// update CommitIndex if true
	// Here has some problem! we can just update when match
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.log[len(rf.log)-1].Index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.log[len(rf.log)-1].Index
		}
		CommitIndexLog(rf.me, rf.commitIndex, rf.currentTerm)
	}
	// and send applyMsg to server
	if rf.commitIndex > rf.lastApplied && cap(rf.applyIng) != len(rf.applyIng) {
		rf.applyIng <- 1
	}

	// 1. if a earler term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// same term candidate can be follower too
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		if rf.currentState != FOLLOWER {
			rf.ToFollower()
		}
		rf.persist()
	} else if args.Term == rf.currentTerm {
		reply.Term = args.Term
		if rf.currentState != FOLLOWER {
			rf.ToFollower()
			rf.persist()
		}
	}

	// if it's a heartbeat(has no log), no use to do so mach thing
	if args.PrevLogIndex == -1 {
		return
	}
	// 2. if doesn't match
	index := args.PrevLogIndex
	//   2.1 follower's log is shorter
	if index > rf.log[len(rf.log)-1].Index {
		// return false and confictTerm and Index
		// Point!
		reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
		reply.ConflictTerm = -1
		return
	}
	//   2.2 follower's log doesn't match at index
	// Point!
	if rf.getloca(index) < 0 {
		return
	}
	if rf.log[rf.getloca(index)].Term != args.PrevLogTerm {
		// delete it and return false
		reply.ConflictTerm = rf.log[rf.getloca(index)].Term
		reply.ConflictIndex = -1
		for i := rf.getloca(index); i >= 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = rf.log[i+1].Index
				break
			}
		}
		if reply.ConflictIndex == -1 {
			reply.ConflictIndex = rf.log[0].Index
		}
		rf.log = rf.log[0:rf.getloca(index)]
		rf.persist()
		return
	}
	// 3. match! append new LogEntries and return true
	if args.Entries != nil {
		entries := args.Entries
		// need to delete at first!
		rf.log = rf.log[0 : rf.getloca(index)+1]
		rf.log = append(rf.log, entries...)
		rf.persist()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// InstallSnapshot just invoked by leader to send all snapshot to follower
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//Just one lag will receive this
func (rf *Raft) InstallSnapshot(req *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if req.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if req.Term > rf.currentTerm {
		rf.currentTerm = req.Term
		reply.Term = req.Term
		if rf.currentState != FOLLOWER {
			rf.ToFollower()
		}
		rf.persist()
	} else if req.Term == rf.currentTerm {
		reply.Term = req.Term
		if rf.currentState != FOLLOWER {
			rf.ToFollower()
			rf.persist()
		}
	}
	if rf.log[0].Index >= req.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	// delete smaller Index in log
	realpos := rf.getloca(req.LastIncludedIndex)
	// if the term of same index of index and follower is the same, the following can be store
	if realpos < len(rf.log) && rf.log[realpos].Term == req.LastIncludedTerm {
		rf.log = rf.log[realpos:]
	} else {
		rf.log = rf.log[0:1]
	}
	rf.log[0].Index = req.LastIncludedIndex
	rf.log[0].Term = req.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.CreState(), req.Data)
	rf.mu.Unlock()
	rf.applySnapshot(req.Data)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// How can we avoid deadlock?
// Just avoid it by release Lock r!
func (rf *Raft) applylog() {
	// rf.applyIng = true
	for {
		select {
		case <-rf.applyIng:
			rf.mu.Lock()
			if rf.commitIndex <= rf.lastApplied {
				rf.mu.Unlock()
				continue
			}
			lastApplied := rf.lastApplied
			commitIndex := rf.commitIndex
			// println(rf.me, rf.currentState, lastApplied+1, commitIndex, "--------1--1------")
			begin := rf.getloca(lastApplied + 1)
			end := rf.getloca(commitIndex + 1)
			if begin < 0 || end > len(rf.log) {
				rf.mu.Unlock()
				continue
			}
			rflogt := rf.log[begin:end]
			rflog := make([]LogEntry, len(rflogt))
			copy(rflog, rflogt)
			rf.mu.Unlock()
			// we should change index to it's real loaction
			// when we update the i entry
			// Snapshot mybe called, and it want to be saved at first
			// so here we should release lock, and reget it then
			// The best idea is copy!

			for i := 0; i < len(rflog); i++ {
				entry := rflog[i]
				msg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index, SnapshotValid: false}
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied++
				rf.mu.Unlock()
			}
			// println(rf.me, rf.currentState, "--------1--2------")
			rf.mu.Lock()
			ApplyIndexLog(rf.me, rf.lastApplied, rf.currentTerm)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applySnapshot(snapshot []byte) {
	rf.mu.Lock()
	snapshotIndex := rf.log[0].Index
	snapshotTerm := rf.log[0].Term
	rf.mu.Unlock()

	msg := ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: snapshotTerm, SnapshotIndex: snapshotIndex}
	rf.applyCh <- msg

	rf.mu.Lock()
	if rf.lastApplied < snapshotIndex {
		rf.lastApplied = snapshotIndex
	}
	if rf.commitIndex < snapshotIndex {
		rf.commitIndex = snapshotIndex
	}
	ApplySnapshotLog(rf.me, rf.lastApplied, rf.currentTerm)
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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
	defer rf.mu.Unlock()
	rf.resetElectionTime()
	reply.VoteGranted = false
	// we should details the currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	// when Term is bigger than now, begin a new term!
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		if rf.currentState != FOLLOWER {
			rf.ToFollower()
		}
		rf.persist()
	} else if args.Term == rf.currentTerm {
		reply.Term = args.Term
		if rf.currentState != FOLLOWER {
			rf.ToFollower()
			rf.persist()
		}
	}
	reply.Term = rf.currentTerm
	// ok if voteFor is null or candidateid and has uptodate log ,vote!
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if (args.LastLogTerm > rf.log[len(rf.log)-1].Term) || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index) {
			if rf.currentState != FOLLOWER {
				rf.ToFollower()
			}
			rf.votedFor = args.CandidateId
			// save votedFor
			rf.persist()
			reply.VoteGranted = true
			TVLog(rf.me, args.CandidateId, rf.currentTerm)
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	isLeader := false

	// Now kv.mu is locked and it want rf.mu.Lock

	// Your code here (2B).
	// 1. if rf is not a leader return false
	// 2. else begin a agreement ,return immediately
	// Dead Lock!
	// println(rf.me, "---1")
	rf.mu.Lock()
	// println(rf.me, "---2")
	if !rf.killed() && rf.currentState == LEADER {
		index = rf.log[len(rf.log)-1].Index + 1
		term = rf.currentTerm
		isLeader = true
		// just add in log, wait for heartbeat to send it! Has some delay, not so good!
		// we can set timer to 0, send a AppendEntries immediatly!
		newlog := LogEntry{Command: command, Index: index, Term: term}
		rf.log = append(rf.log, newlog)
		rf.timeHeartbeat.Reset(0)
		rf.persist()
		AddAppendEntriesLog(rf.me, index, term)
	}
	rf.mu.Unlock()
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
// Covert it to original AppendEntries, that says we can make followers coordinate in heartbeat
func (rf *Raft) AppendEntriesBroadcast(isheart bool) {
	rf.mu.Lock()
	// every time heartBeat or AppendEntries, update commitIndex
	if rf.commitIndex > rf.lastApplied && cap(rf.applyIng) != len(rf.applyIng) {
		rf.applyIng <- 1
	}
	// every term should be the same term, or some error happened
	currTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	for i := 0; !rf.killed() && i < len(rf.peers); i++ {
		//termnow = rf.currentTerm
		if rf.me == i {
			continue
		}
		IsHeartBeat := true
		// to know if we need to send entries
		if !isheart && rf.log[len(rf.log)-1].Index > rf.matchIndex[i] {
			// println("Now it's a AppendEntries")
			IsHeartBeat = false
		}
		// send  AppendEntries, we can take it outside
		go rf.AServerAppendEntires(i, IsHeartBeat, currTerm, leaderCommit)
	}
	rf.mu.Unlock()
}

//Send Append Entries to a server
func (rf *Raft) AServerAppendEntires(i int, IsHeartBeat bool, currTerm int, leaderCommit int) {
	// Here we need a for to hadle false from Reply,execpt heartbeat
	for !rf.killed() {
		req := &AppendEntriesArgs{
			Term:         currTerm,
			LeaderId:     rf.me,
			Entries:      nil,
			LeaderCommit: leaderCommit,
			PrevLogIndex: -1,
			PrevLogTerm:  -1}
		rf.mu.Lock()
		// it's nessassery? Leader may convert to Follower, so it has chance race!
		if rf.currentState != LEADER {
			rf.mu.Unlock()
			return
		}
		// it should be matchIndex[i] if needed no matter
		if rf.matchIndex[i] < leaderCommit {
			req.LeaderCommit = rf.matchIndex[i]
		}
		if !IsHeartBeat {
			req.PrevLogIndex = rf.nextIndex[i] - 1
			realloc := rf.getloca(req.PrevLogIndex)
			// Here we should send InstallSnapshot!
			realpos := rf.getloca(rf.nextIndex[i])
			if realpos < 1 && rf.log[0].Index > 0 {
				go rf.AServerInstallSnapshot(i, currTerm)
				rf.mu.Unlock()
				return
			} else if realpos < 1 {
				realloc = 0
			}
			//  change to real index in log compaction
			req.PrevLogTerm = rf.log[realloc].Term
			// Entries need to be copy!
			reqEntries := rf.log[realloc+1:]
			req.Entries = make([]LogEntry, len(reqEntries))
			copy(req.Entries, reqEntries)
			// println(req.Entries[0].Index)
		}
		// println("No Problem until now")
		EntriesLength := len(req.Entries)
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()
		ok := rf.sendAppendEntries(i, req, reply)
		rf.mu.Lock()
		if ok {
			if reply.Term < rf.currentTerm {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.ToFollower()
				rf.persist()
				rf.mu.Unlock()
				return
			}
			// for HeartBeat, we no use to change state!
			if IsHeartBeat {
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				// is Success, it means EntriesLentgth Entries been stored
				rf.matchIndex[i] = req.PrevLogIndex + EntriesLength
				rf.nextIndex[i] = req.PrevLogIndex + EntriesLength + 1

				// to find a N , N is bigger than commitIndex and more than half been stored by follower
				// log[0].Index has been commited in log compaction
				lenoflog := len(rf.log) - 1
				for j := lenoflog; j >= 1 && rf.log[j].Term == rf.currentTerm; j-- {
					x := 1
					index := rf.log[j].Index
					if index <= rf.commitIndex {
						break
					}
					find := false
					numofpeers := len(rf.peers)
					for k := numofpeers - 1; k >= 0; k-- {
						if rf.matchIndex[k] >= index {
							x++
							if x > numofpeers/2 {
								find = true
								rf.commitIndex = index
								// update commitIndex, we should set heartbeat to 0
								go rf.AppendEntriesBroadcast(true)
								// rf.timeHeartbeat.Reset(0)
								CommitIndexLog(rf.me, index, rf.currentTerm)
								break
							}
						}
					}
					if find {
						break
					}
				}
				rf.mu.Unlock()
				return
			} else {
				// get false, need to retry, so no return
				// println("The problem is no Success!")
				// update nextIndex by accelarate method!
				// Here we should add call InstallSnapshot in!
				// When there has no index we need in the log, we should call InstallSnapshot!
				next := -1
				Term := reply.ConflictTerm
				Index1 := reply.ConflictIndex
				// We know when nextIndex[i] come to 0(in real position), we should call InstallSnapshot
				// can we find no index we need more quickly ? YES!
				// when we can't find a index that can find a entries match Term X or smaller than Term X in log
				// And now the biggest index in follower is Term X!
				if Term != -1 {
					for k := len(rf.log) - 1; k >= 0; k-- {
						if rf.log[k].Term < Term {
							// next = rf.log[k].Index + 1
							break
						} else if rf.log[k].Term == Term {
							next = rf.log[k].Index + 1
							break
						}
					}
				}
				if next != -1 {
					rf.nextIndex[i] = next
				} else {
					rf.nextIndex[i] = Index1
				}
				// simpliest version
				// rf.nextIndex[i]--
				// if realpos of nextIndex[i] is smaller than 1 -- > call InstallSnapshot!
				if rf.nextIndex[i] < 1 {
					rf.nextIndex[i] = 1
				}
				rf.mu.Unlock()
				continue
			}
		} else { // we can retry or just down, now choose later
			AppendCallFailLog(rf.me, i, currTerm)
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) AServerInstallSnapshot(i int, currTerm int) {
	rf.mu.Lock()
	req := &InstallSnapshotArgs{
		Term:              currTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.persister.ReadSnapshot()}
	reply := &InstallSnapshotReply{}
	lastIndex := req.LastIncludedIndex
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(i, req, reply)
	rf.mu.Lock()
	if ok {
		if reply.Term < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// 1. Leader is out of date
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.ToFollower()
			rf.persist()
			rf.mu.Unlock()
			return
		}
		// if success, the follower has snapshot and we should send last log.
		// so set it's nextIndex to lastIncludedIndex + 1
		rf.matchIndex[i] = lastIndex
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.mu.Unlock()
		return
	} else {
		InstallCallFailLog(rf.me, i, currTerm)
	}
	rf.mu.Unlock()
	return
}

//MaybeError
func (rf *Raft) RequestBroadcast() {
	//first converto Candidate
	var votedfrom *int64
	var x int64 = 1
	votedfrom = &x
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	for i := 0; !rf.killed() && i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			req := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term}
			reply := &RequestVoteReply{}
			RequestBroLog(rf.me, i, rf.currentTerm)
			rf.mu.Unlock()
			ok := rf.sendRequestVote(i, req, reply)
			rf.mu.Lock()
			if ok {
				if reply.Term < rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.ToFollower()
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if rf.currentState == CANDIDATE && reply.VoteGranted {
					atomic.AddInt64(votedfrom, 1)
					// get enough vote
					if *votedfrom > int64(len(rf.peers)/2) {
						rf.ToLeader()
						// just get once ok!
						atomic.StoreInt64(votedfrom, -1000)
						rf.mu.Unlock()
						return
					}
				}
			} else {
				RequestCallFailLog(rf.me, i, term)
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) resetElectionTime() {
	rtime := rand.Int31() % 200
	rf.timeElection.Reset(time.Duration(300+rtime) * time.Millisecond)
}

func (rf *Raft) ToCandidate() {
	rf.currentState = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	// store votedFor and currentTerm
	rf.persist()
	rf.resetElectionTime()
	ToCandidateLog(rf.me, rf.currentTerm)
}

func (rf *Raft) ToLeader() {
	rf.currentState = LEADER
	// initiate some leader state
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	nextIndex := rf.log[len(rf.log)-1].Index + 1
	// println("nextIndex:", nextIndex)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
	}
	rf.timeHeartbeat.Reset(0)
	//lastlogterm := rf.log[len(rf.log)-1].Term
	ToLeaderLog(rf.me, rf.currentTerm)
}

func (rf *Raft) ToFollower() {
	rf.currentState = FOLLOWER
	rf.votedFor = -1
	rf.resetElectionTime()
	ToFollowerLog(rf.me, rf.currentTerm)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		// Heartbeat Time, send all server appendEntries
		case <-rf.timeHeartbeat.C:
			// send heartbeat to every server
			rf.mu.Lock()
			if rf.currentState == LEADER {
				// Actually , we should send AppendEntries with logs if needed
				HBLog(rf.me, rf.currentTerm)
				go rf.AppendEntriesBroadcast(false)
			}
			rf.mu.Unlock()
			rf.timeHeartbeat.Reset(time.Duration(100) * time.Millisecond)
		// electionTimer , begin a new election
		case <-rf.timeElection.C:
			rf.mu.Lock()
			if rf.currentState != LEADER {
				rf.ToCandidate()
				go rf.RequestBroadcast()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// println("number of peer is", len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).
	entry := LogEntry{Term: 0, Index: 0}
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, entry)
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.applyIng = make(chan int, 5)
	rtime := rand.Int63() % 200
	rf.timeElection = time.NewTimer(time.Duration(300+rtime) * time.Millisecond)
	rf.timeHeartbeat = time.NewTimer(100 * time.Millisecond)
	rf.ToFollower()
	// rf.timeHeartbeat.Stop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//rf.log[0] is not 0 tells we hava persisit snapshot once!
	if rf.log[0].Index != 0 {
		snapshot := persister.ReadSnapshot()
		rf.applySnapshot(snapshot)
		ReadSnapshotLog(rf.me, rf.log[0].Index, rf.currentTerm)
	}
	// start ticker goroutine to start elections
	go rf.applylog()
	go rf.ticker()

	return rf
}
