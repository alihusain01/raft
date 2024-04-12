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
	"raft/labrpc"
	"sync"
	"sync/atomic"
	"time"
	// "unsafe"
)

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	HEARTBEAT_INTERVAL = 100
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Blank log entry I just made as a placeholder
type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.

	currentTerm   				int
	votedFor      				int
	log           				[]LogEntry
	state          				int32 // 0 - follower, 1 - candidate, 2 - leader
	applyCh       				chan ApplyMsg
	notify 						chan struct{}
	commitIndex 				int
	lastApplied 				int
	electionTimeoutVal    		int
	electionHeartbeat 			time.Time
	appendHeartbeats 			[]time.Time
	nextIndex  					[]int
	matchIndex 					[]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
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
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) changeState(state int32) {
	rf.state = atomic.LoadInt32(&state)

	switch state {
	case FOLLOWER:
	case CANDIDATE:
		rf.votedFor = rf.me
	case LEADER:
		lastIndex := len(rf.log) - 1
		for i := range rf.peers {
			rf.nextIndex[i] = lastIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastIndex
		rf.electionHeartbeat = time.Now()
	default:
		break
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".

	rf.mu.Lock()
	rf.electionHeartbeat = time.Now()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.mu.Unlock()
			return
		}
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			rf.mu.Unlock()
			return
		}
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeState(FOLLOWER)
	}

	lastTerm := rf.log[len(rf.log) - 1].Term
	lastIndex := len(rf.log) - 1

	if lastTerm > args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeState(FOLLOWER)
	rf.electionHeartbeat = time.Now()
	rf.mu.Unlock()

	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.changeState(FOLLOWER)
	rf.electionHeartbeat = time.Now()
	lastIndex := len(rf.log) - 1

	if args.PrevLogIndex > lastIndex {
		reply.Success = false
		reply.NextIndex = lastIndex + 1
		rf.mu.Unlock()
	} else if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		reply.Success = true
		rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
		reply.NextIndex = lastIndex + 1
		rf.mu.Unlock()
	} else {
		reply.Success = false
		term := rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > rf.commitIndex && rf.log[idx].Term == term {
			idx -= 1
		}
		reply.NextIndex = idx + 1
		rf.mu.Unlock()
	}
	
	rf.mu.Lock()

	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notify <- struct{}{}
		}
	}

	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	// Your code here (2B).

	lastIndex := len(rf.log) - 1
	latestIndex := lastIndex + 1

	if isLeader {
		logEntry := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.log = append(rf.log, logEntry)
		rf.matchIndex[rf.me] = latestIndex
	}
	
	for i := range rf.peers {
		rf.appendHeartbeats[i] = time.Now()
	}

	rf.mu.Unlock()
	return latestIndex, term, isLeader
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	rf.notify = make(chan struct{}, 100)
	rf.electionTimeoutVal = rand.Intn(350) + 200
	rf.appendHeartbeats = make([]time.Time, len(rf.peers))
	rf.electionHeartbeat = time.Now()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.electionTimeoutChecker()

	go func() {
		if rf.killed() {
			return
		}
		for {
			select {
			case <-rf.notify:
				if rf.killed() {
					return
				}
				go rf.logReplication()
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			if rf.killed() {
				return
			}
			for {
				time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
				rf.appendToPeers(peer)
			}
		}(i)
	}

	return rf
}

func (rf *Raft) electionTimeoutChecker() {
	for {
		if rf.killed() {
			return
		}

		timeSince := time.Since(rf.electionHeartbeat).Milliseconds()

		if timeSince > int64(rf.electionTimeoutVal) && atomic.LoadInt32(&rf.state) != LEADER || atomic.LoadInt32(&rf.state) == CANDIDATE {
			rf.startElection()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionHeartbeat = time.Now()
	rf.changeState(CANDIDATE)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	rf.mu.Unlock()

	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log) - 1].Term
	}

	numVotes := 1
	grantedVotes := 1
	votesChannel := make(chan bool, len(rf.peers))

	for i := range rf.peers {
		rf.electionHeartbeat = time.Now()

		if i == rf.me {
			continue
		} else {
			go func(ch chan bool, index int) {
				if rf.killed() {
					return
				}
				reply := RequestVoteReply{}
				rf.sendRequestVote(index, &args, &reply)
				ch <- reply.VoteGranted
				if reply.Term > args.Term {
					rf.mu.Lock()
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.changeState(FOLLOWER)
						rf.electionHeartbeat = time.Now()
					}
					rf.mu.Unlock()
				}
			}(votesChannel, i)
		}
		time.Sleep(10 * time.Millisecond)
	}

	for {
		vote := <-votesChannel
		numVotes += 1

		if vote {
			grantedVotes++
		}

		if numVotes == len(rf.peers) || grantedVotes * 2 > len(rf.peers) || numVotes - grantedVotes > len(rf.peers) / 2 {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if grantedVotes <= len(rf.peers) / 2 {
		return
	}

	rf.mu.Lock()

	if rf.currentTerm == args.Term && rf.state == CANDIDATE {
		rf.changeState(LEADER)
	}

	rf.mu.Unlock()

	if atomic.LoadInt32(&rf.state) == LEADER {
		go rf.startHeartbeat()
	}
}

func (rf *Raft) startHeartbeat() {
	for {

		if atomic.LoadInt32(&rf.state) == FOLLOWER || rf.killed() {
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				rf.electionHeartbeat = time.Now()
				continue
			}

			go func(i int) {
				if rf.killed() {
					return
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.log) - 1,
					PrevLogTerm:  rf.log[len(rf.log) - 1].Term,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}

				if len(rf.log) > 0 {
					args.PrevLogIndex = len(rf.log) - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}

				reply := AppendEntriesReply{}

				rf.sendAppendEntries(i, &args, &reply)
			}(i)
		}
		time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
	}
}

func (rf *Raft) appendToPeers(peer int) {
	for {
		rf.mu.Lock()

		if rf.state != LEADER {
			rf.appendHeartbeats[peer] = time.Now()
			rf.mu.Unlock()
			return
		}

		nextIndex := rf.nextIndex[peer]
		lastTerm := rf.log[len(rf.log) - 1].Term
		lastIndex := len(rf.log) - 1

		var logToAppend []LogEntry
		var prevLogIndex, prevLogTerm int

		if nextIndex > lastIndex {
			prevLogIndex = lastIndex
			prevLogTerm = lastTerm
		} else {
			logToAppend = append([]LogEntry{}, rf.log[nextIndex:]...)
			prevLogIndex = nextIndex - 1
			prevLogTerm = rf.log[prevLogIndex].Term
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      logToAppend,
			LeaderCommit: rf.commitIndex,
		}

		rf.appendHeartbeats[peer] = time.Now()
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		responseChannel := make(chan bool, 1)

		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			if rf.killed() {
				return
			}
			responseChannel <- rf.peers[peer].Call("Raft.AppendEntries", args, reply)
			time.Sleep(10 * time.Millisecond)
		}(args, reply)

		select {
		case <- time.After(HEARTBEAT_INTERVAL * time.Millisecond):
			continue
		case ok := <-responseChannel:
			if !ok {
				continue
			}
		}

		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.changeState(FOLLOWER)
			rf.electionHeartbeat = time.Now()
			rf.mu.Unlock()
			return
		}

		if rf.state != LEADER || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			if reply.NextIndex > rf.nextIndex[peer] {
				rf.nextIndex[peer] = reply.NextIndex
				rf.matchIndex[peer] = reply.NextIndex - 1
			}
			if len(args.Entries) > 0 && args.Entries[len(args.Entries) - 1].Term == rf.currentTerm {
				needCommit := false
				for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
					count := 0
					for _, m := range rf.matchIndex {
						if m >= i {
							count += 1
							if count > len(rf.peers) / 2 {
								rf.commitIndex = i
								needCommit = true
								break
							}
						}
					}
					if rf.commitIndex != i {
						break
					}
				}
				if needCommit {
					rf.notify <- struct{}{}
				}
			}
			rf.mu.Unlock()
			return
		}

		if reply.NextIndex > 0 {
			rf.nextIndex[peer] = reply.NextIndex
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) logReplication() {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	var messages []ApplyMsg

	if rf.commitIndex < rf.lastApplied {
		messages = make([]ApplyMsg, 0)
	} else {
		messages = make([]ApplyMsg, 0, rf.commitIndex - rf.lastApplied)
		
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			messages = append(messages, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			})
		}
	}

	rf.mu.Unlock()

	for _, message := range messages {
		rf.applyCh <- message
		rf.mu.Lock()
		rf.lastApplied = message.CommandIndex
		rf.mu.Unlock()
	}

	time.Sleep(10 * time.Millisecond)
}
