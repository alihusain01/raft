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

	// Persistent state on all servers
	currentTerm   int
	votedFor      int
	log           []LogEntry
	role          int32 // 0 - follower, 1 - candidate, 2 - leader
	applyCh       chan ApplyMsg
	notifyApplyCh chan struct{}

	// volatile state on all servers
	commitIndex int
	lastApplied int

	electionTimeoutVal    int
	lastElectionHeartbeat time.Time
	appendEntryHeartbeats []time.Time

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".

	rf.mu.Lock()

	rf.lastElectionHeartbeat = time.Now()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		// print("false vote reason: 0 \n")
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
			// print("false vote reason: 1 \n")
			rf.mu.Unlock()
			return
		}
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		// print("Server: ", rf.me, " changing to follower in request vote \n")
		rf.changeRole(FOLLOWER)
	}

	lastLogTerm := rf.log[len(rf.log)-1].Term
	lastLogIndex := len(rf.log) - 1

	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		// print("false vote reason: 2 \n")
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(FOLLOWER)
	rf.lastElectionHeartbeat = time.Now()

	rf.mu.Unlock()

	return

}

// func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	// Your code here (2A, 2B).
// 	// Read the fields in "args",
// 	// and accordingly assign the values for fields in "reply".

// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	print("Recipient: ", rf.me, " Term: ", rf.currentTerm, " received heartbeat from ", args.LeaderId, " Term: ", args.Term, "Entries: ", args.Entries,  "\n")

// 	rf.lastElectionHeartbeat = time.Now()

// 	// lastLogIndex := len(rf.log) - 1

// 	if args.Term >= rf.currentTerm {
// 		rf.currentTerm = args.Term
// 		atomic.StoreInt32(&rf.role, 0)
// 		reply.Success = true
// 	}

// 	if args.Term < rf.currentTerm {
// 		reply.Term = rf.currentTerm // this might be different
// 		reply.Success = false
// 		return
// 	}
// }

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".

	rf.mu.Lock()
	reply.Term = rf.currentTerm

	// print("Recipient: ", rf.me, " Term: ", rf.currentTerm, " received heartbeat from ", args.LeaderId, " Term: ", args.Term, "\n")

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.changeRole(FOLLOWER)
	rf.lastElectionHeartbeat = time.Now()

	_, lastLogIndex := rf.lastLogTermIndex()

	if args.PrevLogIndex > lastLogIndex { // leader has a larger log
		// print("BP0 \n")
		reply.Success = false
		reply.NextIndex = lastLogIndex + 1
		rf.mu.Unlock()
	} else if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		// print("BP1 \n")
		reply.Success = true
		rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
		reply.NextIndex = lastLogIndex + 1
		rf.mu.Unlock()
	} else {
		// print("BP2 \n")
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
			rf.notifyApplyCh <- struct{}{}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) appendPeerEntries(peerIndex int) {
	for {
		rf.mu.Lock()

		// If the server is no longer the leader, exit the loop
		if rf.role != LEADER {
			rf.appendEntryHeartbeats[peerIndex] = time.Now() // Reset the heartbeat timer
			rf.mu.Unlock()
			// print("returning bc not a leader \n")
			return
		}

		nextIdx := rf.nextIndex[peerIndex]
		lastLogTerm, lastLogIdx := rf.lastLogTermIndex()

		var logToAppend []LogEntry

		var prevLogIndex, prevLogTerm int

		if nextIdx > lastLogIdx { // when the peer has more entries than the leader has
			prevLogIndex = lastLogIdx
			prevLogTerm = lastLogTerm
		} else { // when the leader has more or equal entries than the peer has
			logToAppend = append([]LogEntry{}, rf.log[nextIdx:]...)
			prevLogIndex = nextIdx - 1
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

		rf.appendEntryHeartbeats[peerIndex] = time.Now() // Reset the heartbeat timer
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		responseChannel := make(chan bool, 1)

		// Send the append entries request to the peer
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			responseChannel <- rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
		}(args, reply)

		select {
		case <- time.After(100 * time.Millisecond):
			continue
		case ok := <-responseChannel:
			if !ok {
				continue // retry the appendRPC in next iteration
			}
		}

		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			// print("appendpeerentries\n")
			rf.changeRole(FOLLOWER)
			rf.lastElectionHeartbeat = time.Now()
			rf.mu.Unlock()
			return
		}

		if rf.role != LEADER || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// update peer's nextIndex to match the leader's log
			if reply.NextIndex > rf.nextIndex[peerIndex] {
				rf.nextIndex[peerIndex] = reply.NextIndex
				rf.matchIndex[peerIndex] = reply.NextIndex - 1
			}

			// commit entries of the current term
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
				toCommit := false
				for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
					count := 0
					for _, m := range rf.matchIndex {
						if m >= i {
							count += 1
							if count > len(rf.peers)/2 {
								rf.commitIndex = i
								toCommit = true
								break
							}
						}
					}
					if rf.commitIndex != i {
						break
					}
				}
				if toCommit {
					rf.notifyApplyCh <- struct{}{}
				}
			}
			rf.mu.Unlock()
			return
		}

		if reply.NextIndex > 0 {
			rf.nextIndex[peerIndex] = reply.NextIndex
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) startApplyLogs() {
	if rf.killed() {
		// print("Server: ", rf.me, " Role: ", rf.role, " Killing startHeartbeat \n")
		return
	}
	rf.mu.Lock()
	var msgs []ApplyMsg
	if rf.commitIndex < rf.lastApplied {
		msgs = make([]ApplyMsg, 0)
	} else {
		if atomic.LoadInt32(&rf.role) == 2 {
			print("rf.me: ", rf.me, " is the leader rn\n")
		}
		print("rf.me: ", rf.me, " rf.lastApplied + 1: ", rf.lastApplied+1, " rf.commitIndex: ", rf.commitIndex, "\n")

		msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			})
		}
	}
	rf.mu.Unlock()

	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.mu.Lock()
		rf.lastApplied = msg.CommandIndex
		rf.mu.Unlock()
	}
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
	isLeader := rf.role == LEADER
	_, lastIndex := rf.lastLogTermIndex() // last index of the log

	index := lastIndex + 1
	// print("index: ", index, "\n")

	// If not a leader return instantly
	if isLeader {

		// Your code here (2B).
		logEntry := LogEntry{
			Term:    term,
			Command: command,
		}
		rf.log = append(rf.log, logEntry)
		rf.matchIndex[rf.me] = index
	}
	
	for i := range rf.peers {
		rf.appendEntryHeartbeats[i] = time.Now() // Reset the heartbeat timer
	}

	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.log[len(rf.log)-1].Term
	index := len(rf.log) - 1
	return term, index
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
	// atomic.StoreInt32(&rf.isLeader, 0)
	// print("Server ", rf.me, " has been killed. Role:", rf.role, " Commit Index: ", rf.commitIndex, "\n")
	// for l := range rf.log {
	// 	print("Log entry ", l, " Term: ", rf.log[l].Term, " Command: ", rf.log[l].Command, "\n")
	// }
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) changeRole(role int32) {
	rf.role = atomic.LoadInt32(&role)

	switch role {
	case FOLLOWER:
		// print("Server ", rf.me, " is now a follower\n")
	case CANDIDATE:
		rf.votedFor = rf.me
		// print("Server ", rf.me, " is now a candidate for term ", rf.currentTerm, "\n")
	case LEADER:
		_, lastLogIndex := rf.lastLogTermIndex()
		for i := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
		rf.lastElectionHeartbeat = time.Now()

		print("Server ", rf.me, " is now a leader for term", rf.currentTerm, "\n")
	default:
		break
	}
}

func (rf *Raft) electionTimeoutChecker() {
	for {
		// If the server is dead, exit the loop
		if rf.killed() {
			return
		}

		timeSince := time.Since(rf.lastElectionHeartbeat).Milliseconds()

		if timeSince > int64(rf.electionTimeoutVal) && atomic.LoadInt32(&rf.role) != 2 || atomic.LoadInt32(&rf.role) == 1 {
			// Start an election
			// print("Server ", rf.me, " timed out. Role: ", rf.role, " Time since: ", timeSince, ". Election timeout: ", rf.electionTimeoutVal, "\n")
			rf.startElection()
			// break
		}

	}
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.lastElectionHeartbeat = time.Now()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastElectionHeartbeat = time.Now()
	// rf.electionTimeoutVal = rand.Intn(150) + 150
	rf.changeRole(CANDIDATE)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	rf.mu.Unlock()

	if len(rf.log) > 0 {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}

	voteCount := 1
	grantedCount := 1

	votesChannel := make(chan bool, len(rf.peers))

	for i := range rf.peers {
		rf.lastElectionHeartbeat = time.Now()
		// print("rf.me: ", rf.me, "\n")
		// print("i: ", i, "\n")

		if i == rf.me {
			continue
		} else {
			go func(ch chan bool, index int) {
				// print("Server ", rf.me, " Role: ", rf.role, " sending vote request to server ", index, " for term ", rf.currentTerm, "\n")
				reply := RequestVoteReply{}
				rf.sendRequestVote(index, &args, &reply)
				// print("Server ", rf.me, " Role: ", rf.role, " received vote from server ", index, " granted: ", reply.VoteGranted, " Term: ", reply.Term, "\n")
				ch <- reply.VoteGranted
				if reply.Term > args.Term {
					rf.mu.Lock()
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.changeRole(FOLLOWER)
						rf.lastElectionHeartbeat = time.Now()
					}
					rf.mu.Unlock()
				}
			}(votesChannel, i)
		}
	}

	for {
		vote := <-votesChannel
		voteCount += 1
		if vote {
			grantedCount++
		}
		if voteCount == len(rf.peers) || grantedCount*2 > len(rf.peers) || voteCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		return
	}

	rf.mu.Lock()
	if rf.currentTerm == args.Term && rf.role == CANDIDATE {
		rf.changeRole(LEADER)
	}
	rf.mu.Unlock()

	if atomic.LoadInt32(&rf.role) == 2 {
		go rf.startHeartbeat()
	}
}

func (rf *Raft) startHeartbeat() {
	for {
		// print("I am ", rf.me, ". My role is ", rf.role, "\n")
		if atomic.LoadInt32(&rf.role) == FOLLOWER || rf.killed() {
			// print("Server: ", rf.me, " Role: ", rf.role, " Killing startHeartbeat \n")
			return
		}

		for i := range rf.peers {
			// rf.lastElectionHeartbeat = time.Now()
			if i == rf.me {
				rf.lastElectionHeartbeat = time.Now()
				continue
			}

			go func(i int) {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.log) - 1,
					PrevLogTerm:  rf.log[len(rf.log)-1].Term,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}

				// byte_count := unsafe.Sizeof(args)
				// print("Byte count: ", byte_count, "\n")

				if len(rf.log) > 0 {
					args.PrevLogIndex = len(rf.log) - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}

				reply := AppendEntriesReply{} // initialize to true. will only be false if another server doesn't recognize this server as the leader

				// print("Server ", rf.me, " Role: ", rf.role, " sending heartbeat to ", i, "\n")
				rf.sendAppendEntries(i, &args, &reply)

				// if ok && !reply.Success {
				// 	print("Server ", rf.me, " Role: ", rf.role, " received false from ", i, " and is no longer the leader \n")
				// 	rf.changeRole(FOLLOWER)
				// 	// print("Server ", rf.me, " received false from ", i, " and is no longer the leader\n")
				// 	return
				// }
			}(i)
		}

		time.Sleep(150 * time.Millisecond)
	}
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

	// persistent state on all servers
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.role = FOLLOWER
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan struct{}, 100)

	// volatile state on all servers
	rf.electionTimeoutVal = rand.Intn(350) + 200
	rf.appendEntryHeartbeats = make([]time.Time, len(rf.peers))

	// rf.electionTimeoutVal = rand.Intn(500) + 300
	rf.lastElectionHeartbeat = time.Now()

	// volatile state on leaders
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// print("Started server ", me, " with election timeout ", rf.electionTimeoutVal, "\n")

	go rf.electionTimeoutChecker()

	go func() {
		for {
			select {
			case <-rf.notifyApplyCh:
				rf.startApplyLogs()
			}
		}
	}()

	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		go func(peerIndex int) {
			for {
				if time.Since(rf.appendEntryHeartbeats[peerIndex]).Milliseconds() > HEARTBEAT_INTERVAL {
					rf.appendPeerEntries(peerIndex)
				}
			}
		}(i)
	}

	return rf
}
