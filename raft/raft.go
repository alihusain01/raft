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
)

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
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
	currentTerm int
	votedFor    int
	log         []LogEntry
	role        int32 // 0 - follower, 1 - candidate, 2 - leader

	// volatile state on all servers
	commitIndex int
	lastApplied int

	electionTimeoutVal int
	lastHeartbeat      time.Time

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
	isleader = rf.role == 2
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
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartbeat = time.Now()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = 0
	}

	if rf.role == 1 {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.votedFor = -1
		return
	}

	if args.LastLogIndex >= rf.lastApplied && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
	}

	return

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".

	rf.mu.Lock()
	defer rf.mu.Unlock()

	print("Recipient: ", rf.me, " Term: ", rf.currentTerm, " received heartbeat from ", args.LeaderId, " Term: ", args.Term, "\n")

	rf.lastHeartbeat = time.Now()

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		atomic.StoreInt32(&rf.role, 0)
		reply.Success = true
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm // this might be different
		reply.Success = false
	}

	return
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
	index := rf.lastApplied
	term := rf.currentTerm
	isLeader := rf.role == LEADER

	// Your code here (2B).
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
	}
	rf.mu.Unlock()


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
	// atomic.StoreInt32(&rf.isLeader, 0)
	print("Server ", rf.me, " has been killed. Role: ", rf.role, " \n")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) changeRole(role int32) {
	rf.role = atomic.LoadInt32(&role)

	switch role {
	case FOLLOWER:
		print("Server ", rf.me, " is now a follower\n")
	case CANDIDATE:
		print("Server ", rf.me, " is now a candidate\n")
	case LEADER:
		print("Server ", rf.me, " is now a leader\n")
	default:
		break
	}

}

func (rf *Raft) electionTimeoutChecker() {
	for {
		// If the server is dead, exit the loop
		if rf.killed() {
			break
		}

		timeSince := time.Since(rf.lastHeartbeat).Milliseconds()

		if timeSince > int64(rf.electionTimeoutVal) && atomic.LoadInt32(&rf.role) != 2 || atomic.LoadInt32(&rf.role) == 1 {
			// Start an election
			print("Server ", rf.me, " timed out. Time since: ", timeSince, ". Election timeout: ", rf.electionTimeoutVal, "\n")
			rf.startElection()
			// break
		}

	}
}

func (rf *Raft) startElection() {

	rf.mu.Lock()
	rf.lastHeartbeat = time.Now()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	// rf.electionTimeoutVal = rand.Intn(150) + 150
	rf.role = 1

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
		rf.lastHeartbeat = time.Now()

		if i == rf.me {
			continue
		} else {
			go func(votesChannel chan bool, index int) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(index, &args, &reply)
				votesChannel <- reply.VoteGranted
				if reply.Term > args.Term {
					rf.mu.Lock()
					if rf.currentTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.role = 0
						rf.lastHeartbeat = time.Now()
					}
					rf.mu.Unlock()
				}
			}(votesChannel, i)
		}
	}

	for {
		vote := <-votesChannel
		voteCount++
		if vote == true {
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
	if rf.currentTerm == args.Term && atomic.LoadInt32(&rf.role) == 1 {
		rf.changeRole(LEADER)
	}

	if atomic.LoadInt32(&rf.role) == 2 {
		go rf.startHeartbeat()
	}
	rf.mu.Unlock()

	return

	// if voteCount*2 > len(rf.peers) {
	// 	atomic.StoreInt32(&rf.isLeader, 1)
	// 	print("Server ", rf.me, " is now the leader \n")
	// 	go rf.startHeartbeat()
	// } else {
	// 	atomic.StoreInt32(&rf.isLeader, 0)
	// 	print("Server ", rf.me, " didn't get enough votes \n")
	// }

}

func (rf *Raft) startHeartbeat() {
	for {
		if atomic.LoadInt32(&rf.role) == FOLLOWER || rf.killed() {
			return
		}

		for i := range rf.peers {
			// rf.lastHeartbeat = time.Now()

			if i == rf.me {
				continue
			}

			go func(i int) {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: 0,
					PrevLogTerm:  0,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}

				if len(rf.log) > 0 {
					args.PrevLogIndex = len(rf.log) - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}

				reply := AppendEntriesReply{} // initialize to true. will only be false if another server doesn't recognize this server as the leader

				t1 := time.Now()
				print("Server ", rf.me, " Role: ", rf.role, " sending heartbeat to ", i, "\n")
				ok := rf.sendAppendEntries(i, &args, &reply)

				if ok && reply.Success == false {
					rf.changeRole(FOLLOWER)
					print("Server ", rf.me, " received false from ", i, " and is no longer the leader\n")
					return
				} else if ok && reply.Success == true {
					print("Server ", rf.me, " received true from ", i, " role: ", atomic.LoadInt32(&rf.role), "\n")
				} else {
					print("Server ", rf.me, " didn't receive a response from ", i, " role: ", atomic.LoadInt32(&rf.role), "\n")
					print(". Waited for ", time.Since(t1).Milliseconds(), " milliseconds\n")
				}
			}(i)

			// if i != rf.me {
			// 	args := AppendEntriesArgs{
			// 		Term:         rf.currentTerm,
			// 		LeaderId:     rf.me,
			// 		PrevLogIndex: 0,
			// 		PrevLogTerm:  0,
			// 		Entries:      []LogEntry{},
			// 		LeaderCommit: rf.commitIndex,
			// 	}

			// 	// if len(rf.log) > 0 {
			// 	// 	args.PrevLogIndex = len(rf.log) - 1
			// 	// 	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			// 	// }

			// 	reply := AppendEntriesReply{} // initialize to true. will only be false if another server doesn't recognize this server as the leader

			// 	t1 := time.Now()
			// 	print("Server ", rf.me, " Role: ", rf.role, " sending heartbeat to ", i, "\n")
			// 	ok := rf.sendAppendEntries(i, &args, &reply)

			// 	if ok && reply.Success == false {
			// 		atomic.StoreInt32(&rf.role, 0)
			// 		print("Server ", rf.me, " received false from ", i, " and is no longer the leader\n")
			// 		return
			// 	} else if ok && reply.Success == true {
			// 		print("Server ", rf.me, " received true from ", i, " role: ", atomic.LoadInt32(&rf.role), "\n")
			// 	} else {
			// 		print("Server ", rf.me, " didn't receive a response from ", i, " role: ", atomic.LoadInt32(&rf.role), "\n")
			// 		print(". Waited for ", time.Since(t1).Milliseconds(), " milliseconds\n")
			// 	}
			// }
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
	rf.log = make([]LogEntry, 0)
	rf.role = 0

	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.electionTimeoutVal = rand.Intn(100) + 200

	// if rf.me == 0 {
	// 	rf.electionTimeoutVal = 200
	// } else if rf.me == 1 {
	// 	rf.electionTimeoutVal = 250
	// } else if rf.me == 2 {
	// 	rf.electionTimeoutVal = 300
	// }

	// rf.electionTimeoutVal = rand.Intn(500) + 300
	rf.lastHeartbeat = time.Now()

	// volatile state on leaders
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	print("Started server ", me, " with election timeout ", rf.electionTimeoutVal, "\n")

	go rf.electionTimeoutChecker()

	return rf
}
