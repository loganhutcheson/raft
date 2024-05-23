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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// Globals timeouts (ms)
var electionTimeout int64 = 300
var heartbeatTimeout int64 = 50

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

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	term_updated chan bool

	// Your data here (3A, 3B, 3C).
	currentState      RaftState
	heartBeatReceived bool
	currentTerm       int
	Id                int64
	votedFor          int64
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
	// TODO mutex
	isleader = (rf.currentState == Leader)

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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	Id           int64
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int64
	// TODO add other term related entries
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// All servers
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = 0
		select {
		case rf.term_updated <- true:
		default:
			break
		}
	}

	//  Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == 0 || rf.votedFor == args.Id {
		reply.VoteGranted = true
		rf.votedFor = args.Id
	} else {
		reply.VoteGranted = false
	}

}

// send a RequestVote RPC to a server.
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// All servers
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	//While waiting for votes, a candidate may receive an
	// AppendEntries RPC from another server claiming to be
	// leader. If the leader’s term (included in its RPC) is at least
	// as large as the candidate’s current term, then the candidate
	// recognizes the leader as legitimate and returns to follower
	// state
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = 0
		reply.Success = true
		select {
		case rf.term_updated <- true:
		default:
			break
		}
	}

	//  Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	} else {
		Debug(dTimer, "S%d Follower, Heartbeat received on term %d", rf.me, args.Term)
		rf.heartBeatReceived = true
		rf.currentState = Follower
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Starts the leader logic for elected raft
// Sends heartbeat every 50ms to all servers
func (rf *Raft) leaderHeatbeat() {
	Debug(dTimer, "S%d Leader, Heartbeat started", rf.me)
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if !isLeader {
			Debug(dTimer, "S%d Leader, stopping leader", rf.me)
			// No longer leader, stop sending heartbeats
			return
		}

		ret := make(chan bool, len(rf.peers)-1)
		args := AppendEntriesArgs{rf.currentTerm, rf.Id}
		reply := AppendEntriesReply{0, false}
		for i := range rf.peers {
			// Skip self
			if i == rf.me {
				continue
			}
			// Launch the AppendEntries RPCs in parallel
			go func(peerIndex int) {
				rf.sendAppendEntries(peerIndex, &args, &reply)
				ret <- reply.Success
			}(i) // Pass 'i' as an argument to the function
		}

		// Wait on all replies
		for i := 0; i < len(rf.peers)-1; i++ {
			<-ret
		}

		// Heartbeat timeout - sleep for specified time.
		time.Sleep(time.Duration(heartbeatTimeout) * time.Millisecond)
	}
}

// Ticks on the election timeout for all rafts
// 1  waits the timeout
// 2 starts election if heartbeat not received
func (rf *Raft) electionTicker() {

	for !rf.killed() {

		//ElectionLoop:
		var startElection = false
		var majorityVote = false

		// Election Timeout - pause for a random amount of time.
		// This one is collecting votes
		ms := electionTimeout + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// Continue for leader, leader breaks only when receiving a
		// RPC with higher term
		_, isLeader := rf.GetState()
		if isLeader {
			continue
		}

		// Election timeout went off, let's reset state to allow
		// this raft to vote in other elections.
		if rf.heartBeatReceived {
			// If a heartBeat has been received, reset check.
			rf.heartBeatReceived = false
		} else {
			// Convert to candidate
			startElection = true
		}

		// Candidates
		// If a heartBeat has NOT been received, hold an election.
		if startElection {
			rf.currentState = Candidate // Set to Candidate state
			rf.Id = rand.Int63()        // Grab new ID
			rf.votedFor = rf.Id         // Vote for self
			votes := 1                  // Vote for self
			rf.currentTerm++            // Increment the term
			Debug(dVote, "S%d Follower, starting election id=%d, term=%d",
				rf.me, rf.Id, rf.currentTerm)

			// TODO set term information for filtering election candidates
			args := RequestVoteArgs{rf.currentTerm, rf.Id, 0, 0}
			reply := RequestVoteReply{0, false}

			// Send requestVote RPC to all known rafts in parallel
			vote_results := make(chan bool, len(rf.peers)-1)
			for i := range rf.peers {
				// Skip self
				if i == rf.me {
					continue
				}
				// Launch the requestVote RPCs in parallel
				go func(peerIndex int) {
					rf.sendRequestVote(peerIndex, &args, &reply)
					vote_results <- reply.VoteGranted
				}(i)
			}

			// Count the votes
			for i := 0; i < len(rf.peers)-1; i++ {
				if <-vote_results {
					votes++
				}

				// Go ahead and break out if have a quorom
				if votes > (len(rf.peers) / 2) {
					break
				}

				// case <-rf.term_updated:
				//	goto ElectionLoop
			}

			//close(vote_results)
			Debug(dInfo, "S%d Candidate, votes is %d", rf.me, votes)

			if votes > (len(rf.peers) / 2) {
				majorityVote = true
			}

			// We may technically have received enough votes, but
			// if during the election we got converted to follower
			// by some other higher term RPC - then really we did not win
			// majorityVote = (majorityVote && rf.votedFor == rf.Id)

			// Election results
			if majorityVote {
				// Won election, become leader
				Debug(dVote, "S%d Candidate, got a majority vote!", rf.me)
				rf.currentState = Leader
				// Start the leader routine
				go rf.leaderHeatbeat()
			} else {
				// Lost the election
				Debug(dVote, "S%d Candidate, lost the vote!", rf.me)
			}

		}

	}

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
	rf.currentTerm = 0
	rf.Id = 0
	rf.currentState = Follower
	rf.votedFor = 0
	rf.term_updated = make(chan bool)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start electionTicker goroutine to start elections
	go rf.electionTicker()

	return rf
}
