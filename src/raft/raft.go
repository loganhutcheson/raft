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
var electionTimeout int64 = 500
var HeartbeatTimeout int64 = 50

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

// Define a struct to hold the integer and interface{}
type IntAndInterface struct {
	IntValue int
	AnyValue interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	applyCh chan ApplyMsg

	// Persistent state
	currentTerm int
	votedFor    int64
	log         map[int]IntAndInterface

	// Volatile state
	currentState      RaftState
	heartBeatReceived bool
	leaderId          int64
	commitIndex       int
	lastApplied       int
	nextIndex         map[int]int
	matchindex        map[int]int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
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
	CandidateId  int64
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int64

	// 3B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      map[int]IntAndInterface
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Fast Backup
	XTerm  int // Term of the conflicting entry
	XIndex int // Index of the first entry w/ XTerm
	XLen   int // Length of the log

}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(dTimer, "S%d Follower, received requestVote for term %d", rf.me, args.Term)
	// TODO clean this up

	candidateTerm := args.Term
	followerTerm := rf.currentTerm

	// Case 1: candidate has a lower term
	if candidateTerm < followerTerm {
		// Reject candidate
		Debug(dVote, "S%d Follower, reject vote for %d due to smaller term %d.",
			rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
		return
	}

	// Case 2: candidate has a greater or equal term
	// Grant vote iff votedFor is null or candidateID
	// and log is at least as up-to-date as candidate

	// 5.4.1 Election restriction:
	// vote yes only if higher term in last entry, OR
	// same last term && logs are each greater than or equal to eachother

	if (candidateTerm > followerTerm ||
		rf.votedFor == 0 ||
		rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log[rf.commitIndex].IntValue ||
			args.LastLogTerm == rf.log[rf.commitIndex].IntValue &&
				args.LastLogIndex >= rf.commitIndex) {
		Debug(dVote, "S%d Follower, granting vote to %d same/greater term %d.", rf.me,
			args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentState = Follower
		// Update the term to sync
		rf.currentTerm = args.Term
	} else {
		if candidateTerm > followerTerm {
			rf.currentState = Follower
		}
		Debug(dVote, "S%d Follower, failed vote to %d "+
			"same/greater term %d: "+
			"args.LastLogIndex=%d args.LastLogTerm=%d "+
			" rf.commitIndex=%d rf.log[rf.commitIndex].IntValue=%d ",
			rf.me, args.CandidateId, args.Term,
			args.LastLogIndex, args.LastLogTerm,
			rf.commitIndex, rf.log[rf.commitIndex].IntValue)
		reply.VoteGranted = false
		// Update the term to sync
		rf.currentTerm = args.Term
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

// A launched thread the endlessly tries to commit
// a log to a specified raft index
// also handles the back up procedure upon rejection
func (rf *Raft) AppendAndSync(server int, logIndex int, heartbeat bool) bool {

	newEntries := make(map[int]IntAndInterface)
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	reply.Success = false
	reply.Term = 0
	for !reply.Success {

		// Check if we are still leader
		_, isLeader := rf.GetState()
		if !isLeader {
			return false
		}

		// If the failure is because of follower on
		// higher term, then we don't try to fix consistency
		// revert back to follower
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentState = Follower
			rf.mu.Unlock()
			return false
		}

		// Maintain a nextIndex field (one for each follower)
		// in response to errors, we decrement the nextIndex field
		// and resend the append entry RPC to the failed server

		// Fast Backup
		rf.mu.Lock()
		if reply.XLen > 0 {
			if reply.XTerm == -1 {
				// Follower does not have an entry for this index
				// backup to the last index of the follower's log
				Debug(dInfo, "S%d Leader - setting nextIndex for S%d to %d (case 1)",
					rf.me, server, reply.XLen)
				rf.nextIndex[server] = reply.XLen
			} else {
				// Check if leader has the follower's mismatched term
				found := -1
				for i := 1; i < len(rf.log); i++ {
					if rf.log[i].IntValue == reply.XTerm {
						found = i
						break
					}
				}
				if found >= 0 {
					// Mismatched entries' term present in leader's log,
					// backup to leader's first index of the found term
					Debug(dInfo, "S%d Leader - setting nextIndex for S%d to %d (case 2)",
						rf.me, server, found)
					rf.nextIndex[server] = found
				} else {
					// Not present in leader's log, backup to follower's
					// first index of the missing term
					Debug(dInfo, "S%d Leader - setting nextIndex for S%d to %d (case 3)",
						rf.me, server, reply.XIndex)
					rf.nextIndex[server] = reply.XIndex
				}
			}
		}

		// Add all the missing entries between nextIndex and logIndex
		if rf.nextIndex[server] <= logIndex {
			Debug(dInfo, "S%d sending entries between %d <-> %d to S%d",
				rf.me, rf.nextIndex[server], logIndex, server)
			for i := rf.nextIndex[server]; i <= logIndex; i++ {
				newEntries[i] = rf.log[i]
			}
		}

		prevLogIndex := rf.nextIndex[server] - 1
		args = AppendEntriesArgs{rf.currentTerm, rf.leaderId, prevLogIndex,
			rf.log[prevLogIndex].IntValue, newEntries, rf.commitIndex}
		rf.mu.Unlock()

		// Block on send call as we are already threaded for each raft
		rf.sendAppendEntries(server, &args, &reply)

	}
	// Successful commit by this raft
	// Set nextIndex, so we know which is the next log to commit.
	// Increment the matchIndex so we know we commited a new log.
	rf.mu.Lock()
	rf.nextIndex[server] = logIndex + 1
	rf.matchindex[server] = logIndex
	Debug(dInfo, "S%d Leader - setting nextIndex for S%d to %d (success case)",
		rf.me, server, logIndex+1)
	rf.mu.Unlock()

	return true
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
	term, isLeader = rf.GetState()

	// If this is not a leader return false
	if !isLeader {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Append the log to it's own (do not notify client success yet)
	rf.log[len(rf.log)+1] = IntAndInterface{rf.currentTerm, command}
	logIndex := len(rf.log)
	Debug(dLeader, "S%d Leader, Start() has been called for cmd: %d!",
		rf.me, command)

	// 2. Issue RPCs to each rafts in parallel
	commitResult := make(chan bool)

	for i := range rf.peers {
		// Skip self
		if i == rf.me {
			continue
		}
		// Launch a separate thread for each peer
		go func(peerIndex int) {
			result := rf.AppendAndSync(peerIndex, logIndex, false)
			commitResult <- result
		}(i)
	}

	// Start() returns after some time even if there
	// is not a valid majority of servers present.
	// It appears that the tester expects there to be a Start return
	// and then a nCommitted after some tester sleep period to
	// check that there is not a committment without a majority.
	go func() {
		append_count := 1 // start at 1 for leader
		return_count := 1
		for result := range commitResult {
			return_count++
			if result {
				Debug(dLeader, "S%d Leader, got an approval for log %d!",
					rf.me, command)
				append_count++
				if append_count > (len(rf.peers) / 2) {
					rf.mu.Lock()
					// Increment the commit index
					// what if leader needs to commit multiple after approval?
					// I.E. logIndex > commitIndex
					for logIndex > rf.commitIndex {
						rf.commitIndex++
					}
					// Apply log to state machine in-order
					for rf.lastApplied < rf.commitIndex {
						rf.lastApplied++
						msg := ApplyMsg{true, rf.log[rf.lastApplied].AnyValue,
							rf.lastApplied, false, nil, 0, 0}
						rf.applyCh <- msg
						Debug(dLeader, "S%d Leader, Committing Entry!"+
							"Index:%d Term=%d Cmd=%d",
							rf.me, rf.lastApplied, rf.log[rf.lastApplied].IntValue,
							rf.log[rf.lastApplied].AnyValue)
					}
					rf.mu.Unlock()
					break
				}
			} else {
				Debug(dLeader, "S%d Leader, got a rejection for log %d!",
					rf.me, command)
			}

			if return_count > len(rf.peers) {
				// Leader received ALL replies, but could not commit
				return
			}

		}
	}()
	// Set the index to notify the tester that we think this log is
	// *maybe* committed on the majority of servers.
	return len(rf.log), term, isLeader
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.XLen = len(rf.log)

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		// If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		reply.Success = false
		rf.currentTerm = args.Term
		rf.currentState = Follower
		return
	}

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex > 0 && args.PrevLogTerm != rf.log[args.PrevLogIndex].IntValue {
		// Fast Backup
		// term of the conflicting entry
		if args.PrevLogIndex > 0 {
			reply.XTerm = rf.log[args.PrevLogIndex].IntValue
		} else {
			reply.XTerm = -1
		}

		// Find the first index of XTerm
		index := args.PrevLogIndex
		for rf.log[index].IntValue == rf.log[args.PrevLogIndex].IntValue {
			if index > 0 {
				index--
			} else {
				break
			}
		}
		reply.XIndex = index + 1
		reply.XLen = len(rf.log)
		reply.Success = false
	}

	if reply.Success {
		rf.heartBeatReceived = true

		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it - in order
		for index := args.PrevLogIndex + 1; index <= args.PrevLogIndex+len(args.Entries); index++ {
			if _, exists := rf.log[index]; exists {
				if rf.log[index].IntValue != args.Entries[index].IntValue {
					Debug(dDrop, "S%d Follower, dropping mismatch on Index=%d"+
						" Follower's Term=%d Cmd=%d Leader's Term=%d Cmd=%d",
						rf.me, index, rf.log[index].IntValue, rf.log[index].AnyValue,
						args.Entries[index].IntValue, args.Entries[index].AnyValue)
					delete(rf.log, index)
				}
			}
		}

		// Append any new entries not already in the log - in order

		// TBD - in what case would args.PrevLogIndex + len(args.Entries)
		// calculate to an index that is not included in args.Entries (i.e. null)

		for index := args.PrevLogIndex + 1; index <= args.PrevLogIndex+len(args.Entries); index++ {

			// If the cmd for the calcualted index is nil, then the true index must already be committed
			if args.Entries[index].AnyValue == nil {
				return
			}

			if _, exists := rf.log[index]; !exists {
				rf.log[index] = args.Entries[index]
				Debug(dLog, "S%d Follower, Logging new entry! "+
					" Size=%d Index=%d Term=%d Cmd=%d",
					rf.me, len(rf.log), index,
					args.Entries[index].IntValue, args.Entries[index].AnyValue)
			}

			// TODO remove -- DEBUG
			if args.Entries[index].AnyValue == nil {
				Debug(dError, "S%d Follower, logging a new nil entry w/ PrevLogIndex=%d!!"+
					" Size of entries is %d ",
					rf.me, args.PrevLogIndex, len(args.Entries))
			}
		}

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		}

		// If commitIndex > lastApplied: increment lastApplied, apply
		// log[lastApplied] to state machine
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{true, rf.log[rf.lastApplied].AnyValue,
				rf.lastApplied, false, nil, 0, 0}
			rf.applyCh <- msg
			Debug(dCommit, "S%d Follower, Committing new entry on term%d, lastApplied:%d!"+
				" Index: %d Term=%d Cmd=%d",
				rf.me, args.Term, rf.lastApplied,
				rf.lastApplied, rf.log[rf.lastApplied].IntValue,
				rf.log[rf.lastApplied].AnyValue)
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Starts the leader logic for elected raft
// Sends Heartbeat every to all servers
func (rf *Raft) leaderHeartbeat() {
	Debug(dLeader, "S%d Leader, starting heartbeat", rf.me)
	rf.leaderId = rand.Int63()

	for !rf.killed() {
		_, isLeader := rf.GetState()
		if !isLeader {
			Debug(dLeader, "S%d Leader, stopping heartbeat", rf.me)
			return
		}

		// Send the Heartbeat appendEntriesRPC
		//	args := AppendEntriesArgs{rf.currentTerm, rf.leaderId, rf.commitIndex,
		//		rf.log[rf.commitIndex].IntValue, nil, rf.commitIndex}
		for i := range rf.peers {
			// Skip self
			if i == rf.me {
				continue
			}
			// Launch the AppendEntries RPCs in parallel
			go func(peerIndex int) {
				// use length of the log in case this leader has uncommitted entries
				rf.AppendAndSync(peerIndex, len(rf.log), true)

			}(i)

		}

		// Heartbeat timeout - sleep for specified time.
		time.Sleep(time.Duration(HeartbeatTimeout) * time.Millisecond)
	}
}

// election timeout for all rafts
func (rf *Raft) electionTicker() {

	for !rf.killed() {

		var startElection = false
		var majorityVote = false

		// Election Timeout - pause for a random amount of time.
		// This one is collecting votes
		var wg sync.WaitGroup
		wg.Add(1) // Add count for one election timer
		var election_timeout = make(chan bool, 1)
		go func() {
			defer wg.Done() // Notify WaitGroup that goroutine is finished
			ms := electionTimeout + (rand.Int63() % 250)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			election_timeout <- true
		}()

		// Continue for leader,  leader breaks only when receiving a
		// RPC with higher term
		_, isLeader := rf.GetState()
		if isLeader {
			wg.Wait() // Wait for election timer
			continue
		}

		rf.mu.Lock()
		// Election timeout went off, let's reset state to allow
		// this raft to vote in other elections.
		if rf.heartBeatReceived {
			// If a HeartBeat has been received, reset check.
			rf.heartBeatReceived = false
		} else {
			// Convert to candidate
			startElection = true
		}

		// Candidate
		// If a HeartBeat has NOT been received, hold an election.
		if startElection {
			rf.currentState = Candidate // Set to Candidate state
			candidateId := rand.Int63() // Grab new ID
			rf.votedFor = candidateId   // Vote for self
			votes := 1                  // Vote for self
			rf.currentTerm++            // Increment the term
			Debug(dVote, "S%d Follower, starting election id=%d, term=%d",
				rf.me, candidateId, rf.currentTerm)

			// Set term information for filtering election candidates
			args := RequestVoteArgs{rf.currentTerm, candidateId,
				rf.commitIndex, rf.log[rf.commitIndex].IntValue}

			// Send requestVote RPC to all known rafts in parallel
			vote_results := make(chan bool, len(rf.peers)-1)
			for i := range rf.peers {
				// Skip self
				if i == rf.me {
					continue
				}
				// Launch the requestVote RPCs in parallel
				go func(peerIndex int) {
					reply := RequestVoteReply{}
					rf.sendRequestVote(peerIndex, &args, &reply)
					vote_results <- reply.VoteGranted
				}(i)
			}
			rf.mu.Unlock()

			// Wait for votes
		loop:
			for i := 0; i < len(rf.peers)-1; i++ {
				// The only way around the fact that this RPC can take
				// longer to return than the timeout is if this RPC wait is moved
				// to another thead, that is selected by either this RPC returning
				// OR the timeout breaking
				select {

				case vote := <-vote_results:
					if vote {
						Debug(dInfo, "S%d Candidate, got a vote.", rf.me)
						votes++
					}

				case <-election_timeout:
					Debug(dInfo, "S%d Candidate, timeout.", rf.me)
					break loop

				}

				// Go ahead and break out if we have a quorom
				if votes > (len(rf.peers) / 2) {
					break
				}
				// While waiting, if we became a follower break out.
				if rf.currentState != Candidate {
					break
				}
			}

			rf.mu.Lock()
			Debug(dInfo, "S%d Candidate, received %d votes.", rf.me, votes)

			if votes > (len(rf.peers)/2) && rf.currentState == Candidate {
				majorityVote = true
			}

			// We may technically have received enough votes, but
			// if during the election we got converted to follower
			// by some other higher term RPC - then really we did not win
			// majorityVote = (majorityVote && rf.votedFor == rf.Id)

			// Election results
			if majorityVote {
				// Won election, become leader
				Debug(dVote, "S%d Candidate, won a majority vote!", rf.me)
				rf.currentState = Leader

				// When a leader comes to power, initialize all the nextIndex values
				// to just after the last one of it's log
				for i := range rf.peers {
					Debug(dVote, "S%d initialized nextIndex to %d", rf.me, len(rf.log)+1)
					rf.nextIndex[i] = len(rf.log) + 1
				}

				// Start the leader routine
				go rf.leaderHeartbeat()
			} else {
				// Lost the election
				Debug(dVote, "S%d Candidate, lost the election!", rf.me)
			}
		}
		rf.mu.Unlock()

		wg.Wait() // Wait for election timer

	} // end for
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

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh

	// Persistent state
	rf.currentTerm = 0
	rf.votedFor = 0
	rf.log = make(map[int]IntAndInterface)

	// Volatile state
	rf.currentState = Follower
	rf.heartBeatReceived = false
	rf.leaderId = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make(map[int]int)
	// nextIndex initialized to leader last log + 1
	for i := range rf.peers {
		rf.nextIndex[i] = 1
	}

	rf.matchindex = make(map[int]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start electionTicker goroutine to start elections
	go rf.electionTicker()

	return rf
}
