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

	// TODO 5.4.1 Election restriction:
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
func (rf *Raft) AppendAndSync(server int, cmd interface{}) bool {

	term, _ := rf.GetState()

	// Note - leader has NOT committed this yet until majority
	newCommitIndex := rf.commitIndex + 1
	prevLogIndex := rf.commitIndex
	newEntries := make(map[int]IntAndInterface)
	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	// If we are supplied a cmd, we should first try to commit the
	// new entry.
	if cmd != nil {
		newEntries[newCommitIndex] = IntAndInterface{term, cmd}
		args = AppendEntriesArgs{rf.currentTerm, rf.leaderId, rf.commitIndex,
			rf.log[prevLogIndex].IntValue, newEntries, newCommitIndex}
		Debug(dCommit, "S%d sends appendRPC to raft %d with entries len %d",
			rf.me, server, len(newEntries))
		// Block on send call as we are already threaded for each raft
		rf.sendAppendEntries(server, &args, &reply)
	} else {
		// Otherwise, we assume this is a consistency check and flag failure.
		reply.Success = false
	}

	for !reply.Success {

		// Check if we are still leader
		_, isLeader := rf.GetState()
		if !isLeader {
			return false
		}

		// Maintain a nextIndex field (one for each follower)
		// it's initialized to the currentIndex
		// in response to errors, we decrement the nextIndex field
		// and resend the append entry RPC to the failed server
		// once we receive the success, then we back up to the full log

		// TODO implement fast backup
		// Consistency check failed for the latest log.

		// Back up the nextIndex by 1 and ship it again.
		if newCommitIndex > 0 {
			rf.mu.Lock()
			rf.nextIndex[server]--
			rf.mu.Unlock()
			newCommitIndex--
			newEntries[newCommitIndex] = rf.log[newCommitIndex]
			prevLogIndex--
			// TODO - we should be appending the entries to the map
			// to do a bulk-add once we find a match.
			args = AppendEntriesArgs{rf.currentTerm, rf.leaderId, prevLogIndex,
				rf.log[prevLogIndex].IntValue, newEntries, newCommitIndex}
			Debug(dCommit, "S%d sends appendRPC to raft %d with entries len %d",
				rf.me, server, len(newEntries))
			// Block on send call as we are already threaded for each raft
			rf.sendAppendEntries(server, &args, &reply)

			if reply.Success {
				// we also need to update the nextIndex
				rf.nextIndex[server] = rf.commitIndex
			}
		} else {
			Debug(dCommit, "S%d ERROR sending to %d, 0th element rejected",
				rf.me, server)
			return false
		}

		// TBD - sleep here to avoid a
		// mess of RPCs flooding the network whenever
		// some failure occurs
		time.Sleep(100 * time.Millisecond)

	}
	// Successful commit by this raft
	// Increment the matchIndex so we know we commited a new log.
	rf.mu.Lock()
	rf.matchindex[server]++
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

	// Your code here (3B).
	term, isLeader = rf.GetState()

	// If this is not a leader return false
	if !isLeader {
		return index, term, isLeader
	}

	Debug(dLeader, "S%d Leader, Start() has been called for cmd: %d!",
		rf.me, command)
	// 1. Append the log to it's own (do not notify client success yet)
	rf.mu.Lock()
	newCommitIndex := rf.commitIndex + 1
	rf.log[newCommitIndex] = IntAndInterface{rf.currentTerm, command}
	rf.mu.Unlock()

	// 2. Issue RPCs to each rafts in parallel
	commitResult := make(chan bool)

	for i := range rf.peers {
		// Skip self
		if i == rf.me {
			continue
		}
		// Launch a separate thread for each peer
		go func(peerIndex int) {
			result := rf.AppendAndSync(peerIndex, command)
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
					// Leader commit after majority occurs
					msg := ApplyMsg{true, rf.log[newCommitIndex].AnyValue, newCommitIndex,
						false, nil, 0, 0}
					rf.applyCh <- msg
					// Increment the commit index
					rf.mu.Lock()
					rf.commitIndex++
					rf.mu.Unlock()
					Debug(dLeader, "S%d Leader, Committed Entry!"+
						"Index: %d Term=%d Cmd=%d",
						rf.me, newCommitIndex, rf.log[newCommitIndex].IntValue,
						rf.log[newCommitIndex].AnyValue)
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
	return newCommitIndex, term, isLeader
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
	leaderTerm := args.Term
	followerTerm := rf.currentTerm

	// Case 1: Leader has a higher or equal term
	// We accept this leader iff
	// log is as long as prevCommitIndex and last index matches expected term
	if leaderTerm >= followerTerm {

		if leaderTerm > followerTerm {
			rf.currentTerm = args.Term // Update term to latest
			rf.currentState = Follower // Immediately revert to follower state
		}

		if args.Entries != nil {
			// Log is at least as up to date, and the last entry is the same
			// previous commit in the raft's log matches the append RPC previous commit
			// by checking the the previous index exists in this raft's log
			// and stores the previous term
			if args.PrevLogIndex != len(rf.log) {
				// Reject leader due to missing entries
				Debug(dLog, "S%d Follower, AppendRPC received on term %d "+
					">= term case with entries. Error: missing entries. "+
					"Follower: log length=%d "+
					"Leader: prevIndex=%d prevTerm=%d ",
					rf.me, args.Term, len(rf.log),
					args.PrevLogIndex, args.PrevLogTerm)
				reply.Success = false
			} else if len(rf.log) > 0 &&
				rf.log[args.PrevLogIndex].IntValue != args.PrevLogTerm {
				// Reject leader due to mismatched entries
				Debug(dLog, "S%d Follower, AppendRPC received on term %d "+
					">= term case with entries. Error: mismatched entries. "+
					"Follower: Log len=%d, log[prevIndex].Term=%d\n"+
					"Leader: prevIndex=%d, prevTerm=%d",
					rf.me, args.Term, len(rf.log), rf.log[args.PrevLogIndex].IntValue,
					args.PrevLogIndex, args.PrevLogTerm)
				reply.Success = false
			} else {
				// Accept leader and append log
				rf.votedFor = 0
				reply.Success = true
				rf.heartBeatReceived = true

				// bulk add during consistency fix
				for i := 0; i < len(args.Entries); i++ {
					// Add each of the entries
					Debug(dLog, "S%d Follower, AppendRPC received on term %d "+
						">= term case with entries. Saving log! [total size=%d]"+
						"Index: %d Term=%d Cmd=%d",
						rf.me, args.Term, len(rf.log),
						args.LeaderCommit, args.Entries[args.LeaderCommit].IntValue,
						args.Entries[args.LeaderCommit].AnyValue)
					rf.log[args.LeaderCommit] = args.Entries[args.LeaderCommit]
					args.LeaderCommit++
				}

				// rf.log[args.LeaderCommit] = args.Entries[args.LeaderCommit]

				// Commit entry to statemachine
				// the leader and by proxy a majority of peers
				// have also committed the log.
				if rf.commitIndex < args.PrevLogIndex &&
					len(rf.log) == args.PrevLogIndex {
					Debug(dCommit, "S%d LOGAN WARNING1: %d %d",
						rf.commitIndex, args.PrevLogIndex)
					// Handle more than 1 lagging log
					for rf.commitIndex < args.PrevLogIndex {
						rf.commitIndex++
						msg := ApplyMsg{true, rf.log[rf.commitIndex].AnyValue,
							rf.commitIndex, false, nil, 0, 0}
						rf.applyCh <- msg
						Debug(dCommit, "S%d Follower, AppendRPC received on term %d "+
							">= case with notify update. Committed new entry!"+
							"Index: %d Term=%d Cmd=%d",
							rf.me, args.Term,
							rf.commitIndex, rf.log[rf.commitIndex].IntValue,
							rf.log[rf.commitIndex].AnyValue)
					}
				}
			}
		} else {
			// Accept leader (nil entries)
			Debug(dLog, "S%d Follower, AppendRPC received on term %d "+
				">= term case with nill entries. Accept heartbeat.",
				rf.me, args.Term)
			rf.votedFor = 0
			reply.Success = true
			rf.heartBeatReceived = true

			// Commit outstanding entries
			// if the leader and by proxy a majority of the peers
			// have accepted the entry
			// TBD - handle more than 1 lagging log
			if args.PrevLogIndex > rf.commitIndex &&
				len(rf.log) == args.PrevLogIndex {
				Debug(dCommit, "S%d LOGAN WARNING2: %d %d",
					rf.me, rf.commitIndex, args.PrevLogIndex)
				// Handle more than 1 lagging log
				for rf.commitIndex < args.PrevLogIndex {
					rf.commitIndex++
					msg := ApplyMsg{true, rf.log[rf.commitIndex].AnyValue,
						rf.commitIndex, false, nil, 0, 0}
					rf.applyCh <- msg
					Debug(dCommit, "S%d Follower, AppendRPC received on term %d "+
						">= case with notify update. Committed new entry!"+
						"Index: %d Term=%d Cmd=%d",
						rf.me, args.Term,
						rf.commitIndex, rf.log[rf.commitIndex].IntValue,
						rf.log[rf.commitIndex].AnyValue)
				}
			}
		}
		return
	}

	// Case 2: Leader has lower term - instant rejection.
	Debug(dLog, "S%d Follower, received AppendRPC with lower term=%d, cur=%d"+
		", rejecting...", rf.me, leaderTerm, followerTerm)
	reply.Success = false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Starts the leader logic for elected raft
// Sends heartbeat every to all servers
func (rf *Raft) leaderHeatbeat() {
	Debug(dTimer, "S%d Leader, Heartbeat started", rf.me)
	// Leader Id
	rf.leaderId = rand.Int63()
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if !isLeader {
			Debug(dTimer, "S%d Leader, stopping leader", rf.me)
			// No longer leader, stop sending heartbeats
			return
		}

		// Send the heartbeat append RPC entries
		// contains previous commit index (same as when initialized, or incremented with newly committed indexes
		// contains nil entries
		// contains leaders commit index, which is the same as the currentIndex as nothing is being committed.

		// TODO - need some check to determine if we can send the very last log
		// or the log before last (depending on if we committed)

		// TODO - the heartbeat should be able to check that the accepted
		// rafts have reached consensus on the latest log entry
		// and if not, then the backup append RPCs' should be launched.
		args := AppendEntriesArgs{rf.currentTerm, rf.leaderId, rf.commitIndex,
			rf.log[rf.commitIndex].IntValue, nil, rf.commitIndex}
		for i := range rf.peers {
			// Skip self
			if i == rf.me {
				continue
			}
			// Launch the AppendEntries RPCs in parallel
			go func(peerIndex int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(peerIndex, &args, &reply)

				// false return means that the follower does not contain
				// matching entry for rf.commitIndex

				if !reply.Success {
					// Consistency check failed
					Debug(dError, "S%d Leader, consistency check failed for S%d",
						rf.me, peerIndex)
					// TBD - should I launch consistency fix RPCs if the
					// false return is caused by a disconnect?
					// TBD - how do I decide that this heartbeat had failed previously
					// and these have already been launched?
					rf.AppendAndSync(peerIndex, nil)

				}
			}(i) // Pass 'i' as an argument to the function
		}

		// Heartbeat timeout - sleep for specified time.
		time.Sleep(time.Duration(heartbeatTimeout) * time.Millisecond)
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
			// If a heartBeat has been received, reset check.
			rf.heartBeatReceived = false
		} else {
			// Convert to candidate
			startElection = true
		}

		// Candidate
		// If a heartBeat has NOT been received, hold an election.
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
				Debug(dVote, "S%d Candidate, got a majority vote!", rf.me)
				rf.currentState = Leader
				// Start the leader routine
				go rf.leaderHeatbeat()
			} else {
				// Lost the election
				Debug(dVote, "S%d Candidate, lost the vote!", rf.me)
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}

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
	rf.matchindex = make(map[int]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start electionTicker goroutine to start elections
	go rf.electionTicker()

	return rf
}
