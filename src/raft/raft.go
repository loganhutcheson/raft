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

const HeartbeatTimeout = 100 * time.Millisecond

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
// This is used in the uncommitted log for the Index and the Command
type TermAndCommand struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Channel to send the committed entries
	applyCh chan ApplyMsg

	// Persistent state
	currentTerm int
	votedFor    int64
	log         map[int]TermAndCommand

	// Volatile state
	currentState      RaftState
	heartBeatReceived bool
	leaderId          int64
	commitIndex       int
	lastApplied       int
	nextIndex         map[int]int
	matchindex        map[int]int
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

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	CandidateId             int64
	CandidateTerm           int
	CandidateLastLoggedTerm int
	CandidateLogLength      int
}

// RequestVote RPC reply structure.
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
	Entries      map[int]TermAndCommand
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
// Votes for valid candidates with accept or reject
// based on case[1], case[2]
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// Acquire lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Variables
	currentTerm := rf.currentTerm
	lastLoggedTerm := rf.log[rf.commitIndex].Term
	votedFor := rf.votedFor
	logLength := len(rf.log)
	candidateTerm := args.CandidateTerm
	candidateId := args.CandidateId
	candidateLastLoggedTerm := args.CandidateLastLoggedTerm
	candidateLogLength := args.CandidateLogLength

	// initialize RequestVoteReply to reject
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// if the candidate's term is higher, regardless of whether
	// we voted yes, we need to become a follower and sync terms
	if candidateTerm > currentTerm {
		rf.currentState = Follower     // Set this raft's state to Follower
		rf.currentTerm = candidateTerm // Sync terms
	}

	// Case[1]: candidate has a lower term
	// Reject candidate
	if candidateTerm < currentTerm {
		Debug(dVote, "S%d Follower, rejected candidate %d due to smaller term %d.",
			rf.me, args.CandidateId, args.CandidateTerm)
		return
	}

	// Case[2]: candidate has a greater or equal term
	// Accept candidate under conditions:
	// 		[a]. this raft has not voted this term or already voted this candidate
	//			 meaning the votedFor is null or equal to candidate ID
	//		[b]. *important election restriction*
	//			 that effectively gives power to the highest term/longest log
	//			 due to the implicit fact that winning elections requires a majority,
	//			 and you must win the election to even add to the log
	//		     rule: the candidate log is at least as up-to-date
	//			 where up-to-date means the candidate's last log entry
	//			 is a higher term or if it is equal then make sure
	//			 the candidates log is at least as long

	// condition [a]
	if candidateTerm > currentTerm || votedFor == 0 || votedFor == candidateId {
		// condition [b]
		if candidateLastLoggedTerm > lastLoggedTerm ||
			(candidateLastLoggedTerm == lastLoggedTerm && candidateLogLength >= logLength) {
			reply.VoteGranted = true  // Accept candidate
			rf.votedFor = candidateId // Set votedFor to this candidate
			Debug(dVote, "S%d Follower, granting vote to %d same/greater term %d.", rf.me,
				args.CandidateId, args.CandidateTerm)
		}
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

	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}
	reply.Success = false
	reply.Term = 0
	reply.XLen = 0

	// Loop over sending the AppendEntriesRPC
	for !reply.Success {

		// Acquire lock
		rf.mu.Lock()

		// Check if we are still leader
		_, isLeader := rf.GetState()
		if !isLeader {
			// Release lock
			rf.mu.Unlock()
			return false
		}

		// Reset newEntries
		newEntries := make(map[int]TermAndCommand)

		// If the failure is because of follower on
		// higher term, then we don't try to fix consistency
		// revert back to follower
		if reply.Term > rf.currentTerm {
			rf.currentState = Follower
			// Release lock
			rf.mu.Unlock()
			return false
		}

		// Fast Backup
		if reply.XLen > 0 {
			if reply.XTerm == -1 {
				// Follower does not have an entry for this index
				// backup to the last index of the follower's log
				Debug(dInfo, "S%d Leader - setting nextIndex for S%d to %d (case 1)",
					rf.me, server, reply.XLen)
				rf.nextIndex[server] = reply.XLen
				if heartbeat {
					Debug(dInfo, "S%d Leader - heartbeat", rf.me)
				}
			} else {
				// Check if leader has the follower's mismatched term
				found := -1
				for i := 1; i < len(rf.log); i++ {
					if rf.log[i].Term == reply.XTerm {
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

		// prevLogIndex is the index immediately proceeding
		// any new entries. If there are no new entries, send
		// the commitIndex
		prevLogIndex := 0
		if len(newEntries) == 0 {
			prevLogIndex = rf.commitIndex
		} else {
			prevLogIndex = rf.nextIndex[server] - 1
		}
		prevLogTerm := rf.log[prevLogIndex].Term

		// Reset Reply to avoid labgob decode error
		reply = AppendEntriesReply{}

		args = AppendEntriesArgs{rf.currentTerm, rf.leaderId,
			prevLogIndex, prevLogTerm, newEntries, rf.commitIndex}

		// Release lock
		rf.mu.Unlock()

		// Block on send
		rf.sendAppendEntries(server, &args, &reply)

	}

	// Acquire lock
	rf.mu.Lock()

	// Successful commit by this raft
	// Set nextIndex, so we know which is the next log to commit.
	// Increment the matchIndex so we know we commited a new log.

	// Guard against a concurrent start that has already added
	// a larger set of these entries.
	if rf.nextIndex[server] < logIndex+1 {
		rf.nextIndex[server] = logIndex + 1
		rf.matchindex[server] = logIndex
		Debug(dInfo, "S%d Leader - setting nextIndex for S%d to %d (success case)",
			rf.me, server, logIndex+1)
	}

	// Release lock
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

	// Acquire lock
	rf.mu.Lock()
	// Defer lock
	defer rf.mu.Unlock()

	// "Return false if not leader" -- see professor's note ;)
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	Debug(dLeader, "S%d Leader, Start() has been called for cmd: %d!",
		rf.me, command)

	// Append the new entry to the leader's log
	rf.log[len(rf.log)+1] = TermAndCommand{rf.currentTerm, command}
	logIndex := len(rf.log)

	// Issue RPCs in parallel
	commitResult := make(chan bool)
	for i := range rf.peers {
		// Skip self
		if i == rf.me {
			continue
		}
		// Launch a separate thread for each raft
		go func(peerIndex int) {
			// AppendAndSync could take a while and may never return to this channel
			result := rf.AppendAndSync(peerIndex, logIndex, false)
			// Send the results to commitResult channel
			// *the thread started below*
			commitResult <- result
		}(i)
	}

	// Start() returns immediately
	// however, this thread will wait to count the votes and commit
	// with a majority of rafts adding the log
	go func() {
		// returnedCount counts the number of rafts that returned at ALL
		returnedCount := 1
		// appendedCount counts the number of rafts that returned True
		// indicating that the log has been appended
		appendedCount := 1

		// Iterate over the commitResult channel
		// In Go, this blocks until we receive a result from a separate thread
		for result := range commitResult {

			// "Return false if not leader" -- see professor's note ;)
			_, isLeader := rf.GetState()
			if !isLeader {
				return
			}

			returnedCount++
			// If the return code was true
			if result {
				Debug(dLeader, "S%d Leader, got an approval"+
					" Index=%d Term=%d Cmd=%d!",
					rf.me, logIndex, rf.currentTerm, command)

				appendedCount++
				// Check if a majority logs appended (n/2+1)
				if appendedCount == (len(rf.peers)/2)+1 {

					// Acquire lock
					rf.mu.Lock()

					// Guard against a concurrent start that has already added
					// a larger set of these entries.
					if rf.commitIndex < logIndex {
						rf.commitIndex = logIndex
					}
					// Apply logs to state machine in-order
					for rf.lastApplied < rf.commitIndex {
						rf.lastApplied++
						msg := ApplyMsg{true, rf.log[rf.lastApplied].Command,
							rf.lastApplied, false, nil, 0, 0}
						rf.applyCh <- msg
						Debug(dCommit, "S%d Leader, Committing Entry!"+
							"Index:%d Term=%d Cmd=%d",
							rf.me, rf.lastApplied, rf.log[rf.lastApplied].Term,
							rf.log[rf.lastApplied].Command)
					}

					// Release lock
					rf.mu.Unlock()

					// Break out and return
					break
				}
			} else {
				Debug(dLeader, "S%d Leader, got a rejection for log %d!",
					rf.me, command)
			}

			// Return if we received replies from all rafts
			// and still do not have a majority of raft's logging
			// the new entry, or we reached majority
			if returnedCount >= len(rf.peers) {
				return
			}
		}
	}()

	return logIndex, term, isLeader
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

	// Acquire Lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO - remove
	// Debug(dCommit, "S%d Follower, received AppendEntries %d %d %d", rf.me, args.LeaderCommit, len(rf.log), rf.lastApplied)

	leadersTerm := args.Term
	leadersCommit := args.LeaderCommit
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if leadersTerm > rf.currentTerm {
		rf.currentTerm = leadersTerm
		rf.currentState = Follower
	}

	// [1] Reply false if term < currentTerm
	if leadersTerm < rf.currentTerm {
		reply.Success = false
		return
	} else {
		rf.heartBeatReceived = true
	}
	// [2] Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if rf.log[prevLogIndex].Term != prevLogTerm {
		Debug(dDrop, "S%d Follower, fast backup on index %d", rf.me, prevLogIndex)
		reply.Success = false
		// Fast Backup
		if rf.log[prevLogIndex].Term != 0 {
			// Follower has a conflicting Xterm at PrevLogIndex
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			// Find the first index of XTerm in the follower's log
			index := args.PrevLogIndex
			for rf.log[index].Term == reply.XTerm {
				if index > 0 {
					index--
				} else {
					break
				}
			}
			reply.XIndex = index + 1
			reply.XLen = len(rf.log)
			// Follow
			return
		} else {
			// Follower has no entry at PrevLogIndex
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = len(rf.log)
		}
		return
	}

	// [3] Perform consistency check
	// Note: this check needs to occur for both heartbeats and AppendRPC
	// containing new entries.
	// If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all the follow it.
	// if len(args.Entries) > 0 {
	indexRange := prevLogIndex + len(args.Entries)
	mismatch := false
	mismatchIndex := 0
	for index := (prevLogIndex + 1); index <= indexRange; index++ {
		// First check if the index exists in the follower's log
		if rf.log[index].Term != 0 {
			if rf.log[index].Term != args.Entries[index].Term {
				mismatch = true
				mismatchIndex = index
				break
			}
		}
	}

	if mismatch {
		// Save the original log length as we will be deleting entries
		logLength := len(rf.log)
		for index := mismatchIndex; index <= logLength; index++ {
			Debug(dDrop, "S%d Follower, dropping mismatch on Index=%d"+
				" Term=%d Cmd=%d",
				rf.me, index, rf.log[index].Term, rf.log[index].Command)
			delete(rf.log, index)
		}
	}

	// [4] Append any new entries not already in the log
	// and delete any stale logs after the appended logs (append only)
	for index := (prevLogIndex + 1); index <= indexRange; index++ {
		if rf.log[index].Term == 0 {
			rf.log[index] = args.Entries[index]
			Debug(dLog, "S%d Follower, Logging new entry! "+
				"Index=%d Term=%d Cmd=%d logSize=%d",
				rf.me, index,
				args.Entries[index].Term, args.Entries[index].Command, len(rf.log))
		}
	}

	// Logan - there is a problem here. If there is an index 52 and the leader sends 1-52 with commitIndex > 52,
	// then we can commit a stale 52.
	// The problem was my initial understanding of "last new entry"
	// that is not the follower's log which can contain stale entries.
	// Rather, it is the lew entries being sent by the leader. Which
	// are guaranteed to be correct.

	// [5] If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if leadersCommit > rf.commitIndex {
		rf.commitIndex = min(leadersCommit, indexRange)
	}

	// If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{true, rf.log[rf.lastApplied].Command,
			rf.lastApplied, false, nil, 0, 0}
		rf.applyCh <- msg
		Debug(dCommit, "S%d Follower, Committing new entry on term%d, commitIndex:%d, lastApplied:%d!"+
			" Index: %d Term=%d Cmd=%d prevLogIndex=%d prevLogTerm=%d ",
			rf.me, args.Term, rf.commitIndex, rf.lastApplied,
			rf.lastApplied, rf.log[rf.lastApplied].Term,
			rf.log[rf.lastApplied].Command, prevLogIndex, prevLogTerm)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// startLeaderHeartbeat starts the heartbeat process for a leader Raft instance.
// Sends a heartbeat (AppendEntries) RPC to all peers at a regular interval.
func (rf *Raft) startLeaderHeartbeat() {
	Debug(dLeader, "S%d Leader, starting heartbeat", rf.me)
	rf.leaderId = rand.Int63()

	// Initialize next Index
	for i := range rf.peers {
		if i == rf.me { // Skip self
			continue
		}
		rf.nextIndex[i] = len(rf.log) + 1
	}

	// Helper function to send heartbeats
	sendHeartbeats := func() {
		for i := range rf.peers {
			if i == rf.me { // Skip self
				continue
			}
			// logIndex is the point where we sync
			// for the heartbest, sync to the commitIndex
			go rf.AppendAndSync(i, rf.commitIndex, true) // Send RPC in parallel
		}
	}

	// Heartbeat loop
	ticker := time.NewTicker(HeartbeatTimeout)
	defer ticker.Stop()

	for !rf.killed() && rf.isLeader() {
		sendHeartbeats()
		<-ticker.C // Wait for next heartbeat interval
	}

	Debug(dLeader, "S%d Leader, stopping heartbeat", rf.me)
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
			args := RequestVoteArgs{candidateId, rf.currentTerm,
				rf.log[len(rf.log)].Term, len(rf.log)}

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
				go rf.startLeaderHeartbeat()
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
	rf.log = make(map[int]TermAndCommand)

	// Volatile state
	rf.currentState = Follower
	rf.heartBeatReceived = false
	rf.leaderId = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make(map[int]int)
	// nextIndex initialized to leader last log + 1
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log) + 1
	}

	rf.matchindex = make(map[int]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start electionTicker goroutine to start elections
	go rf.electionTicker()

	return rf
}
