package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// Globals timeouts (ms)
var electionTimeout int64 = 1000

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
type LogEntry struct {
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
	log         []LogEntry

	// Volatile state
	currentState      RaftState
	heartBeatReceived bool
	leaderId          int64
	commitIndex       int
	lastApplied       int
	nextIndex         map[int]int
	matchindex        map[int]int
	lastLogIndex      int // Track the highest log index

	// 3D Snapshot
	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte
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
	// Create the serialized raft state
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastLogIndex)
	raftstate := w.Bytes()

	// TODO: Add snapshot handling when implementing 3D
	rf.persister.Save(raftstate, nil)

	size := rf.persister.RaftStateSize()

	Debug(dPersist, "S%d persist(), size=%d "+
		"currentTerm%d loglength=%d votedFor=%d",
		rf.me, size, rf.currentTerm, len(rf.log), rf.votedFor)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var votedFor int64
	var currentTerm int
	var lastLogIndex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastLogIndex) != nil {
		Debug(dError, "S%d Error decoding persisted state", rf.me)
	} else {
		rf.log = log
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.lastLogIndex = lastLogIndex

		// Validate lastLogIndex against actual log entries
		actualLastIndex := len(rf.log) - 1
		if actualLastIndex != rf.lastLogIndex {
			Debug(dPersist, "S%d Correcting lastLogIndex from %d to %d", rf.me, rf.lastLogIndex, actualLastIndex)
			rf.lastLogIndex = actualLastIndex
		}

		Debug(dPersist, "S%d reboot. Found log of length %d", rf.me, rf.lastLogIndex)

		// TODO: Add snapshot restoration when implementing 3D
	}
}

// TODO: Implement Snapshot function for 3D
// The service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// The tester calls Snapshot() periodically
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// TODO: Implement snapshot functionality for 3D
	// This will involve:
	// 1. Storing logs up to index in snapshot
	// 2. Trimming the log to remove entries up to index
	// 3. Updating snapshot metadata
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
	Entries      []LogEntry
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

// TODO: Add InstallSnapshot structs and functions when implementing 3D

// RequestVote RPC handler.
// Votes for valid candidates with accept or reject
// based on case[1], case[2]
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Variables
	currentTerm := rf.currentTerm
	lastLoggedTerm := 0
	if rf.lastLogIndex > 0 && rf.lastLogIndex < len(rf.log) {
		lastLoggedTerm = rf.log[rf.lastLogIndex].Term
	}
	votedFor := rf.votedFor
	logLength := rf.lastLogIndex
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
		rf.currentState = Follower
		rf.currentTerm = candidateTerm
		rf.persist()
	}

	// Case[1]: candidate has a lower term
	if candidateTerm < currentTerm {
		Debug(dVote, "S%d Follower, rejected candidate %d due to smaller term %d.",
			rf.me, args.CandidateId, args.CandidateTerm)
		return
	}

	// Case[2]: candidate has a greater or equal term
	// Accept candidate under conditions:
	// [a]. this raft has not voted this term or already voted this candidate
	// [b]. candidate log is at least as up-to-date
	if candidateTerm > currentTerm || votedFor == 0 || votedFor == candidateId {
		if candidateLastLoggedTerm > lastLoggedTerm ||
			(candidateLastLoggedTerm == lastLoggedTerm && candidateLogLength >= logLength) {
			reply.VoteGranted = true
			rf.votedFor = candidateId
			rf.persist()
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

// sendAppendEntriesToPeer sends a single AppendEntries RPC to a peer
// and handles the response, updating nextIndex and matchIndex accordingly
func (rf *Raft) sendAppendEntriesToPeer(server int, isHeartbeat bool) bool {
	rf.mu.Lock()

	// Check if we are still leader
	if rf.currentState != Leader {
		rf.mu.Unlock()
		return false
	}

	// Determine what entries to send
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	// Prepare entries to send
	var entries []LogEntry
	if !isHeartbeat {
		// Send all entries from nextIndex to end of log
		startIndex := rf.nextIndex[server]
		if startIndex < len(rf.log) {
			entries = rf.log[startIndex:]
		}
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	// Send RPC
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)

	if !ok {
		// RPC failed, but don't retry here to avoid infinite recursion
		// The heartbeat loop will retry automatically
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check if we're still leader and term hasn't changed
	if rf.currentState != Leader || rf.currentTerm != args.Term {
		return false
	}

	// Handle higher term
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentState = Follower
		rf.votedFor = 0
		rf.persist()
		return false
	}

	if reply.Success {
		// Update nextIndex and matchIndex
		if len(entries) > 0 {
			// Calculate the highest index we sent
			highestSentIndex := prevLogIndex + len(entries)
			rf.nextIndex[server] = highestSentIndex + 1
			rf.matchindex[server] = highestSentIndex
		}

		// Try to commit new entries
		rf.tryCommitEntries()
		return true
	} else {
		// Handle fast backup
		if reply.XLen > 0 {
			if reply.XTerm == -1 {
				// Follower doesn't have entry at prevLogIndex
				rf.nextIndex[server] = reply.XLen
			} else {
				// Find the first index of XTerm in leader's log
				found := -1
				for i := 1; i <= rf.lastLogIndex && i < len(rf.log); i++ {
					if rf.log[i].Term == reply.XTerm {
						found = i
						break
					}
				}
				if found >= 0 {
					rf.nextIndex[server] = found
				} else {
					rf.nextIndex[server] = reply.XIndex
				}
			}
		} else {
			// Simple decrement for unknown conflicts
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
		}

		return false
	}
}

// tryCommitEntries attempts to commit new entries based on matchIndex
func (rf *Raft) tryCommitEntries() {
	for n := rf.commitIndex + 1; n <= rf.lastLogIndex; n++ {
		if n < len(rf.log) && rf.log[n].Term > 0 {
			count := 1 // Count self
			for peer := range rf.peers {
				if peer != rf.me && rf.matchindex[peer] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
			} else {
				break
			}
		}
	}

	// Apply committed entries
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		if rf.lastApplied < len(rf.log) {
			entry := rf.log[rf.lastApplied]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- msg
			Debug(dCommit, "S%d Leader, Committing Entry! Index:%d Term=%d Cmd=%d",
				rf.me, rf.lastApplied, entry.Term, entry.Command)
		}
	}
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

	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	Debug(dLeader, "S%d Leader, Start() has been called for cmd: %d!",
		rf.me, command)

	// Append the new entry to the leader's log
	rf.lastLogIndex++
	// Ensure log is large enough
	for len(rf.log) <= rf.lastLogIndex {
		rf.log = append(rf.log, LogEntry{})
	}
	rf.log[rf.lastLogIndex] = LogEntry{rf.currentTerm, command}
	rf.persist()

	logIndex := rf.lastLogIndex

	// Trigger immediate replication to all followers
	go func() {
		// Track commit status
		commitCount := 1 // Count self
		commitChan := make(chan bool, len(rf.peers)-1)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peerIndex int) {
				if !rf.killed() {
					success := rf.sendAppendEntriesToPeer(peerIndex, false)
					commitChan <- success
				} else {
					commitChan <- false
				}
			}(i)
		}

		// Wait for responses and track commits
		for i := 0; i < len(rf.peers)-1; i++ {
			select {
			case success := <-commitChan:
				if success {
					commitCount++
					if commitCount > len(rf.peers)/2 {
						// Majority achieved, entry should be committed
						break
					}
				}
			case <-time.After(100 * time.Millisecond):
				// Timeout to prevent hanging
				break
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	leadersTerm := args.Term
	leadersCommit := args.LeaderCommit
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if leadersTerm > rf.currentTerm {
		rf.currentTerm = leadersTerm
		rf.persist()
		rf.currentState = Follower
	}

	// [1] Reply false if term < currentTerm
	if leadersTerm < rf.currentTerm {
		reply.Success = false
		return
	} else {
		rf.heartBeatReceived = true
	}

	// Default fast backup
	reply.XIndex = -1
	reply.XLen = -1
	reply.XTerm = -1

	// [2] Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if prevLogIndex >= len(rf.log) || rf.log[prevLogIndex].Term != prevLogTerm {
		term := 0
		if prevLogIndex < len(rf.log) {
			term = rf.log[prevLogIndex].Term
		}
		Debug(dDrop, "S%d Follower, fast backup on index %d, loglen %d term=%d does not match leader's term=%d",
			rf.me, prevLogIndex, len(rf.log), term, prevLogTerm)
		reply.Success = false
		// Fast Backup
		if prevLogIndex < len(rf.log) && rf.log[prevLogIndex].Term != 0 {
			// Follower has a conflicting Xterm at PrevLogIndex
			reply.XTerm = rf.log[prevLogIndex].Term

			// Find the first index of XTerm in the follower's log
			index := args.PrevLogIndex
			for {
				if index < len(rf.log) && rf.log[index].Term == reply.XTerm {
					if index > 0 {
						index--
					} else {
						break
					}
				} else {
					break
				}
			}
			reply.XIndex = index + 1
			reply.XLen = max(rf.lastLogIndex, 1)
			return
		} else {
			// Follower has no entry at PrevLogIndex
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = max(rf.lastLogIndex, 1)
		}
		return
	}

	// [3] Perform consistency check
	indexRange := prevLogIndex + len(args.Entries)
	mismatch := false
	mismatchIndex := 0
	for i, argsEntry := range args.Entries {
		index := prevLogIndex + 1 + i
		// First check if the index exists in the follower's log
		if index < len(rf.log) && rf.log[index].Term != argsEntry.Term {
			mismatch = true
			mismatchIndex = index
			break
		}
	}

	if mismatch {
		// Truncate log from mismatchIndex onwards
		if mismatchIndex < len(rf.log) {
			rf.log = rf.log[:mismatchIndex]
			rf.lastLogIndex = max(0, mismatchIndex-1)
			Debug(dDrop, "S%d Follower, truncating log at index %d", rf.me, mismatchIndex)
			rf.persist()
		}
	}

	// [4] Append any new entries not already in the log
	for i, argsEntry := range args.Entries {
		index := prevLogIndex + 1 + i
		// Ensure log is large enough
		for len(rf.log) <= index {
			rf.log = append(rf.log, LogEntry{})
		}
		rf.log[index] = argsEntry
		// Update lastLogIndex if this is a new entry
		if index > rf.lastLogIndex {
			rf.lastLogIndex = index
		}
		Debug(dLog, "S%d Follower, Logging new entry! "+
			"Index=%d Term=%d Cmd=%d logSize=%d",
			rf.me, index,
			argsEntry.Term, argsEntry.Command, rf.lastLogIndex)
		rf.persist()
	}

	// [5] If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if leadersCommit > rf.commitIndex {
		rf.commitIndex = min(leadersCommit, indexRange)
	}

	// If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		if rf.lastApplied < len(rf.log) {
			entry := rf.log[rf.lastApplied]
			msg := ApplyMsg{true, entry.Command,
				rf.lastApplied, false, nil, 0, 0}
			rf.applyCh <- msg
			Debug(dCommit, "S%d Follower, Committing new entry on term%d, commitIndex:%d, lastApplied:%d!"+
				" Index: %d Term=%d Cmd=%d prevLogIndex=%d prevLogTerm=%d ",
				rf.me, args.Term, rf.commitIndex, rf.lastApplied,
				rf.lastApplied, entry.Term,
				entry.Command, prevLogIndex, prevLogTerm)
			rf.persist()
		}
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

	// Initialize nextIndex and matchIndex
	for i := range rf.peers {
		if i == rf.me { // Skip self
			continue
		}
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchindex[i] = 0
	}

	// Heartbeat loop
	ticker := time.NewTicker(HeartbeatTimeout)
	defer ticker.Stop()

	for !rf.killed() && rf.isLeader() {
		// Send heartbeats to all peers
		for i := range rf.peers {
			if i == rf.me { // Skip self
				continue
			}
			go rf.sendAppendEntriesToPeer(i, true)
		}

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
		var wg sync.WaitGroup
		wg.Add(1)
		var election_timeout = make(chan bool, 1)
		go func() {
			defer wg.Done()
			ms := electionTimeout + (rand.Int63() % 250)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			election_timeout <- true
		}()

		// Continue for leader, leader breaks only when receiving a
		// RPC with higher term
		_, isLeader := rf.GetState()
		if isLeader {
			wg.Wait()
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
			rf.currentState = Candidate
			candidateId := rand.Int63()
			rf.votedFor = candidateId
			votes := 1
			rf.currentTerm++
			rf.persist()
			Debug(dVote, "S%d Follower, starting election id=%d, term=%d",
				rf.me, candidateId, rf.currentTerm)

			// Set term information for filtering election candidates
			lastLoggedTerm := 0
			if rf.lastLogIndex > 0 && rf.lastLogIndex < len(rf.log) {
				lastLoggedTerm = rf.log[rf.lastLogIndex].Term
			}
			args := RequestVoteArgs{candidateId, rf.currentTerm,
				lastLoggedTerm, rf.lastLogIndex}

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

			// Election results
			if majorityVote {
				// Won election, become leader
				Debug(dVote, "S%d Candidate, won a majority vote!", rf.me)
				rf.currentState = Leader

				// When a leader comes to power, initialize all the nextIndex values
				// to just after the last one of it's log
				for i := range rf.peers {
					Debug(dVote, "S%d initialized nextIndex to %d", rf.me, rf.lastLogIndex+1)
					rf.nextIndex[i] = rf.lastLogIndex + 1
				}

				// Start the leader routine
				go rf.startLeaderHeartbeat()
			} else {
				// Lost the election
				Debug(dVote, "S%d Candidate, lost the election!", rf.me)
			}
		}
		rf.mu.Unlock()

		wg.Wait()
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

	rf.applyCh = applyCh

	// Persistent state
	rf.currentTerm = 0
	rf.votedFor = 0
	rf.log = make([]LogEntry, 0) // Initialize as empty slice
	rf.lastLogIndex = 0

	// Volatile state
	rf.currentState = Follower
	rf.heartBeatReceived = false
	rf.leaderId = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make(map[int]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Add dummy entry at index 0 only if log is empty
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})
		rf.lastLogIndex = 0
	}

	// Initialize nextIndex after restoring persistent state
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}

	rf.matchindex = make(map[int]int)

	// start electionTicker goroutine to start elections
	go rf.electionTicker()

	// Log that we started (or restarted)
	Debug(dInfo, "S%d, restarted", rf.me)

	return rf
}
