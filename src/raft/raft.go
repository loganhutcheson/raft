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

	// 3D Snapshot
	snapshotEnabled bool // Flag to enable/disable snapshot functionality
	snapshotIndex   int
	snapshotTerm    int
	snapshot        []byte
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
	raftstate := w.Bytes()

	// Save the snapshot data as provided by the test
	rf.persister.Save(raftstate, rf.snapshot)

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

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		Debug(dError, "S%d Error decoding persisted state", rf.me)
	} else {
		rf.log = log
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm

		Debug(dPersist, "S%d reboot. Found log of length %d", rf.me, len(rf.log))
	}
}

// restore snapshot data
func (rf *Raft) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	// Store the snapshot data as-is
	rf.snapshot = snapshot
	Debug(dSnap, "S%d restored snapshot of size %d", rf.me, len(snapshot))
}

// The service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// The tester calls Snapshot() periodically
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	Debug(dSnap, "S%d Snapshot: attempting to acquire lock", rf.me)

	rf.mu.Lock()

	// Check if snapshots are enabled
	if !rf.snapshotEnabled {
		Debug(dSnap, "S%d Snapshot disabled, ignoring snapshot request at index %d", rf.me, index)
		Debug(dSnap, "S%d Snapshot: releasing lock", rf.me)
		rf.mu.Unlock()
		return
	}

	// Ignore if index not ahead of current snapshot
	if index <= rf.snapshotIndex {
		Debug(dSnap, "S%d Snapshot ignored, index %d <= current snapshot index %d", rf.me, index, rf.snapshotIndex)
		Debug(dSnap, "S%d Snapshot: releasing lock", rf.me)
		rf.mu.Unlock()
		return
	}

	// Validate index does not exceed last global log index
	lastGlobalIndex := rf.snapshotIndex + len(rf.log) - 1
	if index > lastGlobalIndex {
		Debug(dSnap, "S%d Snapshot ignored, index %d > last log index %d", rf.me, index, lastGlobalIndex)
		Debug(dSnap, "S%d Snapshot: releasing lock", rf.me)
		rf.mu.Unlock()
		return
	}

	// Basic check: ensure the snapshot index is committed
	if index > rf.commitIndex {
		Debug(dSnap, "S%d delaying snapshot at index %d until it's committed (current commitIndex: %d)",
			rf.me, index, rf.commitIndex)
		Debug(dSnap, "S%d Snapshot: releasing lock", rf.me)
		rf.mu.Unlock()
		return
	}

	// Convert global index to slice index and validate
	sliceIndex := index - rf.snapshotIndex
	if sliceIndex < 0 || sliceIndex >= len(rf.log) {
		Debug(dSnap, "S%d Snapshot aborted, invalid slice index %d", rf.me, sliceIndex)
		rf.mu.Unlock()
		return
	}

	// Get term at snapshot index and create a snapshot of current state
	snapshotTerm := rf.log[sliceIndex].Term
	if snapshotTerm == 0 {
		Debug(dSnap, "S%d Snapshot aborted, entry at index %d has term 0", rf.me, index)
		rf.mu.Unlock()
		return
	}

	// Store values needed for persistence while holding lock
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	newLog := rf.log[sliceIndex+1:] // New log after truncation

	// Release lock before expensive persistence operation
	rf.mu.Unlock()

	// Do expensive I/O operations without holding lock
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(currentTerm)
	e.Encode(votedFor)
	e.Encode(newLog)
	raftstate := w.Bytes()

	// Copy snapshot data
	snapshotData := make([]byte, len(snapshot))
	copy(snapshotData, snapshot)

	// Re-acquire lock to apply changes atomically
	rf.mu.Lock()

	// CRITICAL: Validate that log state hasn't changed invalidly during I/O
	// Check that we can still apply this snapshot
	if index <= rf.snapshotIndex {
		Debug(dSnap, "S%d Snapshot cancelled, already have newer snapshot %d >= %d",
			rf.me, rf.snapshotIndex, index)
		rf.mu.Unlock()
		return
	}

	// Re-validate the slice index
	sliceIndex = index - rf.snapshotIndex
	if sliceIndex < 0 || sliceIndex >= len(rf.log) {
		Debug(dSnap, "S%d Snapshot cancelled, log structure changed during I/O", rf.me)
		rf.mu.Unlock()
		return
	}

	// Re-validate that the entry at the snapshot index hasn't changed
	if rf.log[sliceIndex].Term != snapshotTerm {
		Debug(dSnap, "S%d Snapshot cancelled, log entry term changed during I/O", rf.me)
		rf.mu.Unlock()
		return
	}

	// Apply the snapshot atomically
	rf.log = rf.log[sliceIndex+1:]
	rf.snapshot = snapshotData
	rf.snapshotIndex = index
	rf.snapshotTerm = snapshotTerm

	// Ensure commitIndex and lastApplied are at least the snapshot index
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	Debug(dSnap, "S%d Snapshot applied at index %d, term %d, log size now %d",
		rf.me, index, snapshotTerm, len(rf.log))

	// Save the pre-computed persistence data
	rf.persister.Save(raftstate, snapshotData)
	size := rf.persister.RaftStateSize()
	Debug(dPersist, "S%d persist() after snapshot, size=%d currentTerm%d loglength=%d votedFor=%d",
		rf.me, size, currentTerm, len(newLog), votedFor)

	rf.mu.Unlock()
}

// EnableSnapshot enables or disables the snapshot functionality
func (rf *Raft) EnableSnapshot(enabled bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapshotEnabled = enabled
	Debug(dSnap, "S%d Snapshot functionality %s", rf.me, map[bool]string{true: "enabled", false: "disabled"}[enabled])
}

// IsSnapshotEnabled returns whether snapshot functionality is enabled
func (rf *Raft) IsSnapshotEnabled() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.snapshotEnabled
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

// InstallSnapshot RPC arguments
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC reply
type InstallSnapshotReply struct {
	Term int
}

// RequestVote RPC handler.
// Votes for valid candidates with accept or reject
// based on case[1], case[2]
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	Debug(dVote, "S%d RequestVote: attempting to acquire lock", rf.me)
	rf.mu.Lock()
	Debug(dVote, "S%d RequestVote: acquired lock", rf.me)
	defer func() {
		Debug(dVote, "S%d RequestVote: releasing lock", rf.me)
		rf.mu.Unlock()
	}()

	// Variables
	currentTerm := rf.currentTerm
	lastLoggedTerm := 0
	// Calculate the actual last log index
	if len(rf.log) > 0 {
		lastLoggedTerm = rf.log[len(rf.log)-1].Term
	} else if rf.snapshotIndex > 0 {
		// If log is empty but we have a snapshot, use snapshot term
		lastLoggedTerm = rf.snapshotTerm
	}
	votedFor := rf.votedFor
	logLength := rf.snapshotIndex + len(rf.log) // Convert to global index
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

	// DEBUG: Log the vote request details
	Debug(dVote, "S%d Follower, RequestVote received: candidate=%d, candidateTerm=%d, candidateLastLogTerm=%d, candidateLogLength=%d",
		rf.me, args.CandidateId, args.CandidateTerm, args.CandidateLastLoggedTerm, args.CandidateLogLength)
	Debug(dVote, "S%d Follower, current state: term=%d, votedFor=%d, lastLogTerm=%d, logLength=%d, snapshotIndex=%d, snapshotTerm=%d",
		rf.me, currentTerm, votedFor, lastLoggedTerm, logLength, rf.snapshotIndex, rf.snapshotTerm)

	// Case[1]: candidate has a lower term
	if candidateTerm < currentTerm {
		Debug(dVote, "S%d Follower, REJECTED candidate %d due to smaller term %d < %d.",
			rf.me, args.CandidateId, args.CandidateTerm, currentTerm)
		return
	}

	// Case[2]: candidate has a greater or equal term
	// Accept candidate under conditions:
	// [a]. this raft has not voted this term or already voted this candidate
	// [b]. candidate log is at least as up-to-date
	Debug(dVote, "S%d Follower, checking vote conditions: candidateTerm=%d, currentTerm=%d, votedFor=%d, candidateId=%d",
		rf.me, candidateTerm, currentTerm, votedFor, candidateId)
	Debug(dVote, "S%d Follower, checking log up-to-date: candidateLastLogTerm=%d, lastLoggedTerm=%d, candidateLogLength=%d, logLength=%d",
		rf.me, candidateLastLoggedTerm, lastLoggedTerm, candidateLogLength, logLength)

	if candidateTerm > currentTerm || votedFor == 0 || votedFor == candidateId {
		Debug(dVote, "S%d Follower, vote condition [a] satisfied", rf.me)
		if candidateLastLoggedTerm > lastLoggedTerm ||
			(candidateLastLoggedTerm == lastLoggedTerm && candidateLogLength >= logLength) {
			Debug(dVote, "S%d Follower, log up-to-date condition [b] satisfied", rf.me)
			reply.VoteGranted = true
			rf.votedFor = candidateId
			rf.persist()
			Debug(dVote, "S%d Follower, GRANTED vote to %d for term %d.", rf.me,
				args.CandidateId, args.CandidateTerm)
		} else {
			Debug(dVote, "S%d Follower, REJECTED candidate %d: log not up-to-date", rf.me, args.CandidateId)
		}
	} else {
		Debug(dVote, "S%d Follower, REJECTED candidate %d: already voted for %d in term %d", rf.me, args.CandidateId, votedFor, currentTerm)
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
	Debug(dLog, "S%d sendAppendEntriesToPeer: attempting to acquire lock", rf.me)
	rf.mu.Lock()
	Debug(dLog, "S%d sendAppendEntriesToPeer: acquired lock", rf.me)

	// DEBUG: Log the initial state for this peer
	Debug(dLog, "S%d Leader, sendAppendEntriesToPeer: server=%d, nextIndex=%d, matchIndex=%d, snapshotIndex=%d, logLen=%d, isHeartbeat=%v",
		rf.me, server, rf.nextIndex[server], rf.matchindex[server], rf.snapshotIndex, len(rf.log), isHeartbeat)

	// Check if we are still leader
	if rf.currentState != Leader {
		Debug(dLog, "S%d Leader, sendAppendEntriesToPeer: no longer leader, returning false", rf.me)
		rf.mu.Unlock()
		return false
	}

	// Determine what entries to send
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0

	// If nextIndex is at the snapshot boundary, we should start from there
	// This prevents the infinite loop where we try to send entries before the snapshot
	if rf.nextIndex[server] == rf.snapshotIndex {
		prevLogIndex = rf.snapshotIndex
		prevLogTerm = rf.snapshotTerm
	} else if prevLogIndex < rf.snapshotIndex {
		// If prevLogIndex is before our snapshot, we can't reference it
		// This will cause the follower to reject the RPC and provide fast backup info
		Debug(dLog, "S%d Leader, prevLogIndex=%d < snapshotIndex=%d, will be rejected by follower",
			rf.me, prevLogIndex, rf.snapshotIndex)
		// Use snapshotTerm when prevLogIndex is before our snapshot
		prevLogTerm = rf.snapshotTerm
	}

	// Handle term lookup for prevLogIndex
	if prevLogIndex == rf.snapshotIndex {
		// prevLogIndex is exactly at the snapshot boundary
		prevLogTerm = rf.snapshotTerm
	} else if prevLogIndex > rf.snapshotIndex {
		// prevLogIndex is in our current log
		prevLogSliceIndex := prevLogIndex - rf.snapshotIndex
		if prevLogSliceIndex >= 0 && prevLogSliceIndex < len(rf.log) {
			prevLogTerm = rf.log[prevLogSliceIndex].Term
		}
	} else if prevLogIndex < rf.snapshotIndex {
		// This should not happen if the test keeps servers synchronized
		// But if it does, we'll let the follower handle it with fast backup
		Debug(dLog, "S%d WARNING: prevLogIndex=%d < snapshotIndex=%d for server %d, using snapshotTerm",
			rf.me, prevLogIndex, rf.snapshotIndex, server)
		// Use snapshotTerm when prevLogIndex is before our snapshot
		prevLogTerm = rf.snapshotTerm
	}

	// Prepare entries to send
	var entries []LogEntry
	startSliceIndex := rf.nextIndex[server] - rf.snapshotIndex
	if startSliceIndex >= 0 && startSliceIndex < len(rf.log) {
		entries = rf.log[startSliceIndex:]
		Debug(dLog, "S%d Leader, sending %d entries to server %d starting from slice index %d (global index %d)",
			rf.me, len(entries), server, startSliceIndex, rf.nextIndex[server])
	} else if startSliceIndex >= len(rf.log) {
		// nextIndex is beyond our log, so no entries to send
		entries = []LogEntry{}
		Debug(dLog, "S%d Leader, no entries to send to server %d (nextIndex=%d beyond log)",
			rf.me, server, rf.nextIndex[server])

		// If nextIndex is beyond our log, we can't send any entries
		// The follower will need to catch up through normal Raft mechanisms
	} else {
		// startSliceIndex < 0 means follower is behind our snapshot
		Debug(dLog, "S%d Leader, follower %d is behind snapshot (nextIndex=%d, snapshotIndex=%d), sending InstallSnapshot",
			rf.me, server, rf.nextIndex[server], rf.snapshotIndex)

		// CRITICAL: Mark that we're sending InstallSnapshot by updating nextIndex immediately
		// This prevents concurrent AppendEntries from interfering
		rf.nextIndex[server] = rf.snapshotIndex + 1
		rf.matchindex[server] = 0 // Reset match index since we're resending everything

		rf.mu.Unlock()
		return rf.sendInstallSnapshotToServer(server)
	}

	// If we have no entries to send and this is not a heartbeat, return
	if len(entries) == 0 && !isHeartbeat {
		rf.mu.Unlock()
		return false
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	Debug(dLog, "S%d sendAppendEntriesToPeer: releasing lock before RPC", rf.me)
	rf.mu.Unlock()

	// Send RPC with timeout
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)

	if !ok {
		// RPC failed, but don't retry here to avoid infinite recursion
		// The heartbeat loop will retry automatically
		return false
	}

	Debug(dLog, "S%d sendAppendEntriesToPeer: re-acquiring lock after RPC", rf.me)
	rf.mu.Lock()
	Debug(dLog, "S%d sendAppendEntriesToPeer: re-acquired lock after RPC", rf.me)
	defer func() {
		Debug(dLog, "S%d sendAppendEntriesToPeer: releasing lock at end", rf.me)
		rf.mu.Unlock()
	}()

	// Check if we're still leader and term hasn't changed
	if rf.currentState != Leader || rf.currentTerm != args.Term {
		return false
	}

	// CRITICAL: Check if follower's nextIndex has been updated by InstallSnapshot
	// This prevents stale AppendEntries from corrupting the log after a snapshot
	if rf.nextIndex[server] > args.PrevLogIndex+len(args.Entries)+1 {
		Debug(dLog, "S%d Leader, AppendEntries STALE for server %d: nextIndex=%d > expectedNext=%d, ignoring response",
			rf.me, server, rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
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
		Debug(dLog, "S%d Leader, AppendEntries SUCCESS for server %d: prevLogIndex=%d, entries=%d",
			rf.me, server, prevLogIndex, len(entries))

		// Update nextIndex and matchIndex
		if len(entries) > 0 {
			// Calculate the highest index we sent
			highestSentIndex := prevLogIndex + len(entries)
			oldNextIndex := rf.nextIndex[server]
			oldMatchIndex := rf.matchindex[server]
			rf.nextIndex[server] = highestSentIndex + 1
			rf.matchindex[server] = highestSentIndex
			Debug(dLog, "S%d Leader, updated indices for server %d: nextIndex %d->%d, matchIndex %d->%d",
				rf.me, server, oldNextIndex, rf.nextIndex[server], oldMatchIndex, rf.matchindex[server])
		} else {
			// For heartbeats, update matchIndex to at least prevLogIndex
			oldMatchIndex := rf.matchindex[server]
			oldNextIndex := rf.nextIndex[server]
			if prevLogIndex >= 0 && prevLogIndex > rf.matchindex[server] {
				rf.matchindex[server] = prevLogIndex
			}
			// Also ensure nextIndex is at least prevLogIndex + 1
			if prevLogIndex >= 0 && rf.nextIndex[server] <= prevLogIndex {
				rf.nextIndex[server] = prevLogIndex + 1
			}
			Debug(dLog, "S%d Leader, heartbeat success for server %d: nextIndex %d->%d, matchIndex %d->%d",
				rf.me, server, oldNextIndex, rf.nextIndex[server], oldMatchIndex, rf.matchindex[server])
		}

		// Try to commit new entries
		rf.tryCommitEntries()
		return true
	} else {
		Debug(dLog, "S%d Leader, AppendEntries FAILED for server %d: prevLogIndex=%d, entries=%d, reply.Term=%d",
			rf.me, server, prevLogIndex, len(entries), reply.Term)

		// Handle fast backup
		if reply.XLen > 0 {
			oldNextIndex := rf.nextIndex[server]
			if reply.XTerm == -1 {
				// Follower doesn't have entry at prevLogIndex
				Debug(dLog, "S%d Leader, FAST BACKUP: follower %d has no entry at prevLogIndex, setting nextIndex[%d] = %d (was %d)",
					rf.me, server, server, reply.XLen, rf.nextIndex[server])
				rf.nextIndex[server] = reply.XLen
			} else {
				// Find the first index of XTerm in leader's log
				Debug(dLog, "S%d Leader, FAST BACKUP: follower %d has conflicting term %d at index %d, searching for first occurrence",
					rf.me, server, reply.XTerm, reply.XIndex)
				found := -1
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].Term == reply.XTerm {
						found = i
						break
					}
				}
				if found >= 0 {
					// Convert slice index to global index
					rf.nextIndex[server] = rf.snapshotIndex + found
					Debug(dLog, "S%d Leader, FAST BACKUP: found XTerm %d at slice index %d, setting nextIndex[%d] = %d",
						rf.me, reply.XTerm, found, server, rf.nextIndex[server])
				} else {
					rf.nextIndex[server] = reply.XIndex
					Debug(dLog, "S%d Leader, FAST BACKUP: XTerm %d not found in log, setting nextIndex[%d] = %d",
						rf.me, reply.XTerm, server, rf.nextIndex[server])
				}
			}
			Debug(dLog, "S%d Leader, FAST BACKUP: updated nextIndex[%d] %d->%d", rf.me, server, oldNextIndex, rf.nextIndex[server])
		} else {
			// Simple decrement for unknown conflicts
			oldNextIndex := rf.nextIndex[server]
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
				Debug(dLog, "S%d Leader, FAST BACKUP: simple decrement for server %d: nextIndex %d->%d",
					rf.me, server, oldNextIndex, rf.nextIndex[server])
			}
		}

		// DEBUG: Log the current state after fast backup
		Debug(dLog, "S%d Leader, after fast backup: nextIndex[%d]=%d, matchIndex[%d]=%d, snapshotIndex=%d",
			rf.me, server, rf.nextIndex[server], server, rf.matchindex[server], rf.snapshotIndex)

		return false
	}
}

// tryCommitEntries attempts to commit new entries based on matchIndex
func (rf *Raft) tryCommitEntries() {
	// This function is called from heartbeat loop, so we already hold the lock
	// Don't try to acquire it again to avoid deadlock
	// Calculate the actual last log index (global index)
	actualLastLogIndex := rf.snapshotIndex + len(rf.log) - 1

	for n := rf.commitIndex + 1; n <= actualLastLogIndex; n++ {
		// Convert global index to slice index
		sliceIndex := n - rf.snapshotIndex
		if sliceIndex >= 0 && sliceIndex < len(rf.log) {
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
		// Convert global index to slice index
		sliceIndex := (rf.lastApplied + 1) - rf.snapshotIndex
		if sliceIndex >= 0 && sliceIndex < len(rf.log) {
			entry := rf.log[sliceIndex]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1,
			}
			// Release mutex before blocking send to avoid deadlock
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied++
		} else if (rf.lastApplied + 1) <= rf.snapshotIndex {
			// Entry is already in snapshot, no need to apply it again
			rf.lastApplied++
		} else {
			// Entry should be in log but isn't - this is an error
			// Skip this entry to avoid infinite loop
			rf.lastApplied++
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
	// Add the new entry to the log first
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})

	// Calculate the actual log index (including snapshot offset)
	logIndex := rf.snapshotIndex + len(rf.log) - 1
	rf.persist()

	// Let the leader heartbeat mechanism handle replication
	// Don't spawn additional goroutines here to prevent mutex contention

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
	Debug(dLog, "S%d AppendEntries: attempting to acquire lock", rf.me)
	rf.mu.Lock()
	Debug(dLog, "S%d AppendEntries: acquired lock", rf.me)
	defer func() {
		Debug(dLog, "S%d AppendEntries: releasing lock", rf.me)
		rf.mu.Unlock()
	}()

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
	// Convert global index to slice index
	prevLogSliceIndex := prevLogIndex - rf.snapshotIndex

	// DEBUG: Log the initial state
	Debug(dLog, "S%d Follower, AppendEntries received: term=%d, leaderId=%d, prevLogIndex=%d, prevLogTerm=%d, entries=%d, leaderCommit=%d",
		rf.me, args.Term, args.LeaderId, prevLogIndex, prevLogTerm, len(args.Entries), args.LeaderCommit)
	Debug(dLog, "S%d Follower, current state: term=%d, state=%d, snapshotIndex=%d, logLen=%d, commitIndex=%d, lastApplied=%d",
		rf.me, rf.currentTerm, rf.currentState, rf.snapshotIndex, len(rf.log), rf.commitIndex, rf.lastApplied)

	// CRITICAL: Reject stale AppendEntries that try to append entries before our snapshot
	// This prevents race conditions where old AppendEntries arrive after InstallSnapshot
	if len(args.Entries) > 0 {
		lastEntryIndex := prevLogIndex + len(args.Entries)
		if lastEntryIndex <= rf.snapshotIndex {
			Debug(dLog, "S%d Follower, REJECTING stale AppendEntries: lastEntryIndex=%d <= snapshotIndex=%d",
				rf.me, lastEntryIndex, rf.snapshotIndex)
			reply.Success = false
			reply.XLen = rf.snapshotIndex + len(rf.log)
			return
		}
	}

	// CRITICAL: Reject AppendEntries where prevLogIndex is within our snapshot
	// After InstallSnapshot, we should only accept AppendEntries with prevLogIndex >= snapshotIndex
	if prevLogIndex < rf.snapshotIndex {
		Debug(dLog, "S%d Follower, REJECTING AppendEntries with prevLogIndex in snapshot: prevLogIndex=%d < snapshotIndex=%d",
			rf.me, prevLogIndex, rf.snapshotIndex)
		reply.Success = false
		reply.XLen = rf.snapshotIndex + len(rf.log)
		return
	}

	// Note: prevLogIndex < rf.snapshotIndex case is now handled by rejection above

	// DEBUG: Log the log consistency check details
	Debug(dLog, "S%d Follower, checking log consistency: prevLogSliceIndex=%d, logLen=%d, expectedTerm=%d",
		rf.me, prevLogSliceIndex, len(rf.log), prevLogTerm)

	if prevLogSliceIndex >= 0 && prevLogSliceIndex < len(rf.log) {
		Debug(dLog, "S%d Follower, log entry at slice index %d: term=%d, command=%d",
			rf.me, prevLogSliceIndex, rf.log[prevLogSliceIndex].Term, rf.log[prevLogSliceIndex].Command)
	}

	// Special case: if prevLogIndex == snapshotIndex, this is valid (snapshot boundary)
	// We don't need to check the log entry since it's in the snapshot
	if prevLogIndex == rf.snapshotIndex {
		// Valid - the entry at snapshotIndex is valid (it's the snapshot boundary)
		// Continue to process the entries normally
	} else if prevLogSliceIndex >= len(rf.log) ||
		(prevLogSliceIndex >= 0 && prevLogSliceIndex < len(rf.log) && rf.log[prevLogSliceIndex].Term != prevLogTerm) {
		term := 0
		if prevLogSliceIndex >= 0 && prevLogSliceIndex < len(rf.log) {
			term = rf.log[prevLogSliceIndex].Term
		}
		Debug(dDrop, "S%d Follower, LOG CONSISTENCY FAILED: global index %d (slice index %d), loglen %d term=%d does not match leader's term=%d",
			rf.me, prevLogIndex, prevLogSliceIndex, len(rf.log), term, prevLogTerm)
		reply.Success = false
		// Fast Backup
		if prevLogSliceIndex >= 0 && prevLogSliceIndex < len(rf.log) && rf.log[prevLogSliceIndex].Term != 0 {
			// Follower has a conflicting Xterm at PrevLogIndex
			reply.XTerm = rf.log[prevLogSliceIndex].Term

			// Find the first index of XTerm in the follower's log
			index := prevLogSliceIndex
			for index > 0 && rf.log[index-1].Term == reply.XTerm {
				index--
			}
			reply.XIndex = index + rf.snapshotIndex // Convert back to global index
			// one past last index available at follower
			reply.XLen = rf.snapshotIndex + len(rf.log)
			return
		} else {
			// Follower has no entry at PrevLogIndex
			reply.XTerm = -1
			reply.XIndex = -1
			// one past last index available at follower
			reply.XLen = rf.snapshotIndex + len(rf.log)
		}
		return
	}

	// [3] Perform consistency check
	mismatch := false
	mismatchIndex := 0
	for i, argsEntry := range args.Entries {
		index := prevLogIndex + 1 + i
		// Convert global index to slice index for checking
		logIndex := index - rf.snapshotIndex
		// First check if the index exists in the follower's log
		if logIndex >= 0 && logIndex < len(rf.log) && rf.log[logIndex].Term != argsEntry.Term {
			mismatch = true
			mismatchIndex = logIndex
			break
		}
	}

	if mismatch {
		// Truncate log from mismatchIndex onwards (mismatchIndex is now a slice index)
		if mismatchIndex < len(rf.log) {
			rf.log = rf.log[:mismatchIndex]
			Debug(dDrop, "S%d Follower, truncating log at slice index %d", rf.me, mismatchIndex)
			rf.persist()
		}
	}

	// [4] Append any new entries not already in the log
	for i, argsEntry := range args.Entries {
		globalIndex := prevLogIndex + 1 + i
		// Convert global index to slice index
		sliceIndex := globalIndex - rf.snapshotIndex

		// Check if we already have this entry and it matches
		if sliceIndex < len(rf.log) && rf.log[sliceIndex].Term == argsEntry.Term {
			// Entry already exists and matches, skip it
			Debug(dLog, "S%d Follower, skipping existing entry at GlobalIndex=%d SliceIndex=%d Term=%d Cmd=%d",
				rf.me, globalIndex, sliceIndex, argsEntry.Term, argsEntry.Command)
			continue
		}

		// Ensure log is large enough
		for len(rf.log) <= sliceIndex {
			rf.log = append(rf.log, LogEntry{})
		}
		// Overwrite existing entry or add new one
		rf.log[sliceIndex] = argsEntry
		Debug(dLog, "S%d Follower, Logging new entry! "+
			"GlobalIndex=%d SliceIndex=%d Term=%d Cmd=%d logSize=%d",
			rf.me, globalIndex, sliceIndex,
			argsEntry.Term, argsEntry.Command, len(rf.log))
	}
	// Persist once after all entries are added
	rf.persist()

	// [5] If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last entry in log)
	if leadersCommit > rf.commitIndex {
		lastLogIndex := rf.snapshotIndex + len(rf.log) - 1
		rf.commitIndex = min(leadersCommit, lastLogIndex)
	}

	// If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine
	for rf.commitIndex > rf.lastApplied {
		// Convert global index to slice index
		sliceIndex := (rf.lastApplied + 1) - rf.snapshotIndex
		if sliceIndex >= 0 && sliceIndex < len(rf.log) {
			entry := rf.log[sliceIndex]
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1,
			}
			// Release mutex before blocking send to avoid deadlock
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied++
			Debug(dCommit, "S%d Follower, Committing new entry on term%d, commitIndex:%d, lastApplied:%d!"+
				" GlobalIndex: %d SliceIndex: %d Term=%d Cmd=%d prevLogIndex=%d prevLogTerm=%d ",
				rf.me, args.Term, rf.commitIndex, rf.lastApplied,
				rf.lastApplied, sliceIndex, entry.Term,
				entry.Command, prevLogIndex, prevLogTerm)
			rf.persist()
		} else if (rf.lastApplied + 1) <= rf.snapshotIndex {
			// Entry is already in snapshot, no need to apply it again
			rf.lastApplied++
		} else {
			// Entry should be in log but isn't - this is an error
			// Skip this entry to avoid infinite loop
			rf.lastApplied++
		}
	}

	reply.Success = true
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	Debug(dLog, "S%d InstallSnapshot: attempting to acquire lock", rf.me)
	rf.mu.Lock()
	Debug(dLog, "S%d InstallSnapshot: acquired lock", rf.me)
	defer func() {
		Debug(dLog, "S%d InstallSnapshot: releasing lock", rf.me)
		rf.mu.Unlock()
	}()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.currentState = Follower
	}

	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Update term and become follower if needed
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = 0
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// If existing log entry has same index and term as snapshot's
	// last included entry, retain log entries following it and reply
	if args.LastIncludedIndex <= rf.snapshotIndex+len(rf.log)-1 {
		// Check if the entry at LastIncludedIndex matches
		existingIndex := args.LastIncludedIndex - rf.snapshotIndex
		if existingIndex >= 0 && existingIndex < len(rf.log) &&
			rf.log[existingIndex].Term == args.LastIncludedTerm {
			// Retain log entries following LastIncludedIndex
			rf.log = rf.log[existingIndex+1:]
			rf.snapshotIndex = args.LastIncludedIndex
			rf.snapshotTerm = args.LastIncludedTerm
			rf.snapshot = args.Data
			rf.persist()
			Debug(dLog, "S%d InstallSnapshot: retained log entries after index %d", rf.me, args.LastIncludedIndex)
			return
		}
	}

	// Discard the entire log
	rf.log = []LogEntry{}
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	// Update commitIndex and lastApplied to the snapshot index
	// This is crucial for maintaining consistency after applying the snapshot
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persist()

	// Reset state machine using snapshot contents
	// Apply the snapshot to the state machine
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// Release mutex before blocking send to avoid deadlock
	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.mu.Lock()
	Debug(dLog, "S%d InstallSnapshot: applied snapshot at index %d", rf.me, args.LastIncludedIndex)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Helper function to send InstallSnapshot RPC when follower is behind snapshot
func (rf *Raft) sendInstallSnapshotToServer(server int) bool {
	// Need to acquire lock to read snapshot data
	rf.mu.Lock()
	if rf.currentState != Leader {
		rf.mu.Unlock()
		return false
	}

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          int(rf.leaderId),
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              make([]byte, len(rf.snapshot)),
	}
	copy(args.Data, rf.snapshot)
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	Debug(dSnap, "S%d Leader, sending InstallSnapshot to S%d: lastIncludedIndex=%d, lastIncludedTerm=%d",
		rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm)

	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		Debug(dSnap, "S%d Leader, InstallSnapshot RPC to S%d failed", rf.me, server)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Process the reply
	if reply.Term > rf.currentTerm {
		// Follower has higher term, convert to follower
		Debug(dSnap, "S%d Leader, InstallSnapshot reply from S%d has higher term %d > %d, converting to follower",
			rf.me, server, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.votedFor = 0
		rf.currentState = Follower
		rf.persist()
		return false
	}

	if reply.Term == rf.currentTerm && rf.currentState == Leader {
		// Success - update follower's nextIndex and matchIndex only if not already advanced
		// This prevents overwriting more recent updates from concurrent AppendEntries
		if rf.nextIndex[server] <= args.LastIncludedIndex+1 {
			Debug(dSnap, "S%d Leader, InstallSnapshot to S%d successful, updating indices: nextIndex=%d->%d, matchIndex=%d->%d",
				rf.me, server, rf.nextIndex[server], args.LastIncludedIndex+1, rf.matchindex[server], args.LastIncludedIndex)
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.matchindex[server] = args.LastIncludedIndex
		} else {
			Debug(dSnap, "S%d Leader, InstallSnapshot to S%d successful but nextIndex already advanced: current=%d, snapshot+1=%d",
				rf.me, server, rf.nextIndex[server], args.LastIncludedIndex+1)
		}
		return true
	}

	return false
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
		// nextIndex should be a global index
		rf.nextIndex[i] = rf.snapshotIndex + len(rf.log)
		rf.matchindex[i] = 0
		Debug(dLeader, "S%d Leader, initialized nextIndex[%d]=%d", rf.me, i, rf.nextIndex[i])
	}

	// Heartbeat loop
	ticker := time.NewTicker(HeartbeatTimeout)
	defer ticker.Stop()

	for !rf.killed() && rf.isLeader() {
		Debug(dLeader, "S%d Leader, sending heartbeats", rf.me)
		// Send heartbeats to all peers serially to reduce mutex contention
		for i := range rf.peers {
			if i == rf.me { // Skip self
				continue
			}
			if rf.killed() || !rf.isLeader() {
				break // Exit early if no longer leader
			}
			Debug(dLeader, "S%d Leader, sending heartbeat to S%d", rf.me, i)
			go rf.sendAppendEntriesToPeer(i, true)
		}

		// Try to commit entries after sending heartbeats
		func() {
			defer func() {
				if r := recover(); r != nil {
					Debug(dError, "S%d Leader, recovered from panic in tryCommitEntries: %v", rf.me, r)
				}
			}()
			if rf.isLeader() {
				rf.mu.Lock()
				if rf.currentState == Leader { // Double-check while holding lock
					rf.tryCommitEntries()
				}
				rf.mu.Unlock()
			}
		}()

		<-ticker.C // Wait for next heartbeat interval
	}

	Debug(dLeader, "S%d Leader, stopping heartbeat", rf.me)
}

// election timeout for all rafts
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		Debug(dVote, "S%d electionTicker: starting iteration", rf.me)
		var startElection = false
		var majorityVote = false

		// Election Timeout - pause for a random amount of time.
		var election_timeout = make(chan bool, 1)
		go func() {
			ms := electionTimeout + (rand.Int63() % 250)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			election_timeout <- true
		}()

		// Continue for leader, leader breaks only when receiving a
		// RPC with higher term
		_, isLeader := rf.GetState()
		if isLeader {
			<-election_timeout
			continue
		}

		// Wait for election timeout before checking if we should start an election
		<-election_timeout

		Debug(dVote, "S%d electionTicker: attempting to acquire lock", rf.me)
		rf.mu.Lock()
		Debug(dVote, "S%d electionTicker: acquired lock", rf.me)
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
			// Calculate the actual last log index
			if len(rf.log) > 0 {
				lastLoggedTerm = rf.log[len(rf.log)-1].Term
			} else if rf.snapshotIndex > 0 {
				// If log is empty but we have a snapshot, use snapshot term
				lastLoggedTerm = rf.snapshotTerm
			}
			args := RequestVoteArgs{candidateId, rf.currentTerm,
				lastLoggedTerm, rf.snapshotIndex + len(rf.log)}

			// Send requestVote RPC to all known rafts in parallel
			vote_results := make(chan bool, len(rf.peers)-1)
			for i := range rf.peers {
				// Skip self
				if i == rf.me {
					continue
				}
				// Launch the requestVote RPCs in parallel
				go func(peerIndex int) {
					Debug(dVote, "S%d electionTicker: spawning RequestVote RPC to S%d", rf.me, peerIndex)
					reply := RequestVoteReply{}
					rf.sendRequestVote(peerIndex, &args, &reply)
					vote_results <- reply.VoteGranted
				}(i)
			}
			Debug(dVote, "S%d electionTicker: releasing lock before waiting for votes", rf.me)
			rf.mu.Unlock()

			// Wait for votes
			for i := 0; i < len(rf.peers)-1; i++ {
				vote := <-vote_results
				if vote {
					Debug(dInfo, "S%d Candidate, got a vote.", rf.me)
					votes++
				}

				// Go ahead and break out if we have a quorum
				if votes > (len(rf.peers) / 2) {
					break
				}
				// While waiting, if we became a follower break out.
				if rf.currentState != Candidate {
					break
				}
			}

			Debug(dVote, "S%d electionTicker: re-acquiring lock to check votes", rf.me)
			rf.mu.Lock()
			Debug(dVote, "S%d electionTicker: re-acquired lock to check votes", rf.me)
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
					nextIndex := len(rf.log)
					Debug(dVote, "S%d initialized nextIndex to %d", rf.me, nextIndex)
					rf.nextIndex[i] = nextIndex
				}

				// Try to commit entries that might already be replicated
				rf.tryCommitEntries()

				// Start the leader routine
				go rf.startLeaderHeartbeat()
			} else {
				// Lost the election
				Debug(dVote, "S%d Candidate, lost the election!", rf.me)
			}
		}
		rf.mu.Unlock()
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

	// Volatile state
	rf.currentState = Follower
	rf.heartBeatReceived = false
	rf.leaderId = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make(map[int]int)

	// Initialize snapshot functionality
	rf.snapshotEnabled = true

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// Add dummy entry at index 0 only if log is empty
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})
	}

	// Initialize snapshot metadata if not already set
	if rf.snapshotIndex == 0 && len(rf.snapshot) > 0 {
		// Try to decode snapshot to get the index
		r := bytes.NewBuffer(rf.snapshot)
		d := labgob.NewDecoder(r)
		var lastIncludedIndex int
		var xlog []interface{}
		if d.Decode(&lastIncludedIndex) == nil && d.Decode(&xlog) == nil {
			rf.snapshotIndex = lastIncludedIndex
			rf.snapshotTerm = 0 // Default term for snapshot
		}
	}

	// Initialize nextIndex after restoring persistent state
	for i := range rf.peers {
		rf.nextIndex[i] = rf.snapshotIndex + len(rf.log)
	}

	rf.matchindex = make(map[int]int)

	// start electionTicker goroutine to start elections
	go rf.electionTicker()

	// Log that we started (or restarted)
	Debug(dInfo, "S%d, restarted", rf.me)

	return rf
}
