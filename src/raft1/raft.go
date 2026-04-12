package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"log"

	//	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/debug"
	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command any
}

type PersistedState struct {
	CurrentTerm   int
	VotedFor      int
	Log           []*LogEntry
	SnapshotIndex int
}

type State int32

const (
	follower State = iota
	candidate
	leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state
	CurrentTerm       int
	VotedFor          int
	Log               []*LogEntry
	SnapshotData      []byte
	LastIncludedIndex int

	//volatile state on all servers
	commitIndex int
	lastApplied int
	state       State

	//volatile on leaders
	nextIndex  []int
	matchIndex []int

	applyCh chan raftapi.ApplyMsg

	//utils
	lastHeartbeatTime time.Time //last time when it heard an RPC
	logAppendedCh     chan struct{}
	applyReadyCh      chan struct{}
	replicateReadyChs []chan struct{}
	snapshotPending   bool
	kill              chan struct{}
}

// printState print the Persisted State of this Raft
func (rf *Raft) printState() {
	log.Printf("CurrentTerm: %d\n "+
		"VotedFor: %d\n "+
		"Log: %v\n "+
		"LastIncludedIndex:%d",
		rf.CurrentTerm, rf.VotedFor, rf.Log, rf.LastIncludedIndex)
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isLeader = rf.state == leader
	return term, isLeader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	persistedState := PersistedState{
		CurrentTerm:   rf.CurrentTerm,
		VotedFor:      rf.VotedFor,
		Log:           rf.Log,
		SnapshotIndex: rf.LastIncludedIndex,
	}
	err := e.Encode(persistedState)
	if err != nil {
		log.Fatal(err)
		return
	}
	rf.persister.Save(w.Bytes(), rf.SnapshotData)
}

// restore previously persisted state.
func (rf *Raft) readPersist(raftStateData []byte, snapshotData []byte) {
	//D3DPrintf("%v: readPersist called", rf.me)
	if raftStateData == nil || len(raftStateData) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(raftStateData)
	d := labgob.NewDecoder(r)
	var raftState PersistedState
	err := d.Decode(&raftState)
	if err != nil {
		log.Fatal(err)
		return
	}
	rf.CurrentTerm = raftState.CurrentTerm
	rf.VotedFor = raftState.VotedFor
	rf.Log = raftState.Log
	rf.LastIncludedIndex = raftState.SnapshotIndex
	rf.lastApplied = rf.LastIncludedIndex
	rf.commitIndex = rf.LastIncludedIndex
	rf.SnapshotData = snapshotData
}

// how many bytes in Raft's persisted Log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastIncludedIndex {
		// if snapshot has smaller index, meaning it is an older state, ignore it.
		return
	}
	debug.D3DPrintf("%v: snapshot at %v, Is leader ? %v ", rf.me, index, rf.state == leader)

	// truncate the log, after index (lastIncludedIndex)
	truncatedLog := append([]*LogEntry{}, rf.Log[rf.physicalIndex(index):]...)
	rf.Log = truncatedLog
	// update LastIncludedIndex,snapshotData
	rf.LastIncludedIndex = index
	rf.SnapshotData = snapshot
	// persist
	rf.persist()
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.Log) - 1 + rf.LastIncludedIndex
}

func (rf *Raft) lastLogTerm() int {
	return rf.Log[len(rf.Log)-1].Term
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int // -1 if the log[LastLogIndex] doesn't exist, otherwise log[LastLogIndex].Term
	ConflictIndex int // index of the first entry in the conflictTerm
}

func (rf *Raft) becomeFollowerWithTerm(term int) {
	rf.state = follower
	rf.CurrentTerm = term
	rf.VotedFor = -1
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//update the lastHeartbeat Time
	if args.Term >= rf.CurrentTerm {
		rf.lastHeartbeatTime = time.Now()
	}

	//higher AppEn, rf -> follower
	//equal AppEn,
	//	case candidate: ->follower,
	//	case leader: ignore
	//	case follower: don't change anything
	//lower Appen, reply false

	//converted to follower if getting higher AppendEntries
	if rf.CurrentTerm < args.Term {
		rf.becomeFollowerWithTerm(args.Term)
		debug.D3APrintf("%v at %v resets VotedFor upon receiving AppendEntries", rf.me, rf.CurrentTerm)
	} else if rf.CurrentTerm == args.Term { //if getting equal term AppendEntries,
		if rf.state == candidate { // and is in election, step down to be a follower
			rf.CurrentTerm = args.Term
			rf.state = follower
			debug.D3APrintf("%v at %v candidate -> %v follower: for receiving equal term AppendEntries", rf.me, rf.CurrentTerm, rf.CurrentTerm)
		}
	} else { // case when CurrentTerm is greater than rpc's term
		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.persist()
		return
	}
	// at this point, this raft would be a follower
	// case leader: doesn't care AppenEntries from out-of-term leaders, reply with success=false
	// start framing AppenEntries reply
	var success bool
	var conflictTerm, conflictIndex int

	if args.PrevLogIndex < rf.LastIncludedIndex {
		// if follower has removed the prevLogIndex due to snapshot, reply false
		success = false
		conflictIndex = rf.LastIncludedIndex + 1
		conflictTerm = -1
	} else if args.PrevLogIndex > rf.lastLogIndex() {
		// if follower's log is too short to check the prevLogIndex, reply false
		success = false
		conflictTerm = -1
		conflictIndex = rf.lastLogIndex() + 1
	} else {
		// prevLogIndex exists in the follower's log, so it can check and reply accordingly
		if args.PrevLogTerm != rf.Log[rf.physicalIndex(args.PrevLogIndex)].Term {
			// if the follower has an entry at PrevLogIndex, but not equal to that of the leader, reply false
			// the conflic in term indicates that somewhere this follower has diverged from the majority
			// therefore it needs to tell the leader where this term starts so that the current leader can find out where they diverge
			success = false
			conflictTerm = rf.Log[rf.physicalIndex(args.PrevLogIndex)].Term

			// now need to find the first occurrence of the log at the conflicTerm:
			// trace back until the term has changed
			i := args.PrevLogIndex
			for i > rf.LastIncludedIndex && rf.Log[rf.physicalIndex(i)].Term == conflictTerm {
				i--
			}
			conflictIndex = i + 1
		} else {
			// if the log in follower matches that the leader, reply true
			success = true
		}
	}

	*reply = AppendEntriesReply{
		Term:          rf.CurrentTerm,
		Success:       success,
		ConflictTerm:  conflictTerm,
		ConflictIndex: conflictIndex,
	}

	// this follower should not do anything to its Log if success is false,
	if success == false {
		rf.persist()
		return
	}
	// get here only when this raft is a follower, and success is true,
	// the follower needs to compare its Log and entries from the leader,
	// and to find the conflicting point, truncating all following entries,
	// then append unvisited remaining entries in the entries from the leader
	if len(args.Entries) > 0 { //condition guard to make sure that when AppendEntries from the leader is empty, the follower doesn't truncate its log
		i := 0
		for ; i < len(args.Entries); i++ {
			j := args.PrevLogIndex + 1 + i
			if j > rf.lastLogIndex() {
				break
			}
			if args.Entries[i].Term != rf.Log[rf.physicalIndex(j)].Term {
				break
			}
		}
		rf.Log = rf.Log[:rf.physicalIndex(args.PrevLogIndex+1+i)] //truncate the conflicting entry and all following ones
		for j := i; j < len(args.Entries); j++ {
			debug.D3BPrintf("%v appended log from %v at %v:\tcommand:%v\n", rf.me, args.LeaderId, args.PrevLogIndex+1+i+j, args.Entries[j])
		}
		rf.Log = append(rf.Log, args.Entries[i:]...) // append unvisited remaining logs from the leader entries to this raft's Log
	}
	rf.persist()

	// now check the leader commit,
	// if leader has more commits than this follower, move ahead as much as it can
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}
	//trigger applier
	select {
	case rf.applyReadyCh <- struct{}{}:
	default:
	}
}

// sendAppendEntries RPC sender
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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

// sendInstallSnapshot RPC sender
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	// return immediately if follower has higher term
	if rf.CurrentTerm > args.Term {
		return
	}

	// update the lastHeartbeat time
	rf.lastHeartbeatTime = time.Now()

	termChanged := false
	// update raft state before preceding, common for all RPCs
	if rf.CurrentTerm < args.Term {
		rf.becomeFollowerWithTerm(args.Term)
		reply.Term = rf.CurrentTerm
		termChanged = true
	}

	// save snapshot file, discard any existing snapshot with a smaller index
	if rf.LastIncludedIndex < args.LastIncludedIndex { // case where leader's snapshot is more advanced.
		// save snapshot to memory
		// (but not yet to decide whether to apply to state machine
		// because state machine could be more advanced from apply logs.)
		rf.SnapshotData = args.Data
		if rf.lastLogIndex() >= args.LastIncludedIndex && rf.Log[rf.physicalIndex(args.LastIncludedIndex)].Term == args.LastIncludedTerm { // case where log is at least up to date (twofold: whether log has LastIncluded, and whether they match) with the received snapshot
			// remove logs that have been included in the snapshot (but retaining the LastIncluded as the dummy entry)
			rf.Log = append([]*LogEntry{}, rf.Log[rf.physicalIndex(args.LastIncludedIndex):]...)
			rf.LastIncludedIndex = args.LastIncludedIndex
		} else { // case where this follower's log is behind or this follower has the log at LastIncludedIndex, but doesn't match, meaning its logs are all obsolete
			// remove all the local logs, creating a dummy log at LastIncluded
			rf.Log = append([]*LogEntry{}, &LogEntry{
				Term:    args.LastIncludedTerm,
				Command: nil,
			})
			rf.LastIncludedIndex = args.LastIncludedIndex
		}

		// now to check whether it need to apply snapshot to state machine
		if rf.lastApplied < args.LastIncludedIndex { // if state machine is behind received snapshot, will need to apply snapshot
			rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
			rf.snapshotPending = true
			// tell applier that it has new snapshot to apply
			select {
			case rf.applyReadyCh <- struct{}{}:
			default:
			}
		} // else: the state machine is more advanced than this received snapshot, don't apply
	} else if termChanged { // case when the term has changed but the snapshot in InstallSnapshot is behind the follower, need to persist the term change
		rf.persist()
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
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

// get real log index from the logical Index(exceeding boundary can still happen in this operation)
func (rf *Raft) physicalIndex(i int) int {
	return i - rf.LastIncludedIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm < args.Term {
		if rf.state == candidate {
			debug.D3APrintf("%v candidate at %v -> follower for higher term RequestVote", rf.me, rf.CurrentTerm)
		}
		rf.becomeFollowerWithTerm(args.Term)
		debug.D3APrintf("%v at %v resets VotedFor upon receiving RequestVote", rf.me, rf.CurrentTerm)
	}

	if args.Term >= rf.CurrentTerm && (rf.VotedFor == args.CandidateId || rf.VotedFor == -1) { // if receiver didn't vote for itself (it's not a candidate)
		if args.LastLogTerm > rf.lastLogTerm() || // and if the candidate term is greater than the receiver,
			(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) { //or of the same term but candidate has number of entries >= to receiver
			rf.lastHeartbeatTime = time.Now() // update lastHeartbeatTime when this raft decides to vote for the candidate, so that it won't trigger another round of election too quickly
			debug.D3APrintf("%v at %v, VotedFor%v -vote-> %v at %v", rf.me, rf.CurrentTerm, rf.VotedFor, args.CandidateId, args.Term)
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.state = follower
		}
	} else {
		debug.D3APrintf("%v at %v,VotedFor %v -NO vote-> %v at %v", rf.me, rf.CurrentTerm, rf.VotedFor, args.CandidateId, args.Term)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
	rf.persist()
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (3B).
	//log.Printf("Start() tries to acquire log")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		rf.Log = append(rf.Log, &LogEntry{Term: rf.CurrentTerm, Command: command})
		rf.persist()
		index = rf.lastLogIndex()
		term = rf.CurrentTerm
		isLeader = true
		debug.D3BPrintf("%v Start(%v), CommandIndex: %v", rf.me, command, index)
		// signal replicationDispatcher, or no when there exists already one pending request
		select {
		case rf.logAppendedCh <- struct{}{}:
		default:
		}
	}
	//log.Printf("Start() done")
	return index, term, isLeader
}

func (rf *Raft) replicationDispatcher() {
	//this part spawn long-running goroutines for each follower to listen for log replication requests
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// if the follower is ready to replicate log, trigger synchroniseFollower
		go func(i int) {
			for {
				<-rf.replicateReadyChs[i]
				rf.synchroniseFollower(i)
			}
		}(i)
	}

	//this part recurringly listens for logAppendedCh and notify each replicateReadyCh to execute synchroniseFollower
	for {
		<-rf.logAppendedCh
		//D3BPrintf("%v: replicationDispatcher request accepted", rf.me)
		rf.mu.Lock()
		//check state
		if rf.state != leader {
			rf.mu.Unlock()
			continue
		}
		// at there, this rf should be the leader
		//notify goroutines that execute replicateToFollower
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			//wake up goroutine only when the follower is out of date
			if rf.nextIndex[i] <= rf.lastLogIndex() {
				select {
				case rf.replicateReadyChs[i] <- struct{}{}:
				default:
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) newAppendEntriesArgsFor(svrIndex int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[svrIndex] - 1,
		PrevLogTerm:  rf.Log[rf.physicalIndex(rf.nextIndex[svrIndex]-1)].Term,
		Entries:      rf.Log[rf.physicalIndex(rf.nextIndex[svrIndex]):],
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) newInstallSnapshotArgs() *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.Log[rf.physicalIndex(rf.LastIncludedIndex)].Term,
		Data:              rf.SnapshotData,
	}
	return args
}

// advanceCommitIndex is for a leader to check whether commit can be advanced and to notify applyReadyCh if it is the case
func (rf *Raft) advanceCommitIndex() {
	//todo: may need to be improved for this loop O(n*n) holds lock for too long
	for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
		if rf.Log[rf.physicalIndex(N)].Term != rf.CurrentTerm { // this is to ensure never commit on terms other than its own
			continue
		}
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.matchIndex[j] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			debug.D3BPrintf("%v Advancing commit index to %d", rf.me, rf.commitIndex)
			//trigger applier
			select {
			case rf.applyReadyCh <- struct{}{}:
			default:
			}
			break
		}
	}
}

// synchroniseFollower synchronises the log of the follower, meaning it resend appendEntries until follower is in sync
func (rf *Raft) synchroniseFollower(i int) {
	rf.mu.Lock()
	needReplicate := rf.nextIndex[i] <= rf.lastLogIndex()
	rf.mu.Unlock()
	for needReplicate {
		rf.replicateToFollower(i)
		rf.mu.Lock()
		needReplicate = rf.nextIndex[i] <= rf.lastLogIndex()
		rf.mu.Unlock()
	}
}

// replicateToFollower will try replicate Log or send Snapshot to follower i,
func (rf *Raft) replicateToFollower(i int) {
	rf.mu.Lock()
	if rf.state != leader { // if this rf is no longer a leader, stop the whole process
		rf.mu.Unlock()
		return
	}
	// case where the leader doesn't have the log to send to the follower, send InstallSnapshot instead
	if rf.LastIncludedIndex >= rf.nextIndex[i] {
		debug.D3DPrintf("%v-InstallSnapshot,lastIncluded:%v->%v", rf.me, rf.LastIncludedIndex, i)
		args := rf.newInstallSnapshotArgs()
		rf.mu.Unlock()
		reply := &InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(i, args, reply)
		if !ok {
			return
		}
		rf.mu.Lock()
		if reply.Term > rf.CurrentTerm {
			rf.becomeFollowerWithTerm(reply.Term)
			rf.persist()
			rf.mu.Unlock()
			return
		}
		// case where the snapshot is accepted by the follower,
		// if the reply is not stale, update nextIndex, matchIndex
		if args.LastIncludedIndex > rf.matchIndex[i] {
			rf.matchIndex[i] = args.LastIncludedIndex
			rf.nextIndex[i] = args.LastIncludedIndex + 1
		}
		rf.mu.Unlock()
		return // return false because usually appendEntries is needed after installing snapshot, as the snapshot point may be behind the leader's latest log
	}

	// case where the leader has the log to send to the follower, send AppendEntries
	args := rf.newAppendEntriesArgsFor(i)
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, args, reply)
	if !ok {
		return
	}
	//D3BPrintf("%v-AppendEntries->%v, with LeaderCommit %v,PrevLogIndex %v,PrevLogTerm %v", rf.me, i, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm)
	// check the term and leadership
	// if this raft knows that it's not the leader anymore, stop
	rf.mu.Lock()
	if reply.Term > rf.CurrentTerm {
		rf.becomeFollowerWithTerm(reply.Term)
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// if this raft is still the leader
	// and the AppendEntries is successful
	if reply.Success {
		// check whether the rpc reply is delayed
		// if the reply of the rpc is out of date, meaning that the current matchIndex is greater, don't move backwards
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if rf.matchIndex[i] >= newMatchIndex {
			//D3BPrintf("%v-AppendEntries->%v till %v, without updating matchIndex", rf.me, i, rf.matchIndex[i])
			rf.persist()
			rf.mu.Unlock()
			return
		}
		rf.matchIndex[i] = newMatchIndex
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		debug.D3BPrintf("%v-updated AppendEntries->%v till %v", rf.me, i, rf.matchIndex[i])

		//if there exists an N such that N > commitIndex,
		//a majority of matchIndex[i]>=N,
		//and Log[N].term == CurrentTerm
		//set commitIndex = N

		rf.advanceCommitIndex()
		rf.persist()
		rf.mu.Unlock()
		return
	}
	// case where the AppendEntries isn't successful, adjust nextIndex and retry
	nextIndex := reply.ConflictIndex
	if reply.ConflictTerm != -1 { //case where follower Log has an entry at PrevLogIndex
		//search backwards until conflictIndex to see if the term exists
		for j := rf.lastLogIndex(); j >= rf.LastIncludedIndex && j >= reply.ConflictIndex; j-- {
			if rf.Log[rf.physicalIndex(j)].Term == reply.ConflictTerm { // term exists
				nextIndex = j + 1
				break
			}
		}
	} // else: nextIndex should be conflictIndex,
	// but it has already been initialised as such,
	// therefore leave out this condition
	rf.nextIndex[i] = nextIndex
	rf.mu.Unlock()
	return
}

// long-running routine. upon receiving of applyReadyCh, send apply committed log message to client until commitIndex
func (rf *Raft) applier() {
	defer close(rf.applyCh)
	for {
		<-rf.applyReadyCh
		//D3BPrintf("%v: applier triggered", rf.me)
		for {
			rf.mu.Lock()
			if rf.snapshotPending || rf.lastApplied < rf.commitIndex {
				if rf.snapshotPending { // need to apply snapshot
					snapshotMsg := raftapi.ApplyMsg{
						SnapshotValid: true,
						Snapshot:      rf.SnapshotData,
						SnapshotTerm:  rf.Log[rf.physicalIndex(rf.LastIncludedIndex)].Term,
						SnapshotIndex: rf.LastIncludedIndex,
					}
					rf.snapshotPending = false
					rf.mu.Unlock()

					// apply snapshot to state machine
					rf.applyCh <- snapshotMsg
					//log.Printf("%v apply snapshot: Index:%3d\n", rf.me, snapshotMsg.SnapshotIndex)
					debug.D3DPrintf("%v apply snapshot: Index:%3d\n", rf.me, snapshotMsg.SnapshotIndex)

					// update lastApplied
					rf.mu.Lock()
					rf.lastApplied = snapshotMsg.SnapshotIndex
					rf.mu.Unlock()
				} else if rf.lastApplied < rf.lastLogIndex() { // need to apply log, added guard to prevent from accessing out of bound log
					logMsg := raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.Log[rf.physicalIndex(rf.lastApplied+1)].Command,
						CommandIndex: rf.lastApplied + 1,
					}
					rf.mu.Unlock()

					// apply log entry to state machine
					rf.applyCh <- logMsg
					debug.D3DPrintf("%v apply log: command:%20d,  commandIndex:%3v\n", rf.me, logMsg.Command, logMsg.CommandIndex)

					// update lastApplied
					rf.mu.Lock()
					rf.lastApplied++
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
				break
			}
		}
	}
}

func (rf *Raft) ticker() {
	for {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 300 and 600
		// milliseconds.
		ms := 300 + (rand.Int63() % 300)
		rf.mu.Lock()
		needElection := rf.state != leader && time.Since(rf.lastHeartbeatTime) > time.Duration(ms)*time.Millisecond
		rf.mu.Unlock()
		//D3APrintf("%v ticker: random time is %v ms, gap from last heartbeat is %v", rf.me, ms, time.Since(rf.lastHeartbeatTime))
		if needElection {
			//D3APrintf("%v ticker timeout", rf.me)
			go rf.startElection()
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.state = candidate
	rf.VotedFor = rf.me //vote for itself
	rf.persist()
	debug.D3APrintf("%v at %v: startElection", rf.me, rf.CurrentTerm)

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	rf.mu.Unlock()

	voteCount := 1

	//send RequestVotes to all other servers
	for i := range rf.peers {
		if i == rf.me { //rf.me doesn't change, lock isn't needed
			continue
		}
		//request vote to i
		go func(i int) {
			reply := &RequestVoteReply{}
			debug.D3APrintf("%v at %v: is to request vote from %v", rf.me, args.Term, i)
			ok := rf.sendRequestVote(i, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.CurrentTerm {
				rf.becomeFollowerWithTerm(rf.CurrentTerm)
				rf.persist()
				return
			}
			if reply.VoteGranted {
				if rf.state != candidate || reply.Term != rf.CurrentTerm {
					debug.D3APrintf("%v at %v <-stale vote- %v at %v", rf.me, args.Term, i, reply.Term)
					return
				}
				debug.D3APrintf("%v at %v <-vote- %v", rf.me, args.Term, i)
				voteCount++
				if voteCount > len(rf.peers)/2 {
					debug.D3APrintf("%v at %v wins ", rf.me, args.Term)
					rf.state = leader
					//reinitialise volatile state on leaders
					for j := range rf.nextIndex {
						rf.nextIndex[j] = rf.lastLogIndex() + 1
					}
					for j := range rf.matchIndex {
						rf.matchIndex[j] = 0
					}
					// append a no-op log
					rf.Log = append(rf.Log, &LogEntry{Term: rf.CurrentTerm, Command: nil})
					rf.persist()

					select {
					case rf.logAppendedCh <- struct{}{}:
					default:
					}
				}
			}
		}(i)
	}
}

// try to send replicateReady every 0.1 second to claim its leadership
func (rf *Raft) sendHeartbeat() {
	for {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.replicateToFollower(i)
		}
		time.Sleep(100 * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]*LogEntry, 1)
	rf.Log[0] = &LogEntry{
		Term:    0,
		Command: nil,
	}
	rf.LastIncludedIndex = 0
	rf.SnapshotData = nil

	//volatile state on all servers:
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = follower

	//volatile state on leaders:
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	//applyCh
	rf.applyCh = applyCh

	//utils initialisation
	rf.lastHeartbeatTime = time.Now()
	rf.logAppendedCh = make(chan struct{}, 1)
	rf.applyReadyCh = make(chan struct{}, 1)
	rf.replicateReadyChs = make([]chan struct{}, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.replicateReadyChs[i] = make(chan struct{}, 1)
	}
	rf.snapshotPending = false
	rf.kill = make(chan struct{})

	debug.D3APrintf("%v starts as a follower", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start listening for replicate log invocation
	go rf.replicationDispatcher()
	// start sending heartbeats
	go rf.sendHeartbeat()
	go rf.applier()

	return rf
}
