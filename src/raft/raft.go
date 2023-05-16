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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const Follower = 0
const Candidate = 1
const Leader = 2

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applych chan ApplyMsg
	electorTriggle time.Time					// 选举超时触发数值
	commitCond sync.Cond				// commit 条件变量
	applyCond sync.Cond					// apply 条件变量

	state int
	currentTerm int
	votedFor int
	log []LogEntry
	
	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	snapshot []byte			// 快照
	leaderId int
}

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	term = 0
	rf.mu.Lock()
	if rf.state == Leader {
		isleader = true
	}
	term = rf.currentTerm
	rf.mu.Unlock()
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
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		Debug(dError, "S%d decode error", rf.me)	
		return
	} else {
		rf.mu.Lock()
	  	rf.currentTerm = currentTerm
	  	rf.votedFor = votedFor
	  	rf.log = log
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	offsetIndex, _ := rf.offset() 
	if index <= offsetIndex || index > rf.commitIndex {
		return
	}
	Debug(dSnap, "S%d snapshot %d", rf.me, index)
	position := 1
	for idx, entry := range rf.log {
		if idx != 0 {	// 跳过 snapshot 日志
			position += 1
			rf.setOffset(entry.Index, entry.Term)
			if entry.Index == index {
				break
			}
		}
	}
	newLog := []LogEntry{}
	newLog = append(newLog, rf.log[0])
	newLog = append(newLog, rf.log[position:]...)
	rf.log = newLog
	rf.snapshot = snapshot

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	Debug(dSnap, "S%d log len %d", rf.me, len(rf.log))
	rf.persister.Save(raftstate, rf.snapshot)
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	XTerm int	// 冲突的 Term
	XIndex int	// 冲突的 Term 的第一个 log 下标
	XLen int	// 日志长度
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool 
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d reject voting to %d at T%d, term is large than %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d reject voting to %d at T%d, has voted", rf.me, args.CandidateId, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	}
	// TODO: 检查 log 是否足够新
	lastLogIndex, lastLogTerm := rf.lastLog()
	if args.LastLogTerm < lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d reject voting to %d at T%d, Candidate's log is outdate", rf.me, args.CandidateId, rf.currentTerm)
		return
	}
	rf.votedFor = args.CandidateId
	rf.setElectorTimer()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	Debug(dVote, "S%d agreed vote to %d at T%d", rf.me, args.CandidateId, rf.currentTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = -1
		Debug(dDrop, "S%d reject append Entries from %d at T%d, term is large than %d", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	}
	rf.state = Follower
	rf.setElectorTimer()
	rf.leaderId = args.LeaderId
	// TODO: 检查 log 是否能够匹配，不能直接截断 log，二是应该继续递减 nextIndex
	offsetIndex, _ := rf.offset()
	if args.PrevLogIndex < offsetIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dDrop, "S%d 's offset(%d) is large than args(%d)", rf.me, offsetIndex, args.PrevLogIndex)
		return
	}
	if args.PrevLogIndex - offsetIndex >= len(rf.log) || rf.log[args.PrevLogIndex - offsetIndex].Term != args.PrevLogTerm {
		xLen := len(rf.log) + offsetIndex
		xIndex := -1
		xTerm := -1
		if args.PrevLogIndex - offsetIndex < len(rf.log) {
			Debug(dDrop, "S%d's log exist conflict at %d-%d", rf.me, args.PrevLogIndex, len(rf.log))
			// rf.log = rf.log[: args.PrevLogIndex]
			// 找到冲突的 term 的第一个 log
			xIndex = args.PrevLogIndex
			xTerm = rf.log[args.PrevLogIndex - offsetIndex].Term
			for idx := args.PrevLogIndex - offsetIndex; idx >= 0; idx-- {
				if rf.log[idx].Term != xTerm {
					break
				}
				xIndex = rf.log[idx].Index
			}
		}
		reply.XIndex = xIndex
		reply.XTerm = xTerm
		reply.XLen = xLen
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dDrop, "S%d's log exist conflict, prevLogIndex:%d prevLogTerm:%d, log len:%d", rf.me, args.PrevLogIndex, args.PrevLogTerm, len(rf.log))
		return
	}
	// TODO: 剔除冲突的日志，因为之前冲突时，日志没有变化，所以在 args.PrevLogIndex 之后的日志都存在问题
	for _, entry := range args.Entries {
		index := entry.Index
		lastIndex, _ := rf.lastLog()
		if index > lastIndex {
		   rf.log = append(rf.log, entry)
		} else if index <= rf.log[0].Index {
		   //当追加的日志处于快照部分,那么直接跳过不处理该日志
		   continue
		} else {
		   if rf.log[index - offsetIndex].Term != entry.Term {
			  rf.log = rf.log[:index - offsetIndex] // 删除当前以及后续所有log
			  rf.log = append(rf.log, entry)                      
		   }
		}
	}
	lastLogIndex, _ := rf.lastLog()
	commitIndex := 0
	if lastLogIndex < args.LeaderCommit {
		commitIndex = lastLogIndex
	} else {
		commitIndex = args.LeaderCommit
	}
	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		rf.applyCond.Signal()
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.setElectorTimer()
	Debug(dInfo, "S%d accept entries at T%d", rf.me, rf.currentTerm)
}

// TODO: 更新 snapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		Debug(dDrop, "S%d drop snapshot rpc from %d, term is small", rf.me, args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
		Debug(dInfo, "S%d turn to follower, args term:%d is large", rf.me, args.Term)
	}
	rf.state = Follower
	rf.setElectorTimer()
	rf.leaderId = args.LeaderId
	rf.persist()
	offsetIndex, _ := rf.offset()
	if args.LastIncludedIndex <= offsetIndex {
		return
	}
	newLog := make([]LogEntry, 1)
	lastLogIndex, _ := rf.lastLog()
	for i := args.LastIncludedIndex + 1; i <= lastLogIndex; i++ {
		newLog = append(newLog, rf.log[i - offsetIndex])
	}
	rf.log = newLog
	rf.setOffset(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.snapshot = args.Data
	Debug(dSnap, "S%d install snapshot rpc success, index:%d, term:%d, log len:%d", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
		Debug(dCommit, "S%d update commitIndex %d", rf.me, rf.commitIndex)
		rf.applyCond.Signal()
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	isLeader := false

	// Your code here (2B).
	if rf.killed() {
		return index, term, isLeader
	}
	rf.mu.Lock()
	if rf.state == Leader {
		isLeader = true
		term = rf.currentTerm
		offsetIndex, _ := rf.offset()
		index = len(rf.log) + offsetIndex
		rf.log = append(rf.log, LogEntry{
			Term: term,
			Index: index,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		Debug(dClient, "L%d received a request, term:%d, index:%d", rf.me, term, index)
		rf.persist()
		rf.sendHeart()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setElectorTimer() {
	// rf.electorTimer.Stop()
	ms := 150 + (rand.Int63() % 150)
	rf.electorTriggle = time.Now().Add(time.Duration(ms) * time.Millisecond)
}
// 获取日志偏移
func (rf *Raft) offset() (int, int) {
	snapEntry := rf.log[0]
	return snapEntry.Index, snapEntry.Term
}

// 设置日志下标偏移
func (rf *Raft) setOffset(index int, term int) {
	rf.log[0].Index = index
	rf.log[0].Term = term
}

// 获取 raft 最后一个日志的索引和 term，log 中第 0 个位置被占位
func (rf *Raft) lastLog() (int, int) {
	entry := rf.log[len(rf.log)-1]
	return entry.Index, entry.Term
}

func (rf *Raft) prevLog(peer int) (int, int) {
	nextIndex := rf.nextIndex[peer]
	offsetIndex, offsetTerm := rf.offset()
	if nextIndex > offsetIndex && nextIndex <= len(rf.log) + offsetIndex {
		prevEntry := rf.log[nextIndex - 1 - offsetIndex]
		return prevEntry.Index, prevEntry.Term
	} else if nextIndex > len(rf.log) + offsetIndex {
		rf.nextIndex[peer] = len(rf.log) + offsetIndex
		prevEntry := rf.log[len(rf.log) - 1]
		return prevEntry.Index, prevEntry.Term
	}
	return offsetIndex, offsetTerm
}

func (rf *Raft) sendHeart() {
	term := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex
	for server := range rf.peers {
		if server != rf.me {
			prevLogIndex, prevLogTerm := rf.prevLog(server)
			nextIdx := rf.nextIndex[server]
			offsetIndex, offsetTerm := rf.offset()
			if nextIdx > offsetIndex {		// 发送日志
				sendEntries := rf.log[nextIdx - offsetIndex:]
				args := AppendEntriesArgs{
					Term: term,
					LeaderId: leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm: prevLogTerm,
					Entries: sendEntries,
					LeaderCommit: leaderCommit,
				}
				reply := AppendEntriesReply{}
				go func(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
					rf.mu.Lock()
					Debug(dTerm, "L%d send AppendEntries at T%d", rf.me, args.Term)
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					if rf.sendAppendEntries(peer, &args, &reply) {
						rf.mu.Lock()
						if rf.currentTerm == term && rf.state == Leader {
							if reply.Success {	// 返回成功暂时不进行处理
								rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
								rf.nextIndex[peer] = rf.matchIndex[peer] + 1
							} else {
								if reply.Term > rf.currentTerm {
									rf.state = Follower
									rf.votedFor = -1
									rf.currentTerm = reply.Term
									rf.leaderId = -1
									rf.persist()
									Debug(dInfo, "S%d from Leader turn to Follower, AppendEntries reply T%d is large", rf.me, reply.Term)
								} else if reply.XTerm == -1 {	// 失败的情况，需要将 nextIndex - 1
									rf.nextIndex[peer] = reply.XLen
								} else if reply.XTerm != -1 {
									nextIndex := -1
									for i := 1; i < len(rf.log); i++ {
										if rf.log[i].Term == reply.XTerm {
											nextIndex = rf.log[i].Index
										}
									}
									if nextIndex != -1 {
										rf.nextIndex[peer] = nextIndex
									} else {
										rf.nextIndex[peer] = reply.XIndex
									}
								}
							} 
						}
						rf.mu.Unlock()
					}
				}(server, args, reply)
			} else {		// 发送快照
				args := InstallSnapshotArgs{	// 每次直接发送全部的 snapshot 文件
					Term: term,
					LeaderId: leaderId,
					LastIncludedIndex: offsetIndex,
					LastIncludedTerm: offsetTerm,
					Offset: 0,
					Data: rf.snapshot,
					Done: true,
				}
				reply := InstallSnapshotReply{}
				go func (peer int, args InstallSnapshotArgs, reply InstallSnapshotReply)  {
					Debug(dSnap, "L%d send snapshot to %d, index:%d, term:%d", rf.me, peer, args.LastIncludedIndex, args.LastIncludedTerm)
					if rf.sendInstallSnapshot(peer, &args, &reply) {
						rf.mu.Lock()
						if rf.currentTerm == term && rf.state == Leader {
							if reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.state = Follower
								rf.votedFor = -1
								rf.leaderId = -1
								rf.persist()
								Debug(dInfo, "S%d from Leader turn to Follower, InstallSnapshot reply T%d is large", rf.me, reply.Term)
							} else {
								if rf.matchIndex[peer] < args.LastIncludedIndex {
									rf.matchIndex[peer] = args.LastIncludedIndex
								}
								if rf.nextIndex[peer] < args.LastIncludedIndex + 1 {
									rf.nextIndex[peer] = args.LastIncludedIndex + 1
								}
							}
						}
						rf.mu.Unlock()
					}
				}(server, args, reply)	
			}
		}
	}
}

func (rf *Raft) newElection() {
	rf.setElectorTimer()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	votedNum := 1
	term := rf.currentTerm
	candidateId := rf.me
	lastLogIndex, lastLogTerm := rf.lastLog()
	Debug(dTerm, "C%d conduct a new election at T%d", rf.me, term)
	args := RequestVoteArgs{
		Term: term,
		CandidateId: candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	rf.persist()
	for server := range rf.peers {
		if server != rf.me {
			go func(id int){
				// 如果候选者已经成功当选或者转为 Follower，则不会继续发送 RPC
				rf.mu.Lock()
				if rf.state != Candidate {	
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				if rf.sendRequestVote(id, &args, &reply) {
					rf.mu.Lock()
					// 如果已经成为 leader 或者 follower，对于回复不予处理
					// 如果回复已经过去，不做处理
					if rf.state == Candidate && rf.currentTerm == term {
						if reply.VoteGranted {
							votedNum += 1
							if votedNum > len(rf.peers) / 2 {
								rf.state = Leader
								offsetIndex, _ := rf.offset()
								for idx := range rf.peers {
									rf.matchIndex[idx] = 0
									rf.nextIndex[idx] = len(rf.log) + offsetIndex
								}
								rf.leaderId = rf.me
								// 发送心跳包
								rf.sendHeart()
								rf.commitCond.Signal()	// 释放 commitCond 条件变量
							}
						} else if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.leaderId = -1
							rf.persist()
							Debug(dInfo, "S%d from Candidate turn to Follower, reply T%d is large", rf.me, reply.Term)
						}
					}
					rf.mu.Unlock()
				}
			}(server)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader && time.Now().After(rf.electorTriggle) {
			rf.newElection()
		} else if rf.state == Leader {
			rf.sendHeart()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(60) * time.Millisecond)
	}
}

func (rf *Raft) commit() {	// leader 更新 commitIndex，如果不是 leader 则阻塞
	for !rf.killed() {
		rf.commitCond.L.Lock()
		for rf.state != Leader {	// 如果 raft 不是 leader，则阻塞住
			rf.commitCond.Wait()
		}
		// TODO: 依次检查 commitIndex 之后的日志是否能被 commit
		flag := false
		offsetIndex, _ := rf.offset()
		if rf.commitIndex + 1 > offsetIndex {
			for i := rf.commitIndex + 1 - offsetIndex; i < len(rf.log); i++ {
				if rf.log[i].Term == rf.currentTerm {
					count := 0
					for idx := range rf.matchIndex {
						if rf.matchIndex[idx] >= i + offsetIndex {
							count += 1
						}
					}
					if count > len(rf.peers) / 2 {	// 超过半数 server 备份，则 commit
						flag = true
						rf.commitIndex = i + offsetIndex
						Debug(dCommit, "L%d commit Entry %v at T%d, commitIndex: %d", rf.me, rf.log[i], rf.currentTerm, rf.commitIndex)
					}
				}
			}
			if flag {
				rf.applyCond.Signal()
			}
		}
		
		rf.commitCond.L.Unlock()
		time.Sleep(time.Duration(20) * time.Millisecond)
	}
}

func (rf *Raft) apply() {	// server 应用到状态机中
	for !rf.killed() {
		rf.applyCond.L.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()	
		}
		lastApplied, commitIndex := rf.lastApplied, rf.commitIndex
		offsetIndex, offsetTerm := rf.offset()
		if lastApplied < offsetIndex {
			Debug(dInfo, "S%d applies snapshot index:%d term:%d in T%d", rf.me, offsetIndex, offsetTerm, rf.currentTerm)
			rf.applyCond.L.Unlock()
			rf.applych <- ApplyMsg{
				CommandValid: false,
				Snapshot:      rf.snapshot,
				SnapshotValid: true,
				SnapshotTerm:  offsetTerm,
				SnapshotIndex: offsetIndex,
			}
			rf.mu.Lock()
			if offsetIndex > rf.lastApplied && lastApplied == rf.lastApplied {
				rf.lastApplied = offsetIndex
			}
			rf.mu.Unlock()
		} else {
			Debug(dInfo, "S%d applies entries %d-%d in T%d", rf.me, lastApplied + 1, commitIndex + 1, rf.currentTerm)
			entries := rf.log[lastApplied+1-offsetIndex : commitIndex+1-offsetIndex]
			rf.applyCond.L.Unlock()
			for _, entry := range entries {
				rf.applych <- ApplyMsg{
					CommandValid: true,
					Command: entry.Command,
					CommandIndex: entry.Index,
					SnapshotValid: false,
				}
			}
			rf.mu.Lock()
			if commitIndex > rf.lastApplied {
				rf.lastApplied = commitIndex
			}
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(20) * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).
	atomic.StoreInt32(&rf.dead, 0)
	rf.mu = sync.Mutex{}
	rf.mu.Lock()
	rf.applych = applyCh
	rf.electorTriggle = time.Now()
	rf.setElectorTimer()
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.setOffset(0, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 10)
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = 1
	}
	rf.matchIndex = make([]int, 10)
	rf.commitCond = sync.Cond{L: &rf.mu}
	rf.applyCond = sync.Cond{L: &rf.mu}
	rf.snapshot = persister.ReadSnapshot()	
	rf.leaderId = -1
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	offsetIndex, _ := rf.offset()
	rf.commitIndex = offsetIndex
	rf.lastApplied = offsetIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commit()
	go rf.apply()

	return rf
}
