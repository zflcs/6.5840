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
	electorTimer *time.Timer			// 选举超时定时器
	heartbeatTimer *time.Timer			// leader 心跳定时器
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
	// Your code here (2C).
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
	// Your code here (2D).

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
		// 状态改变之后，需要重置定时器
		rf.setElectorTimer()
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
	}
	rf.state = Follower
	rf.setElectorTimer()
	// TODO: 检查 log 是否能够匹配，不能直接截断 log，二是应该继续递减 nextIndex
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		xLen := len(rf.log)
		xIndex := -1
		xTerm := -1
		if args.PrevLogIndex < len(rf.log) {
			Debug(dDrop, "S%d's log exist conflict at %d-%d", rf.me, args.PrevLogIndex, len(rf.log))
			// rf.log = rf.log[: args.PrevLogIndex]
			// 找到冲突的 term 的第一个 log
			xIndex = args.PrevLogIndex
			xTerm = rf.log[args.PrevLogIndex].Term
			for idx := args.PrevLogIndex; idx >= 0; idx-- {
				if rf.log[idx].Term != xTerm {
					break
				}
				xIndex -= 1
			}
		}
		reply.XIndex = xIndex
		reply.XTerm = xTerm
		reply.XLen = xLen
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dDrop, "S%d's log exist conflict", rf.me)
		return
	}
	// TODO: 剔除冲突的日志，因为之前冲突时，日志没有变化，所以在 args.PrevLogIndex 之后的日志都存在问题
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > rf.log[len(rf.log)-1].Index {
		   rf.log = append(rf.log, entry)
		} else if index <= rf.log[0].Index {
		   //当追加的日志处于快照部分,那么直接跳过不处理该日志
		   continue
		} else {
		   if rf.log[index].Term != entry.Term {
			  rf.log = rf.log[:index] // 删除当前以及后续所有log
			  rf.log = append(rf.log, entry)                      
		   }
		}
	 }
  
	// rf.log = append(rf.log[: args.PrevLogIndex+1], args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.log)-1 < rf.commitIndex {
			rf.commitIndex = len(rf.log)-1
		}
		rf.applyCond.Signal()
	  }
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.setElectorTimer()
	Debug(dInfo, "S%d accept entries at T%d", rf.me, rf.currentTerm)
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
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{
			Term: term,
			Index: index,
			Command: command,
		})
		rf.matchIndex[rf.me] = len(rf.log)-1
		rf.nextIndex[rf.me] = len(rf.log)
		Debug(dClient, "L%d received a request", rf.me)
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
	if !rf.electorTimer.Stop() {	// 关闭选举定时器
		select {
		case <-rf.electorTimer.C: // try to drain the channel
		default:
		}
	}
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C: // try to drain the channel
		default:
		}
	}
	// rf.electorTimer.Stop()
	ms := 100 + (rand.Int63() % 200)
	rf.electorTimer.Reset(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) setHeartBeatTimer() {
	if !rf.electorTimer.Stop() {	// 关闭选举定时器
		select {
		case <-rf.electorTimer.C: // try to drain the channel
		default:
		}
	}
	if !rf.heartbeatTimer.Stop() {
		select {
		case <-rf.heartbeatTimer.C: // try to drain the channel
		default:
		}
	}
	// rf.electorTimer.Stop() // 关闭选举定时器
	// rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(time.Duration(50) * time.Millisecond)
}

// 获取 raft 最后一个日志的索引和 term，log 中第 0 个位置被占位
func (rf *Raft) lastLog() (int, int) {
	entry := rf.log[len(rf.log)-1]
	return entry.Index, entry.Term
}

func (rf *Raft) prevLog(peer int) (int, int) {
	nextIndex := rf.nextIndex[peer]
	prevEntry := rf.log[nextIndex-1]
	return prevEntry.Index, prevEntry.Term
}

func (rf *Raft) sendHeart() {
	rf.setHeartBeatTimer()
	term := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex
	Debug(dTerm, "L%d send AppendEntries at T%d", rf.me, term)
	for server := range rf.peers {
		if server != rf.me {
			go func (peer int)  {
				// 不需要循环到发送成功
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				prevLogIndex, prevLogTerm := rf.prevLog(peer)
				nextIdx := rf.nextIndex[peer]
				sendEntries := rf.log[nextIdx:]
				rf.mu.Unlock()
				args := AppendEntriesArgs{
					Term: term,
					LeaderId: leaderId,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm: prevLogTerm,
					Entries: sendEntries,
					LeaderCommit: leaderCommit,
				}
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					if rf.currentTerm == term && rf.state == Leader {
						if reply.Success {	// 返回成功暂时不进行处理
							rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[peer] = rf.matchIndex[peer]+1
						} else {
							if reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.votedFor = -1
								rf.currentTerm = reply.Term
								rf.setElectorTimer()
								Debug(dInfo, "S%d from Leader turn to Follower, reply T%d is large", rf.me, reply.Term)
							} else if reply.XTerm == -1 {	// 失败的情况，需要将 nextIndex - 1
								rf.nextIndex[peer] = reply.XLen
							} else if reply.XTerm != -1 {
								nextIndex := -1
								for i := 0; i < len(rf.log); i++ {
									if rf.log[i].Term == reply.XTerm {
										nextIndex = i
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

			}(server)
		}
	}
}

func (rf *Raft) newElection() {
	// if rf.state == Leader {
	// 	panic("raft state is error")
	// }
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
	for server := range rf.peers {
		if server != rf.me {
			go func(id int){
				// 如果候选者已经成功当选或者转为 Follower，则不会继续发送 RPC
				if rf.state != Candidate {	
					return
				}
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
								for idx := range rf.peers {
									rf.matchIndex[idx] = 0
									rf.nextIndex[idx] = len(rf.log)
								}
								// 发送心跳包
								rf.sendHeart()
								rf.commitCond.Signal()	// 释放 commitCond 条件变量
							}
						} else if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.setElectorTimer()
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
		select {
		case <- rf.electorTimer.C:
			rf.mu.Lock()
			rf.newElection()
			rf.mu.Unlock()
		case <- rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.sendHeart()
			}
			rf.mu.Unlock()
		}
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
		for i := rf.commitIndex + 1; i < len(rf.log); i++ {
			if rf.log[i].Term == rf.currentTerm {
				count := 0
				for idx := range rf.matchIndex {
					if rf.matchIndex[idx] >= i {
						count += 1
					}
				}
				if count > len(rf.peers) / 2 {	// 超过半数 server 备份，则 commit
					flag = true
					rf.commitIndex = i
					Debug(dCommit, "L%d commit Entry %v at T%d, commitIndex: %d", rf.me, rf.log[i], rf.currentTerm, rf.commitIndex)
				}
			}
		}
		if flag {
			rf.applyCond.Signal()
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
		Debug(dInfo, "S%d applies entries %d-%d in T%d", rf.me, lastApplied + 1, commitIndex + 1, rf.currentTerm)
		entries := rf.log[lastApplied+1 : commitIndex+1]
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
		// Debug(dInfo, "S%d applies entries %d-%d in T%d", rf.me, lastApplied, commitIndex, rf.currentTerm)
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
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
	rf.electorTimer = time.NewTimer(time.Duration(10) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(10) * time.Millisecond)
	rf.setElectorTimer()
	rf.heartbeatTimer.Stop()
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	// rf.log = append(rf.log, LogEntry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 10)
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = 1
	}
	rf.matchIndex = make([]int, 10)
	rf.commitCond = sync.Cond{L: &rf.mu}
	rf.applyCond = sync.Cond{L: &rf.mu}
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commit()
	go rf.apply()

	return rf
}
