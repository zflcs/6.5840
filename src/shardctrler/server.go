package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead         int32
	configs      []Config // indexed by config num
	seqmap       map[int64]int
	waitOpCh     map[int](chan Op)
	waitDuration time.Duration
}

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type Op struct {
	// Your data here.
	Optype  string
	Cid     int64
	SeqId   int
	Servers map[int][]string // join
	GIDs    []int            // leave
	Shard   int              // move
	GID     int
	Num     int    // query
	Config  Config // query reply
}

func (sc *ShardCtrler) getOpCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	ch, ok := sc.waitOpCh[index]
	if !ok {
		ch = make(chan Op, 1)
		sc.waitOpCh[index] = ch
	}
	return ch
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Optype:  JOIN,
		Cid:     args.Cid,
		SeqId:   args.SeqId,
		Servers: args.Servers,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return
	}
	ch := sc.getOpCh(index)
	select {
	case appliedOp := <-ch:
		if op.Cid == appliedOp.Cid && op.SeqId == appliedOp.SeqId {
			reply.WrongLeader = false
			reply.Err = OK
		}
	case <-time.After(sc.waitDuration):
		reply.Err = "ErrWrongLeader"
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Optype: LEAVE,
		Cid:    args.Cid,
		SeqId:  args.SeqId,
		GIDs:   args.GIDs,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return
	}
	ch := sc.getOpCh(index)
	select {
	case appliedOp := <-ch:
		if op.Cid == appliedOp.Cid && op.SeqId == appliedOp.SeqId {
			reply.WrongLeader = false
			reply.Err = OK
		}
	case <-time.After(sc.waitDuration):
		reply.Err = "ErrWrongLeader"
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Optype: MOVE,
		Cid:    args.Cid,
		SeqId:  args.SeqId,
		Shard:  args.Shard,
		GID:    args.GID,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return
	}
	ch := sc.getOpCh(index)
	select {
	case appliedOp := <-ch:
		if op.Cid == appliedOp.Cid && op.SeqId == appliedOp.SeqId {
			reply.WrongLeader = false
			reply.Err = OK
		}
	case <-time.After(sc.waitDuration):
		reply.Err = "ErrWrongLeader"
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Optype: QUERY,
		Cid:    args.Cid,
		SeqId:  args.SeqId,
		Num:    args.Num,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return
	}
	ch := sc.getOpCh(index)
	select {
	case appliedOp := <-ch:
		if op.Cid == appliedOp.Cid && op.SeqId == appliedOp.SeqId {
			reply.WrongLeader = false
			reply.Err = OK
			reply.Config = appliedOp.Config
		}
	case <-time.After(sc.waitDuration):
		reply.Err = "ErrWrongLeader"
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) run() {
	for msg := range sc.applyCh {
		if sc.killed() {
			return
		}
		if msg.CommandValid { // 日志
			op := msg.Command.(Op)
			sc.mu.Lock()
			if op.SeqId > sc.seqmap[op.Cid] { // 只对最新的序列号进行响应
				// 生成 newConfig
				prevConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{
					Num: prevConfig.Num + 1,
					Shards: prevConfig.Shards,
					Groups: deepCopy(prevConfig.Groups),
				}
				switch op.Optype {
				case JOIN:
					for gid, servers := range op.Servers {
						newConfig.Groups[gid] = servers
					}
					groups := shardsToGroup(&newConfig)
					// 循环，从最大的 shards 移动到 最小的 shards
					for {
						maxGid, minGid := maxGroupGid(groups), minGroupGid(groups)
						if maxGid != 0 && len(groups[maxGid]) - len(groups[minGid]) <= 1 {
							break
						}
						groups[minGid] = append(groups[minGid], groups[maxGid][0])
						groups[maxGid] = groups[maxGid][1:]
					}
					var newShards [NShards]int
					for gid, shards := range groups {
						for _, shard := range shards {
							newShards[shard] = gid
						}
					}
					newConfig.Shards = newShards
					sc.configs = append(sc.configs, newConfig)
				case LEAVE:
					groups := shardsToGroup(&newConfig)
					orphanShards := make([]int, 0)
					for _, gid := range op.GIDs {
						if _, ok := newConfig.Groups[gid]; ok {
							delete(newConfig.Groups, gid)
						}
						if shards, ok := groups[gid]; ok {
							orphanShards = append(orphanShards, shards...)
							delete(groups, gid)
						}
					}
					var newShards [NShards]int
					// load balancing is performed only when raft groups exist
					if len(newConfig.Groups) != 0 {
						for _, shard := range orphanShards {
							target := minGroupGid(groups)
							groups[target] = append(groups[target], shard)
						}
						for gid, shards := range groups {
							for _, shard := range shards {
								newShards[shard] = gid
							}
						}
					}
					newConfig.Shards = newShards
					sc.configs = append(sc.configs, newConfig)
				case MOVE:
					newConfig.Shards[op.Shard] = op.GID
					sc.configs = append(sc.configs, newConfig)
					// kv.kvdata[op.Key] += op.Value
				case QUERY:
				}
				sc.seqmap[op.Cid] = op.SeqId
				// Debug(dInfo, "S%d get msg:%v from applych, operation(put/append) key:%v, value:%v", sc.me, msg, op.Key, kv.kvdata[op.Key])
			}
			if op.Optype == QUERY {
				if op.Num == -1 || op.Num >= len(sc.configs) {
					op.Config = sc.configs[len(sc.configs)-1]
				} else {
					op.Config = sc.configs[op.Num]
				}
			}
			sc.mu.Unlock()
			sc.getOpCh(msg.CommandIndex) <- op
		}
	}
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func shardsToGroup(config *Config) map[int][]int {
	group := make(map[int][]int)
	for gid := range config.Groups {
		group[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		group[gid] = append(group[gid], shard)
	}
	return group
}

func minGroupGid(groups map[int][]int) int {
	var keys []int
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	ans, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(groups[gid]) < min {
			ans, min = gid, len(groups[gid])
		}
	}
	return ans
}

func maxGroupGid(groups map[int][]int) int {
	if shards, ok := groups[0]; ok && len(shards) > 0 {
		return 0
	}
	var keys []int
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	ans, max := -1, -1
	for _, gid := range keys {
		if len(groups[gid]) > max {
			ans, max = gid, len(groups[gid])
		}
	}
	return ans
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqmap = make(map[int64]int)
	sc.waitOpCh = make(map[int]chan Op)
	sc.waitDuration = 500 * time.Millisecond
	go sc.run()

	return sc
}
