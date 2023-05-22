package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype       string
	Cid          int64
	SeqId        int
	Key          string
	Value        string
	UpdateConfig shardctrler.Config
	ShardId      int
	Shard        Shard
	SeqMap       map[int64]int
}

type OpReply struct {
	Cid   int64
	SeqId int
	Err   Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead         int32
	lastConfig   shardctrler.Config
	config       shardctrler.Config
	shards       []Shard
	seqmap       map[int64]int
	waitOpCh     map[int](chan OpReply)
	waitDuration time.Duration
	sc           *shardctrler.Clerk
	persister    *raft.Persister
}

func (kv *ShardKV) getOpCh(index int) chan OpReply {
	ch, ok := kv.waitOpCh[index]
	if !ok {
		ch = make(chan OpReply, 1)
		kv.waitOpCh[index] = ch
	}
	return ch
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 在发送到 raft 之前直接判断是否属于当前的 shard
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shards[shardId].Data == nil {
		reply.Err = ErrNotReady
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ErrNotReady {
		return
	}
	op := Op{
		Optype: GET,
		Key:    args.Key,
		Cid:    args.Cid,
		SeqId:  args.SeqId,
	}
	err := kv.send2Raft(op)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shards[shardId].Data == nil {
		reply.Err = ErrNotReady
	} else {
		reply.Err = OK
		reply.Value = kv.shards[shardId].Data[args.Key]
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shards[shardId].Data == nil {
		reply.Err = ErrNotReady
	}
	kv.mu.Unlock()
	if reply.Err == ErrWrongGroup || reply.Err == ErrNotReady {
		return
	}
	op := Op{
		Optype: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Cid:    args.Cid,
		SeqId:  args.SeqId,
	}
	reply.Err = kv.send2Raft(op)
}

// AddShard move shards from caller to this server
func (kv *ShardKV) AddShard(args *ShardArgs, reply *ShardReply) {
	op := Op{
		Optype:  ADDSHARD,
		Cid:     args.Cid,
		SeqId:   args.SeqId,
		ShardId: args.ShardId,
		Shard:   args.Shard,
		SeqMap:  args.SeqMap,
	}
	reply.Err = kv.send2Raft(op)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mu = sync.Mutex{}
	kv.dead = 0
	kv.shards = make([]Shard, shardctrler.NShards)
	kv.seqmap = make(map[int64]int)
	kv.sc = shardctrler.MakeClerk(kv.ctrlers)
	kv.waitDuration = 500 * time.Millisecond
	kv.waitOpCh = make(map[int]chan OpReply)
	kv.persister = persister
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.fromSnapshot(snapshot)
	}
	go kv.Run()
	go kv.CheckConfig()
	return kv
}

// PersistSnapShot Snapshot get snapshot data of kvserver
func (kv *ShardKV) GenSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.seqmap)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	return w.Bytes()
}

func (kv *ShardKV) fromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards []Shard
	var seqMap map[int64]int
	var config, lastConfig shardctrler.Config
	if d.Decode(&shards) != nil || d.Decode(&seqMap) != nil || d.Decode(&config) != nil || d.Decode(&lastConfig) != nil {
	} else {
		kv.shards = shards
		kv.seqmap = seqMap
		kv.config = config
		kv.lastConfig = lastConfig
	}
}

func (kv *ShardKV) allShardSend() bool {
	for shard, gid := range kv.lastConfig.Shards {
		if gid == kv.gid && kv.config.Shards[shard] != kv.gid && kv.shards[shard].ConfigNum < kv.config.Num {	// 存在分片未发送
			return false
		}
	}
	return true
}

func (kv *ShardKV) allShardReceived() bool {
	for shard, gid := range kv.lastConfig.Shards {
		if gid != kv.gid && kv.config.Shards[shard] == kv.gid && kv.shards[shard].ConfigNum < kv.config.Num {	// 存在分片没有收到
			return false
		}
	}
	return true
}

// 判断当前节点是否处于迁移期
func (kv *ShardKV) getMigrateShard(ConfigNum int, KvMap map[string]string) Shard {
	migrateShard := Shard{
		Data:      make(map[string]string),
		ConfigNum: ConfigNum,
	}
	for k, v := range KvMap {
		migrateShard.Data[k] = v
	}
	return migrateShard
}

func (kv *ShardKV) CheckConfig() {
	kv.mu.Lock()
	currentConfig := kv.config
	rf := kv.rf
	kv.mu.Unlock()
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		if _, isLeader := rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()
		// 判断是否把不属于自己的部分给分给别人了
		if !kv.allShardSend() {
			seqMap := make(map[int64]int)
			for k, v := range kv.seqmap {
				seqMap[k] = v
			}
			for shardId, gid := range kv.lastConfig.Shards {
				if gid == kv.gid && kv.config.Shards[shardId] != kv.gid && kv.shards[shardId].ConfigNum < kv.config.Num {
					sendDate := kv.getMigrateShard(kv.config.Num, kv.shards[shardId].Data)
					args := ShardArgs{
						SeqMap:  seqMap,
						ShardId: shardId,
						Shard:   sendDate,
						Cid:     int64(gid),
						SeqId:   kv.config.Num,
					}
					serversList := kv.config.Groups[kv.config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.make_end(name)
					}
					// 开启协程对每个客户端发送切片(这里发送的应是别的组别，自身的共识组需要raft进行状态修改）
					go func(servers []*labrpc.ClientEnd, args *ShardArgs) {
						index := 0
						start := time.Now()
						for {
							var reply ShardReply
							// 对自己的共识组内进行add
							ok := servers[index].Call("ShardKV.AddShard", args, &reply)
							if ok && reply.Err == OK || time.Since(start) >= 2*time.Second {
								// 如果成功
								kv.mu.Lock()
								op := Op{
									Optype:  DELETESHARD,
									Cid:     int64(kv.gid),
									SeqId:   kv.config.Num,
									ShardId: args.ShardId,
								}
								kv.mu.Unlock()
								kv.send2Raft(op)
								break
							}
							index = (index + 1) % len(servers)
							if index == 0 {
								time.Sleep(100 *time.Millisecond)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			continue
		}
		if !kv.allShardReceived() {
			kv.mu.Unlock()
			continue
		}

		// current configuration is configured, poll for the next configuration
		currentConfig = kv.config
		sck := kv.sc
		kv.mu.Unlock()

		newConfig := sck.Query(currentConfig.Num + 1)
		if newConfig.Num != currentConfig.Num+1 {
			continue
		}

		op := Op{
			Optype:       UPDATECONFIG,
			Cid:          int64(kv.gid),
			SeqId:        newConfig.Num,
			UpdateConfig: newConfig,
		}
		kv.send2Raft(op)
	}
}

func (kv *ShardKV) send2Raft(op Op) Err {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	ch := kv.getOpCh(index)
	kv.mu.Unlock()
	select {
	case opReply := <-ch:
		kv.mu.Lock()
		delete(kv.waitOpCh, index)
		if opReply.SeqId != op.SeqId || opReply.Cid != op.Cid {
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()
		return opReply.Err

	case <-time.After(kv.waitDuration):
		return ErrOverTime
	}
}

func (kv *ShardKV) isDuplicate(clientId int64, seqId int) bool {
	lastSeqId, ok := kv.seqmap[clientId]
	if !ok {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *ShardKV) Run() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				op := msg.Command.(Op)
				reply := OpReply{
					Cid:   op.Cid,
					SeqId: op.SeqId,
					Err:   OK,
				}
				if op.Optype == PUT || op.Optype == GET || op.Optype == APPEND {
					shardId := key2shard(op.Key)
					if kv.config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.shards[shardId].Data == nil {
						// 如果应该存在的切片没有数据那么这个切片就还没到达
						reply.Err = ErrNotReady
					} else {
						if !kv.isDuplicate(op.Cid, op.SeqId) {
							kv.seqmap[op.Cid] = op.SeqId
							switch op.Optype {
							case PUT:
								kv.shards[shardId].Data[op.Key] = op.Value
							case APPEND:
								kv.shards[shardId].Data[op.Key] += op.Value
							case GET:
								// Do nothing
							}
						}
					}
				} else {
					// request from server of other group
					switch op.Optype {
					case UPDATECONFIG:
						curConfig := kv.config
						upConfig := op.UpdateConfig
						if curConfig.Num < upConfig.Num {
							for shard, gid := range upConfig.Shards {
								if gid == kv.gid && curConfig.Shards[shard] == 0 {
									// 如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配）
									kv.shards[shard].Data = make(map[string]string)
									kv.shards[shard].ConfigNum = upConfig.Num
								}
							}
							kv.lastConfig = curConfig
							kv.config = upConfig
						}
					case ADDSHARD:
						// 如果配置号比op的SeqId还低说明不是最新的配置
						if kv.config.Num < op.SeqId {
							reply.Err = ErrNotReady
							break
						}
						if !(kv.shards[op.ShardId].Data != nil || op.Shard.ConfigNum < kv.config.Num) {
							kv.shards[op.ShardId] = kv.getMigrateShard(op.Shard.ConfigNum, op.Shard.Data)
							for clientId, seqId := range op.SeqMap {
								if r, ok := kv.seqmap[clientId]; !ok || r < seqId {
									kv.seqmap[clientId] = seqId
								}
							}
						}
					case DELETESHARD:
						if op.SeqId >= kv.config.Num {
							kv.shards[op.ShardId].Data = nil
							kv.shards[op.ShardId].ConfigNum = op.SeqId
						}
					}
				}

				// 如果需要snapshot，且超出其stateSize
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
					snapshot := kv.GenSnapshot()
					kv.rf.Snapshot(msg.CommandIndex, snapshot)
				}
				ch := kv.getOpCh(msg.CommandIndex)
				ch <- reply
				kv.mu.Unlock()
			}
			if msg.SnapshotValid {
				// 读取快照的数据
				kv.mu.Lock()
				kv.fromSnapshot(msg.Snapshot)
				kv.mu.Unlock()
				continue
			}
		}
	}
}