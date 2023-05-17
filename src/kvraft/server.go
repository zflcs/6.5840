package kvraft

import (
	"bytes"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"

	// "log"
	"sync"
	"sync/atomic"
)

// const Debug = false

// func DPrintf(format string, a ...interface{}) (n int, err error) {
// 	if Debug {
// 		log.Printf(format, a...)
// 	}
// 	return
// }

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype string
	Cid    int64
	SeqId  int
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdata       map[string]string
	seqmap       map[int64]int
	waitOpCh     map[int](chan Op)
	waitDuration time.Duration
	persister    *raft.Persister
}

func (kv *KVServer) getOpCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, ok := kv.waitOpCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.waitOpCh[index] = ch
	}
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Optype: GET,
		Key:    args.Key,
		Cid:    args.Cid,
		SeqId:  args.SeqId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getOpCh(index)
	select {
	case appliedOp := <-ch:
		if op.Cid == appliedOp.Cid && op.SeqId == appliedOp.SeqId {
			reply.Value = appliedOp.Value
			if reply.Value == "" {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
			}
		}
	case <-time.After(kv.waitDuration):
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Optype: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Cid:    args.Cid,
		SeqId:  args.SeqId,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getOpCh(index)
	select {
	case appliedOp := <-ch:
		if op.Cid == appliedOp.Cid && op.SeqId == appliedOp.SeqId {
			reply.Err = OK
		}
	case <-time.After(kv.waitDuration):
		reply.Err = ErrWrongLeader
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) run() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		if msg.CommandValid { // 日志
			op := msg.Command.(Op)
			kv.mu.Lock()
			if op.SeqId > kv.seqmap[op.Cid] { // 只对最新的序列号进行响应
				switch op.Optype {
				case GET:
					// do nothing
				case PUT:
					kv.kvdata[op.Key] = op.Value
				case APPEND:
					kv.kvdata[op.Key] += op.Value
				}
				kv.seqmap[op.Cid] = op.SeqId
				Debug(dInfo, "S%d get msg:%v from applych, operation(put/append) key:%v, value:%v", kv.me, msg, op.Key, kv.kvdata[op.Key])
			} else {
				Debug(dDrop, "S%d, op seq is duplicated", kv.me)
			}

			if op.Optype == GET {
				op.Value = kv.kvdata[op.Key]
				Debug(dInfo, "S%d get msg:%v from applych, operation(get) key:%v, value:%v", kv.me, msg, op.Key, kv.kvdata[op.Key])
			}
			// 判断是否需要生成快照, msg 被提交了，所以在这里判断，如果满足条件，直接调用 rf.snapshot
			currentStateSize := len(kv.persister.ReadRaftState())
			if kv.maxraftstate > 0 &&  currentStateSize >= kv.maxraftstate {
				go kv.rf.Snapshot(msg.CommandIndex, kv.getSnapshot())
			}
			kv.mu.Unlock()
			kv.getOpCh(msg.CommandIndex) <- op
		} else {
			// TODO: 从快照中读取 kvdata
			snapshot := msg.Snapshot
			kv.readSnapshot(snapshot)
		}
	}
}

// 对整个 KV 数据生成快照
func (kv *KVServer) getSnapshot() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(kv.kvdata)
	encoder.Encode(kv.seqmap)
	snapshot := buffer.Bytes()
	return snapshot
}

func (kv *KVServer) readSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 {
		// DPrintf("=%v= the snapshot is useless", kv.me)
		return
	}

	var kvdata map[string]string
	var seqmap map[int64]int
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	if decoder.Decode(&kvdata) != nil || decoder.Decode(&seqmap) != nil {
	} else {
		kv.kvdata = kvdata
		kv.seqmap = seqmap
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvdata = make(map[string]string)
	kv.seqmap = make(map[int64]int)
	kv.waitOpCh = make(map[int]chan Op)
	kv.waitDuration = 500 * time.Millisecond
	kv.persister = persister
	kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.run()
	return kv
}
