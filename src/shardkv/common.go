package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                  Err = "OK"
	ErrNoKey                = "ErrNoKey"
	ErrWrongGroup           = "ErrWrongGroup"
	ErrWrongLeader          = "ErrWrongLeader"
	ErrOutDate              = "ErrOutDate"
	ErrNotReady             = "ErrNotReady"
	ErrInconsistentData     = "ErrInconsistentData"
	ErrOverTime             = "ErrOverTime"
)

type Err string

const (
	GET          = "Get"
	PUT          = "Put"
	APPEND       = "Append"
	UPDATECONFIG = "UpdateConfig"
	ADDSHARD     = "AddShard"
	DELETESHARD  = "DeleteShard"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid   int64
	SeqId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid   int64
	SeqId int
}

type GetReply struct {
	Err   Err
	Value string
}

type Shard struct {
	Data      map[string]string
	ConfigNum int
}

type ShardArgs struct {
	ShardId int
	Shard   Shard
	SeqMap  map[int64]int
	Cid     int64
	SeqId   int
}

type ShardReply struct {
	Err Err
}
