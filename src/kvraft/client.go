package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cid int64
	nextSeq int
	prevLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.cid = nrand()
	ck.nextSeq = 1
	ck.prevLeader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		Cid: ck.cid,
		SeqId: ck.nextSeq,
	}
	ck.nextSeq ++
	for i := ck.prevLeader; ; i = (i + 1) % len(ck.servers) {	// 总有一个 leader，因此可以退出循环
		reply := GetReply{}
		if ck.servers[i].Call("KVServer.Get", &args, &reply) {
			switch reply.Err {
			case OK:
				ck.prevLeader = i
				Debug(dInfo, "S%d GET:%v done, Value:%v", i, key, reply.Value)
				return reply.Value
			case ErrNoKey:
				ck.prevLeader = i
				Debug(dInfo, "S%d GET:%v false, no key value", i, key)
				return ""
			case ErrWrongLeader:
				continue
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Cid:   ck.cid,
		SeqId: ck.nextSeq,
	}
	ck.nextSeq++

	for i := ck.prevLeader; ; i = (i + 1) % len(ck.servers) {
		reply := GetReply{}
		if ck.servers[i].Call("KVServer.PutAppend", &args, &reply) {
			switch reply.Err {
			case OK:
				ck.prevLeader = i
				Debug(dInfo, "S%d OP:%v done, Key:%v Value:%v", i, op, key, value)
				return
			case ErrWrongLeader:
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
