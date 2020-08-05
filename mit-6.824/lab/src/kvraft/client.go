package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const timeoutRetryInterval = 100 * time.Millisecond
const retryInterval = 10 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id     int64
	seqNum int

	clusterSize int
	leader      int
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

	ck.id = nrand()
	ck.seqNum = 0

	ck.clusterSize = len(servers)
	ck.leader = 0

	return ck
}

func (ck *Clerk) call(api string, args interface{}, reply interface{}) bool {
	return ck.servers[ck.leader].Call(api, args, reply)
}

//
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
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{ClerkId: ck.id, OpSeqNum: ck.seqNum, Key: key}
	reply := GetReply{}
	for {
		DPrintf("Clerk: %d ---- SeqNum: %d: send Get(%s) to Server %d", ck.id, ck.seqNum, key, ck.leader)
		if ck.call("KVServer.Get", &args, &reply) && (reply.Err == OK || reply.Err == ErrNoKey) {
			DPrintf("Clerk: %d ---- SeqNum: %d: recieve reply:%v from Server %d", ck.id, ck.seqNum, reply, ck.leader)
			ck.seqNum++
			return reply.Value
		}
		if reply.Err == ErrTimout {
			time.Sleep(timeoutRetryInterval)
		} else {
			ck.leader = (ck.leader + 1) % ck.clusterSize
			time.Sleep(retryInterval)
		}
		reply = GetReply{}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{ClerkId: ck.id, OpSeqNum: ck.seqNum, Op: op, Key: key, Value: value}
	reply := PutAppendReply{}
	for {
		DPrintf("Clerk: %d ---- SeqNum: %d: send %s(%s, %s) to Server %d", ck.id, ck.seqNum, op, key, value, ck.leader)
		if ck.call("KVServer.PutAppend", &args, &reply) && reply.Err == OK {
			DPrintf("Clerk: %d ---- SeqNum: %d: recieve reply:%v from Server %d", ck.id, ck.seqNum, reply, ck.leader)
			ck.seqNum++
			return
		}
		if reply.Err == ErrTimout {
			time.Sleep(timeoutRetryInterval)
		} else {
			ck.leader = (ck.leader + 1) % ck.clusterSize
			time.Sleep(retryInterval)
		}
		reply = PutAppendReply{}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
