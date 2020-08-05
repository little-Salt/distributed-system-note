package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

const commitTimeout time.Duration = 2 * time.Second

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	getOp    = "Get"
	putOp    = "Put"
	appendOp = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId   int64
	OpSeqNum  int
	Operation string
	Key       string
	Val       string
}

type Result struct {
	Seq int
	Err Err
	Val string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastAppliedIndex int

	kvStore    map[string]string
	chStore    map[int64]chan Result
	clerkStore map[int64]Result
}

func (kv *KVServer) commitOp(op Op) (val string, err Err) {
	_, isLeader := kv.rf.GetState()

	// reduce duplicate log
	if !isLeader {
		DPrintf("KVServer %d: not current leader.", kv.me)
		err = ErrWrongLeader
		return
	} else {
		kv.mu.Lock()
		lastRes, ok := kv.clerkStore[op.ClerkId]
		kv.mu.Unlock()
		if ok && op.OpSeqNum <= lastRes.Seq {
			val, err = lastRes.Val, lastRes.Err
			return
		}
	}

	index, term, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.mu.Lock()
		DPrintf("KVServer %d: try commit op: %+v, with index: %d, term %d.", kv.me, op, index, term)
		resultCh, ok := kv.chStore[op.ClerkId]
		if !ok {
			resultCh = make(chan Result)
			kv.chStore[op.ClerkId] = resultCh
		}
		kv.mu.Unlock()

		select {
		case res := <-resultCh:
			DPrintf("KVServer %d: finish op: %+v, with index: %d, term %d.", kv.me, op, index, term)
			if res.Seq == op.OpSeqNum {
				val, err = res.Val, res.Err
			}
		case <-time.After(commitTimeout):
			DPrintf("KVServer %d: commit Timeout, request resend op", kv.me)
			err = ErrTimout
		}

	} else {
		DPrintf("KVServer %d: not current leader.", kv.me)
		err = ErrWrongLeader
	}

	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	getOp := Op{ClerkId: args.ClerkId, OpSeqNum: args.OpSeqNum, Operation: getOp, Key: args.Key}
	reply.Value, reply.Err = kv.commitOp(getOp)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	putAppendOp := Op{ClerkId: args.ClerkId, OpSeqNum: args.OpSeqNum, Operation: args.Op, Key: args.Key, Val: args.Value}
	_, reply.Err = kv.commitOp(putAppendOp)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readSnapshot() {
	data := kv.rf.GetSnapShot()

	if data == nil {

	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvStore map[string]string
	var clerkStore map[int64]Result

	if d.Decode(&kvStore) != nil || d.Decode(&clerkStore) != nil {

	} else {
		kv.kvStore = kvStore
		kv.clerkStore = clerkStore
	}
}

func (kv *KVServer) run() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.applyMsg(&msg)
	}
}

func (kv *KVServer) applyMsg(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("KVServer %d: try apply msg:%+v.", kv.me, *msg)
	index, term := msg.CommandIndex, msg.CommandTerm
	if msg.CommandValid {
		op := msg.Command.(Op)
		DPrintf("KVServer %d: try apply op: %+v, with index: %d, term %d.", kv.me, op, index, term)
		kv.applyOp(op)

		if kv.maxraftstate != -1 && kv.rf.GetPersistSize() > kv.maxraftstate {
			data := kv.takeSnapshot()
			go kv.rf.TakeSnapshot(index, data)
		}

	} else {
		DPrintf("KVServer %d: try read snapshot: index: %d, term %d.", kv.me, index, term)
		kv.readSnapshot()
	}
	if index > kv.lastAppliedIndex {
		kv.lastAppliedIndex = index
	}

}

func (kv *KVServer) sendResult(clerkId int64, res Result) {
	ch, ok := kv.chStore[clerkId]
	if ok {
		select {
		case ch <- res:
			DPrintf("KVServer %d: send result.", kv.me)
		default:
		}
	}
}

func (kv *KVServer) applyOp(op Op) {
	res := Result{Seq: op.OpSeqNum, Err: OK}
	lastRes, ok := kv.clerkStore[op.ClerkId]
	opSeq := op.OpSeqNum
	if !ok || lastRes.Seq < opSeq {
		switch op.Operation {
		case getOp:
			_, ok := kv.kvStore[op.Key]
			if !ok {
				res.Err = ErrNoKey
			}
		case putOp:
			kv.kvStore[op.Key] = op.Val
		case appendOp:
			kv.kvStore[op.Key] += op.Val
		default:
			// nothing
		}
		res.Val = kv.kvStore[op.Key]
		kv.clerkStore[op.ClerkId] = res
		DPrintf("KVServer %d: apply op: %+v, res:%v", kv.me, op, res)
	} else {
		res.Err = lastRes.Err
		res.Val = lastRes.Val
	}

	kv.sendResult(op.ClerkId, res)
}

func (kv *KVServer) takeSnapshot() []byte {
	// serialize-deserialize
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.clerkStore)
	return w.Bytes()
}

//
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
//
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
	kv.kvStore = make(map[string]string)
	kv.chStore = make(map[int64]chan Result)
	kv.clerkStore = make(map[int64]Result)

	go kv.run()

	return kv
}
