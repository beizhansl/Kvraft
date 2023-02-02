package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = 1
	PUT    = 2
	APPEND = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	ClientId  int64
	RequestId int64
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	// Your definitions here.
	// The kv database is important!
	DB map[string]string
	// every client's applied request
	FinshedReq map[int64]int64
	// every routine waiting for applyCh
	// using index to distinct
	waitQueue         map[int]chan Op
	lastIncludedIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cid := args.ClientId
	rid := args.RequestId
	key := args.Key
	kv.mu.Lock()
	rd, have := kv.FinshedReq[cid]
	if have {
		// dupicated,just return value and ok!
		if rd >= rid {
			reply.Err = OK
			val, h := kv.DB[key]
			if !h {
				reply.Err = ErrNoKey
			} else {
				reply.Value = val
			}
			kv.mu.Unlock()
			return
		}
	}
	// we should put it to raft!
	command := Op{Type: "Get", ClientId: cid, RequestId: rid, Key: key}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// wait for applyCh-index
	ch, have := kv.waitQueue[index]
	if !have {
		kv.waitQueue[index] = make(chan Op, 1)
		ch = kv.waitQueue[index]
	}
	kv.mu.Unlock()
	GetwaitCh(ch, args, reply)

	kv.mu.Lock()
	ch, have = kv.waitQueue[index]
	if have {
		close(ch)
		delete(kv.waitQueue, index)
	}
	kv.mu.Unlock()
}

func GetwaitCh(ch chan Op, args *GetArgs, reply *GetReply) {
	select {
	case op := <-ch:
		// error ClientId and RequestId
		if op.ClientId != args.ClientId || op.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
			return
		}
		// It is the request!
		if op.Type == ErrNoKey {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = op.Value
		}
		return
	case <-time.After(400 * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}

func PutAppendwaitCh(ch chan Op, args *PutAppendArgs, reply *PutAppendReply) {
	select {
	case op := <-ch:
		// error ClientId and RequestId
		if op.ClientId != args.ClientId || op.RequestId != args.RequestId {
			reply.Err = ErrWrongLeader
			return
		}
		// It is the request!
		reply.Err = OK
		return
	case <-time.After(400 * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cid := args.ClientId
	rid := args.RequestId
	key := args.Key
	kv.mu.Lock()
	rd, have := kv.FinshedReq[cid]
	if have {
		// dupicated,just return value and ok!
		if rd >= rid {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}
	// we should put it to raft!
	command := Op{Type: args.Op, ClientId: cid, RequestId: rid, Key: key, Value: args.Value}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// wait for applyCh-index
	ch, have := kv.waitQueue[index]
	if !have {
		kv.waitQueue[index] = make(chan Op, 1)
		ch = kv.waitQueue[index]
	}
	kv.mu.Unlock()
	PutAppendwaitCh(ch, args, reply)
	kv.mu.Lock()
	ch, have = kv.waitQueue[index]
	if have {
		close(ch)
		delete(kv.waitQueue, index)
	}
	kv.mu.Unlock()
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

func (kv *KVServer) applyLog() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command
				// first update op
				x := op.(Op)
				cid := x.ClientId
				rid := x.RequestId
				// To know if has done
				kv.mu.Lock()
				rd, have := kv.FinshedReq[cid]
				hasDone := false
				if have && rd >= rid {
					hasDone = true
				}
				// To do anything if hasDone is false
				if msg.CommandIndex == kv.lastIncludedIndex+1 {
					kv.lastIncludedIndex = msg.CommandIndex
				}
				if x.Type == "Get" {
					val, ok := kv.DB[x.Key]
					if !ok {
						val = ""
						x.Type = ErrNoKey
					}
					x.Value = val
				} else if x.Type == "Put" && !hasDone {
					kv.DB[x.Key] = x.Value
				} else if x.Type == "Append" && !hasDone {
					val, ok := kv.DB[x.Key]
					if ok {
						val = val + x.Value
					} else {
						val = x.Value
					}
					kv.DB[x.Key] = val
				}
				// If has Ch, answer it！
				ch, have := kv.waitQueue[msg.CommandIndex]
				if have {
					ch <- x
				}
				// update FinshedReq!
				if !hasDone {
					kv.FinshedReq[cid] = rid
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				// SnapShots!
				kv.mu.Lock()
				dbtmp := msg.Snapshot
				// Impossible appear!~
				// if len(dbtmp) == 0 {
				// 	kv.DB = make(map[string]string)
				// 	kv.FinshedReq = make(map[int64]int64)
				// }
				if kv.lastIncludedIndex < msg.SnapshotIndex {
					kv.lastIncludedIndex = msg.SnapshotIndex
					r := bytes.NewBuffer(dbtmp)
					d := labgob.NewDecoder(r)
					if d.Decode(&kv.DB) != nil || d.Decode(&kv.FinshedReq) != nil {
						log.Fatal("ReadPersist Fail!")
					}
				}
				kv.mu.Unlock()
			}
			kv.mu.Lock()
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.DB)
				e.Encode(kv.FinshedReq)
				data := w.Bytes()
				// println(kv.lastIncludedIndex)
				lastIndex := kv.lastIncludedIndex
				kv.mu.Unlock()
				// println(kv.me, "----A")
				kv.rf.Snapshot(lastIndex, data)
				// println(kv.me, "----B")
			} else {
				kv.mu.Unlock()
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store xw through the underlying Raft
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
	kv.DB = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg, 20)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	//Here to decode it!
	//persister.ReadSnapshot()
	kv.waitQueue = make(map[int]chan Op)
	kv.FinshedReq = make(map[int64]int64)

	// You may need initialization code here.
	kv.lastIncludedIndex = 0
	// do while process to get applyLog
	go kv.applyLog()

	return kv
}
