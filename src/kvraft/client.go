package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	requestId int64
	leaderId  int
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
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderId = 0
	return ck
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
	// You will have to modify this function.
	rid := atomic.AddInt64(&ck.requestId, 1)
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: rid}
	i := 0
	for {
		serverId := (ck.leaderId + i) % len(ck.servers)
		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				return ""
			}
		}
		i++
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
	// You will have to modify this function.
	rid := atomic.AddInt64(&ck.requestId, 1)
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: rid}
	i := 0
	for {
		serverId := (ck.leaderId + i) % len(ck.servers)
		reply := PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			}
		}
		i = (i + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
