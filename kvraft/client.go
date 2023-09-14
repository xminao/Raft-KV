package kvraft

import (
	"6.824/labrpc"
	"time"

	"crypto/rand"
	"math/big"
)

// use clerkId and commandId to identify a clerk command, to avoid non-linear error.
type Clerk struct {
	servers []*labrpc.ClientEnd // kvservers
	// You will have to modify this struct.
	leaderId      int   // cache kvserver leader id, reduce invalid requests.
	clerkId       int64 // clerk unique id
	lastCommandId int   // ensure that each command has a different identifier and cannot be changed before the previous command is processed.
}

// can replace with snowflake
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
	ck.clerkId = nrand()
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
	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		CommandId: ck.allocCommandId(),
	}
	server := ck.leaderId // kvserver which ck will rpc with.
	for {
		reply := GetReply{}
		ok := ck.SendGet(server%len(ck.servers), &args, &reply)
		if ok {
			// server not leader, change server id, retry
			if reply.Err == ErrWrongLeader {
				server += 1
				continue
			}
			ck.leaderId = server
			return reply.Value
		} else {
			// change server id, retry
			server += 1
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ""
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
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		CommandId: ck.allocCommandId(),
	}
	server := ck.leaderId

	for {
		reply := PutAppendReply{}
		ok := ck.SendPutAppend(server%len(ck.servers), &args, &reply)
		if ok {
			// wrong leader, retry
			if reply.Err == ErrWrongLeader {
				server += 1
				time.Sleep(50 * time.Millisecond)
				continue
			}
			//fmt.Printf("[ClientPutAppend] Clerk=%d send key=%v, value=%v to server=%d", ck.clerkId, key, value, server)
			ck.leaderId = server // cache leader id
			break
		} else {
			server += 1
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// revoke KVServer to handle
func (ck *Clerk) SendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

func (ck *Clerk) SendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// ----------------------- utils --------------------------------
func (ck *Clerk) allocCommandId() int {
	ck.lastCommandId += 1
	return ck.lastCommandId
}
