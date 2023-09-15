package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// clerk struct in server, for convenience
// tracks the operation.
type ClerkOps struct {
	seqId       int // clerk current seq id
	getCh       chan Op
	putAppendCh chan Op
	msgUniqueId int // uid for rpc waiting msg
}

// kvserver send to raft-library, the argument of Start method.
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Name  string // operation name: "Get", "Put", "Append"

	ClerkId   int64
	CommandId int
	ServerId  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataSource           map[string]string
	dispatcher           map[int]chan Notification
	lastAppliedCommandId map[int64]int // key:clerkId value:commandId, identify a unique command

	appliedRaftLogIndex int
}

func (kv *KVServer) needTakeSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}

	if kv.rf.GetRaftStateSize() >= kv.maxraftstate {
		return true
	}
	return false
}

func (kv *KVServer) takeSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.dataSource)
	e.Encode(kv.lastAppliedCommandId)
	appliedRaftLogIndex := kv.appliedRaftLogIndex
	kv.mu.Unlock()

	kv.rf.Snapshot(appliedRaftLogIndex, w.Bytes())
}

type Notification struct {
	ClerkId   int64
	CommandId int
}

// is duplicate command in clerk: clearId
func (kv *KVServer) isDuplicateCommand(clerkId int64, commandId int) bool {
	appliedCommandId, ok := kv.lastAppliedCommandId[clerkId]
	if !ok || commandId > appliedCommandId {
		return false
	}
	return true
}

// return wrongleader
func (kv *KVServer) waitApplying(op Op, timeout time.Duration) Err {
	// return common part of GetReply and PutAppendReply
	//i.e, WrongLeader
	// send command to raft-library, revoke Start.
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader
	}

	if kv.needTakeSnapshot() {
		kv.takeSnapshot()
	}

	kv.mu.Lock()
	if _, ok := kv.dispatcher[index]; !ok {
		// if in index no notification, create new
		//fmt.Printf("new notification in index=%d\n", index)
		kv.dispatcher[index] = make(chan Notification, 1)
	}
	ch := kv.dispatcher[index]
	kv.mu.Unlock()

	var err Err

	select {
	case notify := <-ch:
		//fmt.Printf("notify=%v, op.clerk=%d, op.commandid=%d\n", notify, op.ClerkId, op.CommandId)
		if notify.ClerkId != op.ClerkId || notify.CommandId != op.CommandId {
			// leader has changed
			err = ErrWrongLeader
		} else {
			err = OK
		}
	case <-time.After(timeout):
		kv.mu.Lock()
		if kv.isDuplicateCommand(op.ClerkId, op.CommandId) {
			err = OK
		} else {
			err = ErrWrongLeader
		}
		kv.mu.Unlock()
	}

	kv.mu.Lock()
	delete(kv.dispatcher, index)
	kv.mu.Unlock()
	return err
}

//
// args: client send to kv, reply: kv reply to client
//
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:       args.Key,
		Name:      "Get",
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		ServerId:  kv.me,
	}

	// wait for raft being applied
	// or leader changed (log is override, and never gets applied)
	reply.Err = kv.waitApplying(op, 500*time.Millisecond)

	if reply.Err != ErrWrongLeader {
		kv.mu.Lock()
		value, ok := kv.dataSource[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Value = value
			return
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Name:      args.Op,
		ClerkId:   args.ClerkId,
		CommandId: args.CommandId,
		ServerId:  kv.me,
	}

	reply.Err = kv.waitApplying(op, 500*time.Millisecond)

	//fmt.Printf("time=%v, args=%v, wrongleader=%v.\n", time.Now(), args, reply.WrongLeader)
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

	kv.dataSource = make(map[string]string)
	kv.dispatcher = make(map[int]chan Notification)
	kv.lastAppliedCommandId = make(map[int64]int)

	// recover from snapshot
	snapshot := persister.ReadSnapshot()
	kv.installSnapshot(snapshot)

	// You may need initialization code here.
	go func() {
		// range channel will wait for channel closed.
		for msg := range kv.applyCh {
			// if command not valid
			if !msg.CommandValid && !msg.SnapshotValid {
				continue
			}

			if msg.SnapshotValid {
				kv.installSnapshot(msg.Snapshot)
				continue
			}

			op := msg.Command.(Op) // parse command to Op
			kv.mu.Lock()
			if kv.isDuplicateCommand(op.ClerkId, op.CommandId) {
				kv.mu.Unlock()
				continue
			}

			switch op.Name {
			case "Put":
				kv.dataSource[op.Key] = op.Value
			case "Append":
				kv.dataSource[op.Key] += op.Value
			}
			kv.lastAppliedCommandId[op.ClerkId] = op.CommandId
			kv.appliedRaftLogIndex = msg.SnapshotIndex

			if ch, ok := kv.dispatcher[msg.CommandIndex]; ok {
				notify := Notification{
					ClerkId:   op.ClerkId,
					CommandId: op.CommandId,
				}
				ch <- notify
			}

			kv.mu.Unlock()
		}
	}()
	return kv
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot != nil {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		if d.Decode(&kv.dataSource) != nil ||
			d.Decode(&kv.lastAppliedCommandId) != nil {
			fmt.Printf("recover snapshot error.")
		}
	}
}
