package kvraft

import (
	"Fault-Tolerant-KV-Server/labgob"
	"Fault-Tolerant-KV-Server/labrpc"
	"Fault-Tolerant-KV-Server/raft"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation      string // "Get", "Put", or "Append"
	Key            string // The key associated with the operation
	Value          string // The value for "Put" or "Append" operations (empty for "Get")
	ClientID       int64  // A unique identifier for the client
	SequenceNumber int    // A sequence number to detect duplicate requests
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister

	// Your definitions here.
	db             map[string]string  // Key/value database
	duplicateTable map[int64]int      // Tracks the last processed SequenceNumber for each ClientID
	resultCh       map[string]chan Op // Tracks channels for waiting RPC responses

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, _ = DPrintf("fn (Get): key = %v, value = %v", args.Key, reply.Value)

	kv.mu.Lock()
	if lastSeq, ok := kv.duplicateTable[args.ClientID]; ok && lastSeq >= args.SequenceNumber {
		_, _ = DPrintf("fn (Get) already compleated OP: = %v, value = %v", args.Key, kv.db[args.Key])
		reply.Value = kv.db[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operation:      "Get",
		Key:            args.Key,
		ClientID:       args.ClientID,
		SequenceNumber: args.SequenceNumber,
	}

	// Propose the operation to Raft
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		_, _ = DPrintf("fn (Get): raft isn't leader")
		reply.Err = ErrWrongLeader
		return
	}

	// Create a channel to wait for the result
	key := fmt.Sprintf("%v%v", args.ClientID, args.SequenceNumber)
	kv.mu.Lock()
	if _, exists := kv.resultCh[key]; !exists {
		kv.resultCh[key] = make(chan Op, 1)
	}
	ch := kv.resultCh[key]
	kv.mu.Unlock()

	// Wait for the result or timeout
	select {
	case <-time.After(500 * time.Millisecond): // Timeout
		reply.Err = ErrTimeout
	case committedOp, ok := <-ch:
		if ok {
			if committedOp.ClientID == args.ClientID && committedOp.SequenceNumber == args.SequenceNumber {
				kv.mu.Lock()
				value, isInDB := kv.db[args.Key]
				kv.mu.Unlock()
				if isInDB {
					reply.Value = value
					reply.Err = OK
				} else {
					reply.Value = ""
					reply.Err = ErrNoKey
				}
			} else {
				reply.Err = ErrWrongLeader
			}
		} else {
			// droebit ase iyos
			reply.Err = ErrTimeout
		}
	}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, _ = DPrintf("fn (Put): key = %v, value = %v", args.Key, args.Value)
	kv.mu.Lock()
	if lastSeq, ok := kv.duplicateTable[args.ClientID]; ok && lastSeq >= args.SequenceNumber {
		// Already processed this request
		_, _ = DPrintf("fn (Put) already compleated OP: = %v, value = %v", args.Key, kv.db[args.Key])
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operation:      "Put",
		Key:            args.Key,
		Value:          args.Value,
		ClientID:       args.ClientID,
		SequenceNumber: args.SequenceNumber,
	}

	_, _, isLeader := kv.rf.Start(op)
	reply.Err = OK
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Create a channel to wait for the result
	key := fmt.Sprintf("%v%v", args.ClientID, args.SequenceNumber)
	kv.mu.Lock()
	_, _ = DPrintf("fn (Put): key = %v, value = %v, state = locked", args.Key, args.Value)
	if _, exists := kv.resultCh[key]; !exists {
		kv.resultCh[key] = make(chan Op, 1)
	}
	ch := kv.resultCh[key]
	kv.mu.Unlock()

	// Wait for the result or timeout
	select {
	case <-time.After(500 * time.Millisecond): // Timeout
		_, _ = DPrintf("fn (Put): Timeout reading from channel")
		reply.Err = ErrTimeout

	case committedOp, ok := <-ch:
		_, _ = DPrintf("fn (Put): Reading from channel, committedOp.key = %v, committedOp.value = %v",
			committedOp.Key, committedOp.Value)
		if ok {
			if committedOp.ClientID == args.ClientID && committedOp.SequenceNumber == args.SequenceNumber {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
		} else {
			reply.Err = ErrTimeout
		}
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//_, _ = DPrintf("fn (Append): key = %v, value = %v", args.Key, args.Value)
	kv.mu.Lock()
	if lastSeq, ok := kv.duplicateTable[args.ClientID]; ok && lastSeq >= args.SequenceNumber {
		_, _ = DPrintf("fn (Append) already compleated OP: key = %v, value = %v", args.Key, args.Value)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Operation:      "Append",
		Key:            args.Key,
		Value:          args.Value,
		ClientID:       args.ClientID,
		SequenceNumber: args.SequenceNumber,
	}

	// Propose the operation to Raft
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Create a channel to wait for the result
	key := fmt.Sprintf("%v%v", args.ClientID, args.SequenceNumber)
	kv.mu.Lock()
	if _, exists := kv.resultCh[key]; !exists {
		kv.resultCh[key] = make(chan Op, 1)
	}
	ch := kv.resultCh[key]
	kv.mu.Unlock()

	// Wait for the result or timeout
	select {
	case committedOp, ok := <-ch:
		if ok {
			if committedOp.ClientID == args.ClientID && committedOp.SequenceNumber == args.SequenceNumber {
				reply.Err = OK
			} else {
				reply.Err = ErrWrongLeader
			}
		} else {
			reply.Err = ErrTimeout
		}
	case <-time.After(500 * time.Millisecond): // Timeout
		reply.Err = ErrTimeout
	}

}

func (kv *KVServer) processApplyCh() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				key := fmt.Sprintf("%v%v", op.ClientID, op.SequenceNumber)
				kv.applyOperation(op, key)

				// (Part B)
				if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
					kv.createSnapshot(msg.CommandIndex)
				}
			} else if msg.SnapshotValid {
				// (Part B)
				kv.applySnapshot(msg.Snapshot)
			}
		}
	}
}

func (kv *KVServer) applyOperation(op Op, index string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if lastSeq, ok := kv.duplicateTable[op.ClientID]; ok && op.SequenceNumber <= lastSeq {
		return
	}

	// Apply the operation
	switch op.Operation {
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
	case "Get":
		// Get does not modify the database
	}

	// Update the duplicate table with the latest sequence number
	kv.duplicateTable[op.ClientID] = op.SequenceNumber

	// Notify waiting RPC handlers (if any)
	if ch, ok := kv.resultCh[index]; ok {
		select {
		case ch <- op:
		default:
		}
		close(ch)
		delete(kv.resultCh, index)
	}
}

// Kill the tester calls Kill() when a KVServer instance won't
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

func (kv *KVServer) applySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var duplicateTable map[int64]int

	if d.Decode(&db) != nil || d.Decode(&duplicateTable) != nil {
		panic("Failed to decode snapshot")
	} else {
		kv.db = db
		kv.duplicateTable = duplicateTable
	}
}

func (kv *KVServer) createSnapshot(lastApplied int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Serialize state
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(kv.db)
	_ = e.Encode(kv.duplicateTable)
	snapshot := w.Bytes()

	// Notify Raft
	kv.rf.Snapshot(lastApplied, snapshot)
}

// StartKVServer servers[] contains the ports of the set of
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
	kv.mu = sync.Mutex{}

	kv.db = make(map[string]string)
	kv.duplicateTable = make(map[int64]int)
	kv.resultCh = make(map[string]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.applySnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.processApplyCh()

	return kv
}
