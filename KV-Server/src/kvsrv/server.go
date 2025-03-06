package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type LastProcessedRequest struct {
	SequenceNumber int
	Result         string
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	memory           map[string]string
	clientRequestMap map[int64]LastProcessedRequest
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.memory[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastReq, exists := kv.clientRequestMap[args.ClientID]
	if exists && lastReq.SequenceNumber >= args.SequenceNumber {
		return
	}

	kv.memory[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastReq, exists := kv.clientRequestMap[args.ClientID]
	if exists && lastReq.SequenceNumber >= args.SequenceNumber {
		reply.Value = lastReq.Result
		return
	}

	key := args.Key
	value := args.Value
	reply.Value = kv.memory[key]
	kv.memory[key] += value

	kv.clientRequestMap[args.ClientID] = LastProcessedRequest{
		SequenceNumber: args.SequenceNumber,
		Result:         reply.Value,
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.memory = make(map[string]string)
	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.clientRequestMap = make(map[int64]LastProcessedRequest) // Initialize map

	return kv
}
