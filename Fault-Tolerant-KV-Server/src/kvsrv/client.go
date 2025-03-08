package kvsrv

import (
	"Fault-Tolerant-KV-Server/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID       int64
	SequenceNumber int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.ClientID = nrand()
	ck.SequenceNumber = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:            key,
		ClientID:       ck.ClientID,
		SequenceNumber: ck.SequenceNumber,
	}
	reply := GetReply{}
	ck.SequenceNumber++

	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			// If we get a reply, return the value
			return reply.Value
		}
		// If not successful, wait for a while and then retry
		time.Sleep(10 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		Key:            key,
		Value:          value,
		ClientID:       ck.ClientID,
		SequenceNumber: ck.SequenceNumber,
	}
	reply := PutAppendReply{}
	ck.SequenceNumber++

	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			// If we get a reply, return the value
			return reply.Value
		}
		// If not successful, wait for a while and then retry
		time.Sleep(10 * time.Millisecond) // Introduce a small delay before retrying
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
