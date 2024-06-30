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

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data      map[string]string
	requestID map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	reply.Value = kv.data[key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msgID, ok := kv.requestID[args.ClerkID]; ok && msgID == args.MsgID {
		return
	}
	kv.requestID[args.ClerkID] = args.MsgID
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msgID, ok := kv.requestID[args.ClerkID]; ok && msgID == args.MsgID {
		return
	}
	kv.requestID[args.ClerkID] = args.MsgID
	reply.Value = kv.data[args.Key]
	kv.data[args.Key] = reply.Value + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.requestID = make(map[int64]int64)
	return kv
}
