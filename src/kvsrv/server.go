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
	data  map[string]string
	cache map[int64]record
}

type record struct {
	id    int64
	value string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	// ci := args.ClientID
	// ri := args.ReqID

	// if v, ok := kv.cache[ci]; ok && v.id == ri {
	// 	reply.Value = v.value
	// 	return
	// }

	v, ok := kv.data[key]

	if ok {
		reply.Value = v
	}
	// kv.cache[ci] = record{ri, reply.Value}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := args.Value
	ci := args.ClientID
	ri := args.ReqID

	if v, ok := kv.cache[ci]; ok && ri == v.id {
		reply.Value = v.value
		return
	}
	kv.data[key] = value
	kv.cache[ci] = record{ri, reply.Value}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := args.Value
	ci := args.ClientID
	ri := args.ReqID

	if v, ok := kv.cache[ci]; ok && ri == v.id {
		reply.Value = v.value
		return
	}

	v, ok := kv.data[key]

	if ok {
		reply.Value = v
	}

	kv.data[key] = reply.Value + value
	kv.cache[ci] = record{ri, reply.Value}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.cache = make(map[int64]record)
	return kv
}
