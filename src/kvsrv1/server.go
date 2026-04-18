package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvm map[string]*VersionedValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvm = make(map[string]*VersionedValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	*reply = rpc.GetReply{}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exits := kv.kvm[args.Key]; !exits {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = kv.kvm[args.Key].Value
	reply.Version = kv.kvm[args.Key].Version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, exists := kv.kvm[args.Key]; exists { //has the key
		if v.Version == args.Version { //version matches
			kv.kvm[args.Key].Value = args.Value
			kv.kvm[args.Key].Version += 1
			reply.Err = rpc.OK
		} else { //has the key, version doesn't match
			reply.Err = rpc.ErrVersion
		}
	} else { //doesn't have the key
		if args.Version == 0 {
			kv.kvm[args.Key] = &VersionedValue{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else { //doesn't have the key, and the version isn't correct either
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
