package kvraft

import (
	"bytes"
	"log"
	"sync"

	"6.5840/debug"
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	kvm  map[string]*VersionedValue
	rwMu sync.RWMutex
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here

	var reply any
	switch typedReq := req.(type) {
	case rpc.GetArgs:
		kv.rwMu.RLock()
		verVal, exists := kv.kvm[typedReq.Key]
		kv.rwMu.RUnlock()
		if !exists {
			reply = rpc.GetReply{
				Err: rpc.ErrNoKey,
			}
		} else {
			reply = rpc.GetReply{
				Value:   verVal.Value,
				Version: verVal.Version,
				Err:     rpc.OK,
			}
		}
		debug.D4BPrintf("server%v: DoOp: Get req %v, reply:%v \n", kv.me, req, reply)
	case rpc.PutArgs:
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()
		if v, exists := kv.kvm[typedReq.Key]; exists {
			if v.Version == typedReq.Version { //version matches
				kv.kvm[typedReq.Key].Value = typedReq.Value
				kv.kvm[typedReq.Key].Version += 1
				reply = rpc.PutReply{
					Err: rpc.OK,
				}
			} else { //has the key, version doesn't match
				reply = rpc.PutReply{
					Err: rpc.ErrVersion,
				}
			}
		} else {
			if typedReq.Version == 0 {
				kv.kvm[typedReq.Key] = &VersionedValue{Value: typedReq.Value, Version: 1}
				reply = rpc.PutReply{
					Err: rpc.OK,
				}
			} else { //doesn't have the key, and the version isn't correct either
				reply = rpc.PutReply{
					Err: rpc.ErrNoKey,
				}
			}
		}
		debug.D4BPrintf("server%v: DoOp: Put req %v, reply:%v \n", kv.me, req, reply)
	default:
		debug.D4BPrintf("DoOp: Unknown req type %T\n", req)
		reply = nil // it's neither Get, nor Put.
	}
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

	//debug.D4CPrintf("server%v: Snapshot()\n", kv.me)
	// make a copy of kv
	kv.rwMu.RLock()
	var snapshot = make(map[string]*VersionedValue)
	for k, v := range kv.kvm {
		copiedV := &VersionedValue{
			Value:   v.Value,
			Version: v.Version,
		}
		snapshot[k] = copiedV
	}
	kv.rwMu.RUnlock()

	// encode copied snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(snapshot)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if data == nil || len(data) < 1 {
		return
	}

	//debug.D4CPrintf("server%v: Restore()\n", kv.me)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvm map[string]*VersionedValue
	err := d.Decode(&kvm)
	if err != nil {
		log.Fatal(err)
		return
	}

	debug.D4CPrintf("server%v: Restored from snapshot.\n", kv.me)

	//var sb strings.Builder
	//for k, v := range kvm {
	//	s := fmt.Sprintf("%v-%v\t", k, v)
	//	sb.WriteString(s)
	//}

	//debug.D4CPrintf("server%v: Restored from snapshot:%v\n", kv.me, sb.String())

	kv.rwMu.Lock()
	defer kv.rwMu.Unlock()
	kv.kvm = kvm
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm used DoOp(), just use the result DoOp() returns
		*reply = rep.(rpc.GetReply)
	} else { // if rpc isn't ok, meaning this server is not the leader,
		reply.Err = rpcErr
	}

	debug.D4BPrintf("server%v:Get %v, reply: %v, %v, %v \n", kv.me, args.Key, reply.Value, reply.Version, reply.Err)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	//log.Printf("server %v Put called", kv.me)
	//defer log.Printf("server %v Put done", kv.me)

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm used DoOp(), just use the result DoOp() returns
		*reply = rep.(rpc.PutReply)
	} else { // if rpc isn't ok, meaning this server is not the leader,
		reply.Err = rpcErr
	}
	debug.D4BPrintf("server%v: Put %v:%v,version:%v, reply: %v \n", kv.me, args.Key, args.Value, args.Version, reply.Err)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.

	// only make kvm when it's nil because after reboot, its kvm will be restored from raft snapshot (state machine),
	// and it should not be overwritten.
	if kv.kvm == nil {
		kv.kvm = make(map[string]*VersionedValue)
	}
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
