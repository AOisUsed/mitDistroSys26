package shardgrp

import (
	"bytes"
	"log"
	"sync"

	"6.5840/debug"
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type LastRequest struct {
	RequestId uint64
	Reply     any // for this KVServer, this is either rpc.GetReply or rpc.PutReply.
}

type SnapshotData struct {
	Kvm               map[string]*VersionedValue
	LastReqByClientId map[uint64]*LastRequest
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	kvm               map[string]*VersionedValue
	lastReqByClientId map[uint64]*LastRequest
	rwMu              sync.RWMutex
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here

	var reply any
	switch typedReq := req.(type) {
	case rpc.GetArgs:
		// deduplicate request
		kv.rwMu.RLock()
		lastReq, exists := kv.lastReqByClientId[typedReq.ClientId]
		kv.rwMu.RUnlock()
		if exists {
			// if the request has been executed, simply return the result and not execute it again.
			if typedReq.RequestId <= lastReq.RequestId {
				// note that typedReq.reqId < lastReq.reqId condition should not occur if client functions correctly
				// because requestId from the client shouldn't increase if the pending request wasn't successfully handled by server
				if typedReq.RequestId < lastReq.RequestId {
					debug.D5APrintf("server%v:client %v sending request with impossible id:%v, as lastReq id: %v!\n", kv.me, typedReq.ClientId, typedReq.RequestId, lastReq.RequestId)
				}
				debug.D5APrintf("server%v:client %v sending duplicated request:%v\n", kv.me, typedReq.ClientId, typedReq.RequestId)
				return lastReq.Reply // if client bugs (i.e. sending request of the id smaller than lastReq), server is not responsible for sending the correct reply, and will just send reply of lastReq (which doesn't reflect the truth).
			}
		}
		// execute the operation
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
		//debug.D5APrintf("server%v: DoOp: Get req %v, reply:%v \n", kv.me, req, reply)
		// Save to LastReqByClientId
		kv.rwMu.Lock()
		kv.lastReqByClientId[typedReq.ClientId] = &LastRequest{
			RequestId: typedReq.RequestId,
			Reply:     reply,
		}
		kv.rwMu.Unlock()
		//debug.D5APrintf("server%v: saved LastReqByClientId {%v:%v}", kv.me, typedReq.ClientId, reply)
	case rpc.PutArgs:
		// deduplicate request
		kv.rwMu.RLock()
		lastReq, exists := kv.lastReqByClientId[typedReq.ClientId]
		kv.rwMu.RUnlock()
		if exists {
			// if the request has been executed, simply return the result and not execute it again.
			if typedReq.RequestId <= lastReq.RequestId {
				// note that typedReq.Id < lastReq.Id condition should not occur if client functions correctly
				// because requestId from the client shouldn't increase if the pending request wasn't successfully handled by server
				if typedReq.RequestId < lastReq.RequestId {
					debug.D5APrintf("server%v:client %v sending request with impossible id:%v!\n", kv.me, typedReq.ClientId, typedReq.RequestId)
				}
				debug.D5APrintf("server%v:client %v sending duplicated request:%v\n", kv.me, typedReq.ClientId, typedReq.RequestId)
				return lastReq.Reply // if client bugs (i.e. sending request of the id smaller than lastReq), server is not responsible for sending the correct reply, and will just send reply of lastReq (which doesn't reflect the truth).
			}
		}
		// execute the operation
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
		//debug.D5APrintf("server%v: DoOp: Put req %v, reply:%v \n", kv.me, req, reply)
		// Save to LastReqByClientId
		kv.lastReqByClientId[typedReq.ClientId] = &LastRequest{
			RequestId: typedReq.RequestId,
			Reply:     reply,
		}
		//debug.D5APrintf("server%v: saved LastReqByClientId {%v:%v}", kv.me, typedReq.ClientId, reply)
	default:
		debug.D5APrintf("DoOp: Unknown req type %T\n", req)
		reply = nil // it's neither Get, nor Put.
	}
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

	//debug.D5APrintf("server%v: Snapshot()\n", kv.me)
	// make a copy of kvm
	kv.rwMu.RLock()
	var kvm = make(map[string]*VersionedValue)
	for k, v := range kv.kvm {
		copiedV := &VersionedValue{
			Value:   v.Value,
			Version: v.Version,
		}
		kvm[k] = copiedV
	}

	// make a copy of LastReqByClientId
	var lastReqByClientId = make(map[uint64]*LastRequest)
	for clientId, lastRequest := range kv.lastReqByClientId {
		copiedReq := &LastRequest{
			RequestId: lastRequest.RequestId,
			Reply:     lastRequest.Reply,
		}
		lastReqByClientId[clientId] = copiedReq
	}
	kv.rwMu.RUnlock()

	// snapshot
	snapshot := SnapshotData{
		Kvm:               kvm,
		LastReqByClientId: lastReqByClientId,
	}

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
	if data == nil || len(data) < 1 {
		return
	}

	//debug.D5APrintf("server%v: Restore()\n", kv.me)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot SnapshotData
	err := d.Decode(&snapshot)
	if err != nil {
		log.Fatal(err)
		return
	}

	debug.D5APrintf("server%v: Restored from snapshot.\n", kv.me)

	//var sb strings.Builder
	//for k, v := range kvm {
	//	s := fmt.Sprintf("%v-%v\t", k, v)
	//	sb.WriteString(s)
	//}

	//debug.D5APrintf("server%v: Restored from snapshot:%v\n", kv.me, sb.String())

	kv.rwMu.Lock()
	defer kv.rwMu.Unlock()
	kv.kvm = snapshot.Kvm
	kv.lastReqByClientId = snapshot.LastReqByClientId
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

	debug.D5APrintf("server%v:Get %v, reply: %v, %v, %v \n", kv.me, args.Key, reply.Value, reply.Version, reply.Err)
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
	debug.D5APrintf("server%v: Put %v:%v,version:%v, reply: %v \n", kv.me, args.Key, args.Value, args.Version, reply.Err)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	kv.rsm.Submit(*args)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// only make kvm when it's nil because after reboot, its Kvm will be restored from raft snapshot (state machine),
	// and it should not be overwritten.
	if kv.kvm == nil {
		kv.kvm = make(map[string]*VersionedValue)
		kv.lastReqByClientId = make(map[uint64]*LastRequest)
	}

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
