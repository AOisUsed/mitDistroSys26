package kvraft

import (
	"bytes"
	"log"
	"sync"

	"kvstore/debug"
	"kvstore/kvraft/rsm"
	"kvstore/kvsrv/rpcapi"
	"kvstore/labgob"
	"kvstore/rpc"
	"kvstore/tester"
)

type VersionedValue struct {
	Value   string
	Version rpcapi.Tversion
}

type LastPut struct {
	RequestId uint64
	Reply     rpcapi.PutReply // for this KVServer, this is rpcapi.PutReply.
}

type SnapshotData struct {
	Kvm               map[string]*VersionedValue
	LastPutByClientId map[uint64]*LastPut
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	kvm               map[string]*VersionedValue
	lastPutByClientId map[uint64]*LastPut
	rwMu              sync.RWMutex
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
	case rpcapi.GetArgs:
		// execute the operation
		kv.rwMu.RLock()
		verVal, exists := kv.kvm[typedReq.Key]
		kv.rwMu.RUnlock()
		if !exists {
			reply = rpcapi.GetReply{
				Err: rpcapi.ErrNoKey,
			}
		} else {
			reply = rpcapi.GetReply{
				Value:   verVal.Value,
				Version: verVal.Version,
				Err:     rpcapi.OK,
			}
		}
		debug.D4BPrintf("server%v: DoOp: Get req %v, reply:%v \n", kv.me, req, reply)
	case rpcapi.PutArgs:
		// deduplicate request
		kv.rwMu.RLock()
		lastPut, exists := kv.lastPutByClientId[typedReq.ClientId]
		kv.rwMu.RUnlock()
		if exists {
			// if the request has been executed, simply return the result and not execute it again.
			if typedReq.RequestId <= lastPut.RequestId {
				// Note: if request.Id < lastPut.Id, the server will return inaccurate deduplication info.
				// 		This happens because the dedup data for this request has already been overwritten by a
				// 		request with a greater ID, but the server still replies with lastPut's result.
				//
				// 		Therefore, the client MUST serialise PUT requests — never increment requestId before
				// 		receiving the result of the previous PUT.
				//
				// Design rationale (minimalist approach):
				//
				// To correctly handle a single client's concurrent PUTs over an unreliable network,
				// the server would need to retain all dedup data until the client explicitly ACKs:
				// "all requests before ID xxx have been received, you may safely discard older dedup data."
				// This would require an ACK mechanism (e.g., including ACK info on each PUT request),
				// as well as a deadline for garbage collection in case the client doesn't ACK (due to network or the termination of requesting).
				//
				// The main problem is that such a deadline must be tuned to network conditions,
				// making it environment-dependent. For simplicity and universality,
				// I opt for this design instead.

				if typedReq.RequestId < lastPut.RequestId {
					debug.D4BPrintf("server%v:client %v sent new PUT request  id: %v) before receiving previous result!\n", kv.me, typedReq.ClientId, typedReq.RequestId)
				}
				debug.D4BPrintf("server%v:client %v sent duplicated request:%v\n", kv.me, typedReq.ClientId, typedReq.RequestId)
				return lastPut.Reply // if client bugs (i.e. sending request of the id smaller than lastPut), server is not responsible for sending the correct reply, and will just send reply of lastPut (which doesn't reflect the truth).
			}
		}
		// execute the operation
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()
		if v, exists := kv.kvm[typedReq.Key]; exists {
			if v.Version == typedReq.Version { //version matches
				kv.kvm[typedReq.Key].Value = typedReq.Value
				kv.kvm[typedReq.Key].Version += 1
				reply = rpcapi.PutReply{
					Err: rpcapi.OK,
				}
			} else { //has the key, version doesn't match
				reply = rpcapi.PutReply{
					Err: rpcapi.ErrVersion,
				}
			}
		} else {
			if typedReq.Version == 0 {
				kv.kvm[typedReq.Key] = &VersionedValue{Value: typedReq.Value, Version: 1}
				reply = rpcapi.PutReply{
					Err: rpcapi.OK,
				}
			} else { //doesn't have the key, and the version isn't correct either
				reply = rpcapi.PutReply{
					Err: rpcapi.ErrNoKey,
				}
			}
		}
		debug.D4BPrintf("server%v: DoOp: Put req %v, reply:%v \n", kv.me, req, reply)
		// Save to LastPutByClientId
		kv.lastPutByClientId[typedReq.ClientId] = &LastPut{
			RequestId: typedReq.RequestId,
			Reply:     reply.(rpcapi.PutReply),
		}
		debug.D4BPrintf("server%v: saved LastPutByClientId {%v:%v}", kv.me, typedReq.ClientId, reply)
	default:
		debug.D4BPrintf("DoOp: Unknown req type %T\n", req)
		reply = nil // it's neither Get, nor Put.
	}
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

	//debug.D4CPrintf("server%v: Snapshot()\n", kv.me)
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

	// make a copy of LastPutByClientId
	var lastPutByClientId = make(map[uint64]*LastPut)
	for clientId, lastRequest := range kv.lastPutByClientId {
		copiedReq := &LastPut{
			RequestId: lastRequest.RequestId,
			Reply:     lastRequest.Reply,
		}
		lastPutByClientId[clientId] = copiedReq
	}
	kv.rwMu.RUnlock()

	// snapshot
	snapshot := SnapshotData{
		Kvm:               kvm,
		LastPutByClientId: lastPutByClientId,
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

// Restore from snapshot after server crash
func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	//debug.D4CPrintf("server%v: Restore()\n", kv.me)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot SnapshotData
	err := d.Decode(&snapshot)
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
	kv.kvm = snapshot.Kvm
	kv.lastPutByClientId = snapshot.LastPutByClientId
}

func (kv *KVServer) Get(args *rpcapi.GetArgs, reply *rpcapi.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpcapi.OK { // if rpc is ok, meaning rsm used DoOp(), just use the result DoOp() returns
		*reply = rep.(rpcapi.GetReply)
	} else { // if rpc isn't ok, meaning this server is not the leader,
		reply.Err = rpcErr
	}

	debug.D4BPrintf("server%v:Get %v, reply: %v, %v, %v \n", kv.me, args.Key, reply.Value, reply.Version, reply.Err)
}

func (kv *KVServer) Put(args *rpcapi.PutArgs, reply *rpcapi.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	//log.Printf("server %v Put called", kv.me)
	//defer log.Printf("server %v Put done", kv.me)

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpcapi.OK { // if rpc is ok, meaning rsm used DoOp(), just use the result DoOp() returns
		*reply = rep.(rpcapi.PutReply)
	} else { // if rpc isn't ok, meaning this server is not the leader,
		reply.Err = rpcErr
	}
	debug.D4BPrintf("server%v: Put %v:%v,version:%v, reply: %v \n", kv.me, args.Key, args.Value, args.Version, reply.Err)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*rpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call testgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpcapi.PutArgs{})
	labgob.Register(rpcapi.PutReply{})
	labgob.Register(rpcapi.GetArgs{})
	labgob.Register(rpcapi.GetReply{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv, tester.GroupLabel(gid))
	// You may need initialization code here.

	// only make Kvm when it's nil because after reboot, its Kvm will be restored from raft snapshot (state machine),
	// and it should not be overwritten.
	if kv.kvm == nil {
		kv.kvm = make(map[string]*VersionedValue)
		kv.lastPutByClientId = make(map[uint64]*LastPut)
	}
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*rpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
