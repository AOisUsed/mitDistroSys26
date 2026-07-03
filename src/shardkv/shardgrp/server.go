package shardgrp

import (
	"bytes"
	"log"
	"sync"

	"kvstore/debug"
	"kvstore/kvraft/rsm"
	"kvstore/kvsrv/rpcapi"
	"kvstore/labgob"
	"kvstore/rpc"
	"kvstore/shardkv/shardcfg"
	"kvstore/shardkv/shardgrp/shardrpc"
	"kvstore/tester"
)

type VersionedValue struct {
	Value   string
	Version rpcapi.Tversion
}

type LastPut struct {
	RequestId uint64
	Reply     rpcapi.PutReply // for this KVServer, this is  rpcapi.PutReply.
}

// shard state machine
type ShardStatus int

const (
	Absent  ShardStatus = iota // shard doesn't exist in this server
	Serving                    // shard is serving requests
	Frozen                     // shard is frozen
)

func statusToString(digit ShardStatus) string {
	switch digit {
	case 0:
		return "Absent"
	case 1:
		return "Serving"
	case 2:
		return "Frozen"
	}
	return "Unknown"
}

type ShardState struct {
	Kv                map[string]*VersionedValue
	LastPutByClientId map[uint64]*LastPut
}

type SnapshotData struct {
	Kvm               [shardcfg.NShards]map[string]*VersionedValue
	LastPutByClientId map[uint64]*LastPut
	CfgNumByShid      map[shardcfg.Tshid]shardcfg.Tnum
	ShardStatuses     [shardcfg.NShards]ShardStatus
	ShardClients      [shardcfg.NShards]map[uint64]struct{}
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// for kv
	kvm               [shardcfg.NShards]map[string]*VersionedValue
	lastPutByClientId map[uint64]*LastPut

	// for shard
	cfgNumByShid  map[shardcfg.Tshid]shardcfg.Tnum
	shardStatuses [shardcfg.NShards]ShardStatus
	shardClients  [shardcfg.NShards]map[uint64]struct{} // record which clients have done operation on the shard

	//rwMutex
	rwMu sync.RWMutex
}

func (kv *KVServer) DoOp(req any) any {

	var reply any
	switch request := req.(type) {

	// == KV == //
	// == Get == //
	case rpcapi.GetArgs:
		// check whether the shard that the key belongs to is frozen
		shid := shardcfg.Key2Shard(request.Key)
		// check whether it's serving for the shard
		if kv.shardStatuses[shid] != Serving {
			reply = rpcapi.GetReply{
				Err: rpcapi.ErrWrongGroup,
			}
			return reply
		}

		// execute the Get operation
		verVal, exists := kv.kvm[shid][request.Key]
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
		//debug.D5APrintf("shardkvserver %v: DoOp: Get req %v, reply:%v \n", kv.me, req, reply)
		//debug.D5APrintf("shardkvserver %v: saved LastPutByClientId {%v:%v}", kv.me, request.ClientId, reply)

	//== Put == //
	case rpcapi.PutArgs:
		// check whether the shard that the key belongs to is frozen
		shid := shardcfg.Key2Shard(request.Key)
		// check whether it's serving for the shard
		if kv.shardStatuses[shid] != Serving {
			reply = rpcapi.PutReply{
				Err: rpcapi.ErrWrongGroup,
			}
			return reply
		}

		// deduplicate request: return saved duplicate PUT reply
		lastPut, exists := kv.lastPutByClientId[request.ClientId]
		if exists {
			// if the request has been executed, simply return the result and not execute it again.
			if request.RequestId <= lastPut.RequestId {
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

				if request.RequestId < lastPut.RequestId {
					debug.D4BPrintf("server%v:client %v sent new PUT request  id: %v) before receiving previous result!\n", kv.me, request.ClientId, request.RequestId)
				}
				debug.D4BPrintf("server%v:client %v sent duplicated request:%v\n", kv.me, request.ClientId, request.RequestId)
				return lastPut.Reply // if client bugs (i.e. sending request of the id smaller than lastPut), server is not responsible for sending the correct reply, and will just send reply of lastPut (which doesn't reflect the truth).
			}
		}

		// execute the Put operation
		kvshard := kv.kvm[shid] // kv map of the respective shard
		if v, exists := kvshard[request.Key]; exists {
			if v.Version == request.Version { //version matches
				kvshard[request.Key].Value = request.Value
				kvshard[request.Key].Version += 1
				reply = rpcapi.PutReply{
					Err: rpcapi.OK,
				}
			} else { //has the key, version doesn't match
				reply = rpcapi.PutReply{
					Err: rpcapi.ErrVersion,
				}
			}
		} else {
			if request.Version == 0 {
				kvshard[request.Key] = &VersionedValue{Value: request.Value, Version: 1}
				reply = rpcapi.PutReply{
					Err: rpcapi.OK,
				}
			} else { //doesn't have the key, and the version isn't correct either
				reply = rpcapi.PutReply{
					Err: rpcapi.ErrNoKey,
				}
			}
		}
		//debug.D5APrintf("shardkvserver %v: DoOp: Put req %v, reply:%v \n", kv.me, req, reply)
		// Save to LastPutByClientId
		kv.lastPutByClientId[request.ClientId] = &LastPut{
			RequestId: request.RequestId,
			Reply:     reply.(rpcapi.PutReply),
		}
		kv.shardClients[shid][request.ClientId] = struct{}{}
	//debug.D5APrintf("shardkvserver %v: saved LastPutByClientId {%v:%v}", kv.me, request.ClientId, reply)

	// == Sharding == //
	// == FreezeShard == //
	case shardrpc.FreezeShardArgs:
		debug.D5APrintf("shardkvserver %v: DoOp Freeze(shard: %v, Num: %v)", kv.me, request.Shard, request.Num)
		shid := request.Shard
		localCfgNum := kv.cfgNumByShid[shid]
		reqCfgNum := request.Num

		if reqCfgNum > localCfgNum {
			switch kv.shardStatuses[shid] {
			case Serving:
				debug.ObserveMigrationPrintf("[G%d] server-%v: 冻结分片(%v), Config #%v, 原状态: %v", kv.gid, kv.me, request.Shard, request.Num, statusToString(kv.shardStatuses[request.Shard]))
				kv.shardStatuses[shid] = Frozen
				kv.cfgNumByShid[shid] = reqCfgNum
				reply = shardrpc.FreezeShardReply{
					State: kv.marshallShardState(shid),
					Num:   reqCfgNum,
					Err:   rpcapi.OK,
				}
			case Absent, Frozen: // illegal operation on this state
				reply = shardrpc.FreezeShardReply{
					State: nil,
					Num:   localCfgNum,
					Err:   rpcapi.ErrIllegalOperation,
				}
			}
		} else if reqCfgNum == localCfgNum {
			switch kv.shardStatuses[shid] {
			case Frozen, Absent:
				reply = shardrpc.FreezeShardReply{
					State: kv.marshallShardState(shid), // 注意， Absent状态时，说明此分片已经在此group中被删除了，此处是空。由于DeleteShard前提是分片接受者已经完成了install， 所以在controller尝试把此空的分片数据装到分片接受者处时，接受者会幂等操作（拒绝），因而不影响正确性
					Num:   reqCfgNum,
					Err:   rpcapi.OK,
				}
			case Serving:
				reply = shardrpc.FreezeShardReply{ // illegal operation on this state
					State: nil,
					Num:   reqCfgNum,
					Err:   rpcapi.ErrIllegalOperation,
				}
			}
		} else { // reqCfgNum < localCfgNum
			reply = shardrpc.FreezeShardReply{
				State: nil,
				Num:   localCfgNum,
				Err:   rpcapi.OK,
			}
		}
		return reply
	// == InstallShard == //
	case shardrpc.InstallShardArgs:
		debug.D5APrintf("shardkvserver %v: DoOp InstallShard(shard: %v, stateSize: %v, Num: %v)", kv.me, request.Shard, len(request.State), request.Num)
		// check config Num
		localCfgNum := kv.cfgNumByShid[request.Shard]
		if request.Num > localCfgNum {
			switch kv.shardStatuses[request.Shard] {
			case Absent:
				debug.ObserveMigrationPrintf("[G%d] server-%v: 安装分片(%v), Config #%v, 原状态: %v", kv.gid, kv.me, request.Shard, request.Num, statusToString(kv.shardStatuses[request.Shard]))
				kv.installShardState(request.Shard, request.State)
				kv.shardStatuses[request.Shard] = Serving
				kv.cfgNumByShid[request.Shard] = request.Num // increment local config num
				reply = shardrpc.InstallShardReply{
					Err: rpcapi.OK,
				}
			case Frozen, Serving:
				reply = shardrpc.InstallShardReply{ // illegal operation on this state
					Err: rpcapi.ErrIllegalOperation,
				}
			}
		} else if request.Num == localCfgNum { // duplicated Install
			switch kv.shardStatuses[request.Shard] {
			case Serving:
				reply = shardrpc.InstallShardReply{
					Err: rpcapi.OK,
				}
			case Absent, Frozen: // illegal state for install
				reply = shardrpc.InstallShardReply{
					Err: rpcapi.ErrIllegalOperation,
				}
			}
		} else { // reqCfgNum < localCfgNum
			reply = shardrpc.InstallShardReply{
				Err: rpcapi.OK,
			}
		}
		return reply

	// == DeleteShard == //
	case shardrpc.DeleteShardArgs:
		debug.D5APrintf("shardkvserver %v: DoOp DeleteShard(shard: %v, Num: %v)", kv.me, request.Shard, request.Num)
		shid := request.Shard
		localCfgNum := kv.cfgNumByShid[shid]

		if request.Num == localCfgNum {
			switch kv.shardStatuses[request.Shard] {
			case Frozen:
				debug.ObserveMigrationPrintf("[G%d] server-%v: 删除分片(%v), Config #%v, 原状态: %v", kv.gid, kv.me, request.Shard, request.Num, statusToString(kv.shardStatuses[request.Shard]))
				kv.kvm[shid] = nil          // delete kv for the shard
				kv.shardClients[shid] = nil // delete information of clients that operated on this shard,
				// note that for current design, don't change lastPutByClient because we don't know whether the lastPut of a certain client recorded is the record of the deleted shard.
				// If we wish to remove that, we'll need carry the lastPut together with the key that it operated on
				kv.shardStatuses[shid] = Absent
				kv.cfgNumByShid[shid] = request.Num // update config Num
				reply = shardrpc.DeleteShardReply{
					Err: rpcapi.OK,
				}
			case Absent:
				// duplicated request
				reply = shardrpc.DeleteShardReply{
					Err: rpcapi.OK,
				}
			case Serving:
				reply = shardrpc.DeleteShardReply{
					Err: rpcapi.ErrIllegalOperation,
				}
			}
		} else if request.Num > localCfgNum {
			reply = shardrpc.DeleteShardReply{ // illegal operation
				Err: rpcapi.ErrIllegalOperation,
			}
		} else { // request.Num < localCfgNum
			reply = shardrpc.DeleteShardReply{
				Err: rpcapi.OK,
			}
		}
	default:
		debug.D5APrintf("shardkvserver %v: DoOp: Unknown req type %T\n", kv.me, req)
		reply = nil
	}
	return reply
}

// marshallShardState into bytes
func (kv *KVServer) marshallShardState(shid shardcfg.Tshid) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	lastPutByClientId := make(map[uint64]*LastPut)
	Clnts := kv.shardClients[shid] // clients that did Ops on this shard
	for client := range Clnts {    // we only care about the lastPut of clients that worked on this shard
		lastPutByClientId[client] = kv.lastPutByClientId[client]
	}

	kvm := make(map[string]*VersionedValue)
	for k, v := range kv.kvm[shid] {
		kvm[k] = v
	}
	shardState := ShardState{
		Kv:                kvm,
		LastPutByClientId: lastPutByClientId,
	}
	err := e.Encode(shardState)
	if err != nil {
		log.Fatal(err)
	}
	marshalledShardState := w.Bytes()
	return marshalledShardState
}

// need to used within Write Lock
func (kv *KVServer) installShardState(shid shardcfg.Tshid, shardstate []byte) {
	r := bytes.NewBuffer(shardstate)
	d := labgob.NewDecoder(r)
	var shardState ShardState
	err := d.Decode(&shardState)
	if err != nil {
		log.Fatalf("Fatal: %v", err)
	}

	// install shardkv
	kvm := make(map[string]*VersionedValue)
	for k, v := range shardState.Kv {
		kvm[k] = v
	}
	kv.kvm[shid] = kvm
	// install lastPutByClientId (), shardClients
	kv.shardClients[shid] = make(map[uint64]struct{})
	for clientId, Request := range shardState.LastPutByClientId {
		// shardClients
		kv.shardClients[shid][clientId] = struct{}{}
		// lastPutByClientId: only keep the latest (bigger requestId)
		if existingReq, exists := kv.lastPutByClientId[clientId]; !exists ||
			Request.RequestId > existingReq.RequestId {
			kv.lastPutByClientId[clientId] = Request
		}
	}
}

func (kv *KVServer) Snapshot() []byte {

	//debug.D5APrintf("shardkvserver %v: Snapshot()\n", kv.me)

	// make a copy of kvm
	kv.rwMu.Lock()
	var kvm [shardcfg.NShards]map[string]*VersionedValue
	for shid, shardkv := range kv.kvm {
		var copiedShardkv = make(map[string]*VersionedValue)
		for k, v := range shardkv {
			copiedV := &VersionedValue{
				Value:   v.Value,
				Version: v.Version,
			}
			copiedShardkv[k] = copiedV
		}
		kvm[shid] = copiedShardkv
	}

	// make a copy of LastPutByClientId
	var lastPutByClientId = make(map[uint64]*LastPut)
	for clientId, lastPut := range kv.lastPutByClientId {
		copiedReq := &LastPut{
			RequestId: lastPut.RequestId,
			Reply:     lastPut.Reply,
		}
		lastPutByClientId[clientId] = copiedReq
	}

	// make a copy of cfgNumByShid
	var cfgNumByShid = make(map[shardcfg.Tshid]shardcfg.Tnum)
	for shid, num := range kv.cfgNumByShid {
		cfgNumByShid[shid] = num
	}

	// make a copy of shardStatuses
	var shardStatuses [shardcfg.NShards]ShardStatus
	for shid, status := range kv.shardStatuses {
		shardStatuses[shid] = status
	}

	// make a copy of shardClients
	var shardClients [shardcfg.NShards]map[uint64]struct{}
	for shid := range kv.shardClients {
		clients := make(map[uint64]struct{})
		for client := range kv.shardClients[shid] {
			clients[client] = struct{}{}
		}
		shardClients[shid] = clients
	}
	kv.rwMu.Unlock()

	// snapshot
	snapshot := SnapshotData{
		Kvm:               kvm,
		LastPutByClientId: lastPutByClientId,
		CfgNumByShid:      cfgNumByShid,
		ShardStatuses:     shardStatuses,
		ShardClients:      shardClients,
	}

	// print number of keys in each shard
	var keyNum [shardcfg.NShards]int
	for shid, kv := range kvm {
		keyNum[shid] = len(kv)
	}
	debug.ObserveSnapshotPrintf("[G%d] server-%v: 生成快照, 各分片key数: %v", kv.gid, kv.me, keyNum)
	debug.D5APrintf("shardkvserver :%v Snapshoted \n keyNum: %v\n", kv.me, keyNum)

	// encode copied snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(snapshot)
	if err != nil {
		log.Fatalf("Fatal: %v", err)
		return nil
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	debug.D5APrintf("shardkvserver %v: Restore()\n", kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot SnapshotData
	err := d.Decode(&snapshot)
	if err != nil {
		log.Fatalf("Fatal: %v", err)
		return
	}

	// print number of keys in each shard
	var keyNum [shardcfg.NShards]int
	for shid, kv := range snapshot.Kvm {
		keyNum[shid] = len(kv)
	}
	debug.ObserveSnapshotPrintf("[G%d] server-%v: 从快照恢复, 各分片key数: %v", kv.gid, kv.me, keyNum)
	debug.D5APrintf("shardkvserver %v: Restored from snapshot\n keyNum: %v\n", kv.me, keyNum)

	kv.rwMu.Lock()
	defer kv.rwMu.Unlock()
	// restore from snapshot
	kv.kvm = snapshot.Kvm
	kv.lastPutByClientId = snapshot.LastPutByClientId
	kv.cfgNumByShid = snapshot.CfgNumByShid
	kv.shardStatuses = snapshot.ShardStatuses
	kv.shardClients = snapshot.ShardClients
}

func (kv *KVServer) Get(args *rpcapi.GetArgs, reply *rpcapi.GetReply) {

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpcapi.OK { // if rpc is ok, meaning rsm used DoOp(), just use the result DoOp() returns
		*reply = rep.(rpcapi.GetReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}

	debug.D5APrintf("shardkvserver %v: Get(%+v): %+v \n", kv.me, args, reply)
}

func (kv *KVServer) Put(args *rpcapi.PutArgs, reply *rpcapi.PutReply) {

	//log.Printf("shardkvserver %v Put called", kv.me)
	//defer log.Printf("server %v Put done", kv.me)

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpcapi.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(rpcapi.PutReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
	debug.D5APrintf("shardkvserver %v: Put(%+v): %+v \n", kv.me, args, reply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpcapi.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(shardrpc.FreezeShardReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
	debug.D5APrintf("shardkvserver %v: FreezeShard(shard:%v, Num:%v) -> stateSize:%v, replyNum:%v, Err:%v\n",
		kv.me, args.Shard, args.Num, len(reply.State), reply.Num, reply.Err)
}

// Install the supplied state for the specified shard.
// DoOp() only returns OK or WrongLeader
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	debug.D5APrintf("shardkvserver %v <-InstallShard(shard: %v, stateSize: %v, Num: %v) \n", kv.me, args.Shard, len(args.State), args.Num)
	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpcapi.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(shardrpc.InstallShardReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
	debug.D5APrintf("shardkvserver %v: InstallShard(shard:%v, stateSize:%v, Num:%v) -> Err:%v\n",
		kv.me, args.Shard, len(args.State), args.Num, reply.Err)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpcapi.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(shardrpc.DeleteShardReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*rpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call testgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpcapi.PutArgs{})
	labgob.Register(rpcapi.GetArgs{})
	labgob.Register(rpcapi.PutReply{})
	labgob.Register(rpcapi.GetReply{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(ShardState{})

	// initialisations:

	// kvm
	kv := &KVServer{gid: gid, me: me}
	var kvm [shardcfg.NShards]map[string]*VersionedValue
	for i := 0; i < shardcfg.NShards; i++ {
		kvm[i] = make(map[string]*VersionedValue)
	}
	kv.kvm = kvm

	// lastPutByClientId
	kv.lastPutByClientId = make(map[uint64]*LastPut)

	// cfgNumByShid tracks the latest configuration number this server has
	// processed for each shard.
	kv.cfgNumByShid = make(map[shardcfg.Tshid]shardcfg.Tnum)
	for i := 0; i < shardcfg.NShards; i++ {
		if gid == shardcfg.Gid1 {
			kv.cfgNumByShid[shardcfg.Tshid(i)] = 1
		} else {
			kv.cfgNumByShid[shardcfg.Tshid(i)] = 0
		}
	}

	// shardStatuses: only the initial group owns all shards at startup.
	var shardStatuses [shardcfg.NShards]ShardStatus
	for shid := 0; shid < shardcfg.NShards; shid++ {
		if gid == shardcfg.Gid1 {
			shardStatuses[shid] = Serving
		} else {
			shardStatuses[shid] = Absent
		}
	}
	kv.shardStatuses = shardStatuses

	// shardClients, initialise as empty
	var shardClients [shardcfg.NShards]map[uint64]struct{}
	for shid := 0; shid < shardcfg.NShards; shid++ {
		shardClients[shid] = make(map[uint64]struct{})
	}
	kv.shardClients = shardClients

	// initialise RSM (which may overwrite this server from its raft snapshot)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv, tester.GroupLabel(gid))

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*rpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
