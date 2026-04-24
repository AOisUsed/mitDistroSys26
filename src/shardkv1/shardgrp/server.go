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
	"6.5840/shardkv1/shardcfg"
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

type ShardState struct {
	Kv                map[string]*VersionedValue
	LastReqByClientId map[uint64]*LastRequest
}

type SnapshotData struct {
	Kvm               [shardcfg.NShards]map[string]*VersionedValue
	LastReqByClientId map[uint64]*LastRequest
	CfgNumByShid      map[shardcfg.Tshid]shardcfg.Tnum
	FrozenByShid      map[shardcfg.Tshid]bool
	ResponsibleShards map[shardcfg.Tshid]struct{}
	ShardClients      [shardcfg.NShards]map[uint64]struct{}
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	// for kv
	kvm               [shardcfg.NShards]map[string]*VersionedValue
	lastReqByClientId map[uint64]*LastRequest

	// for shard
	cfgNumByShid      map[shardcfg.Tshid]shardcfg.Tnum
	frozenByShid      map[shardcfg.Tshid]bool
	responsibleShards map[shardcfg.Tshid]struct{}
	shardClients      [shardcfg.NShards]map[uint64]struct{} // record which clients have done operation on the shard

	rwMu sync.Mutex
}

// check whether the server is responsible for the shard, need to be used within Read Lock
func (kv *KVServer) isResponsibleForShard(shid shardcfg.Tshid) bool {
	if _, exists := kv.responsibleShards[shid]; exists {
		return true
	}
	return false
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here

	var reply any
	switch request := req.(type) {

	// kv: Get, Put
	case rpc.GetArgs:
		// check whether the shard that the key belongs to is frozen
		shid := shardcfg.Key2Shard(request.Key)
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()

		// check whether it's responsible for the shard
		if !kv.isResponsibleForShard(shid) {
			reply = rpc.GetReply{
				Err: rpc.ErrWrongGroup,
			}
			return reply
		}

		// deduplicate request: return saved duplicate request reply
		lastReq, exists := kv.lastReqByClientId[request.ClientId]
		if exists {
			// if the request has been executed, simply return the result and not execute it again.
			if request.RequestId <= lastReq.RequestId {
				// note that request.reqId < lastReq.reqId condition should not occur if client functions correctly
				// because requestId from the client shouldn't increase if the pending request wasn't successfully handled by server
				if request.RequestId < lastReq.RequestId {
					log.Fatalf("shardkvserver %v:client %v sending request with impossible id:%v, as lastReq id: %v!\n", kv.me, request.ClientId, request.RequestId, lastReq.RequestId)
				}
				debug.D5APrintf("shardkvserver %v:client %v sending duplicated request:%v\n", kv.me, request.ClientId, request.RequestId)
				return lastReq.Reply // if client bugs (i.e. sending request of the id smaller than lastReq), server is not responsible for sending the correct reply, and will just send reply of lastReq (which doesn't reflect the truth).
			}
		}

		// execute the Get operation
		verVal, exists := kv.kvm[shid][request.Key]
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
		//debug.D5APrintf("shardkvserver %v: DoOp: Get req %v, reply:%v \n", kv.me, req, reply)
		// Save to LastReqByClientId
		kv.lastReqByClientId[request.ClientId] = &LastRequest{
			RequestId: request.RequestId,
			Reply:     reply,
		}
		kv.shardClients[shid][request.ClientId] = struct{}{}
		//debug.D5APrintf("shardkvserver %v: saved LastReqByClientId {%v:%v}", kv.me, request.ClientId, reply)
	case rpc.PutArgs:
		// check whether the shard that the key belongs to is frozen
		shid := shardcfg.Key2Shard(request.Key)
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()

		// check whether it's responsible for the shard
		if !kv.isResponsibleForShard(shid) {
			reply = rpc.PutReply{
				Err: rpc.ErrWrongGroup,
			}
			return reply
		}

		// check whether the shard is frozen, and return WrongGroup if frozen
		isFrozen, exists := kv.frozenByShid[shid]
		if exists && isFrozen {
			reply = rpc.PutReply{
				Err: rpc.ErrWrongGroup,
			}
			return reply
		}

		// deduplicate request: return saved duplicate request reply
		lastReq, exists := kv.lastReqByClientId[request.ClientId]
		if exists {
			// if the request has been executed, simply return the result and not execute it again.
			if request.RequestId <= lastReq.RequestId {
				// note that request.Id < lastReq.Id condition should not occur if client functions correctly
				// because requestId from the client shouldn't increase if the pending request wasn't successfully handled by server
				if request.RequestId < lastReq.RequestId {
					debug.D5APrintf("shardkvserver %v:client %v sending request with impossible id:%v!\n", kv.me, request.ClientId, request.RequestId)
				}
				debug.D5APrintf("shardkvserver %v:client %v sending duplicated request:%v\n", kv.me, request.ClientId, request.RequestId)
				return lastReq.Reply // if client bugs (i.e. sending request of the id smaller than lastReq), server is not responsible for sending the correct reply, and will just send reply of lastReq (which doesn't reflect the truth).
			}
		}

		// execute the Put operation
		kvshard := kv.kvm[shid] // kv map of the respective shard
		if v, exists := kvshard[request.Key]; exists {
			if v.Version == request.Version { //version matches
				kvshard[request.Key].Value = request.Value
				kvshard[request.Key].Version += 1
				reply = rpc.PutReply{
					Err: rpc.OK,
				}
			} else { //has the key, version doesn't match
				reply = rpc.PutReply{
					Err: rpc.ErrVersion,
				}
			}
		} else {
			if request.Version == 0 {
				kvshard[request.Key] = &VersionedValue{Value: request.Value, Version: 1}
				reply = rpc.PutReply{
					Err: rpc.OK,
				}
			} else { //doesn't have the key, and the version isn't correct either
				reply = rpc.PutReply{
					Err: rpc.ErrNoKey,
				}
			}
		}
		//debug.D5APrintf("shardkvserver %v: DoOp: Put req %v, reply:%v \n", kv.me, req, reply)
		// Save to LastReqByClientId
		kv.lastReqByClientId[request.ClientId] = &LastRequest{
			RequestId: request.RequestId,
			Reply:     reply,
		}
		kv.shardClients[shid][request.ClientId] = struct{}{}
	//debug.D5APrintf("shardkvserver %v: saved LastReqByClientId {%v:%v}", kv.me, request.ClientId, reply)

	// sharding: Freeze, Install, Delete
	case shardrpc.FreezeShardArgs:
		debug.D5APrintf("shardkvserver %v: DoOp Freeze(shard: %v, Num: %v)", kv.me, request.Shard, request.Num)
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()
		// check config Num
		// if local has higher, reject the request
		shid := request.Shard
		localCfgNum, exists := kv.cfgNumByShid[shid]
		if !exists {
			log.Fatalf("shardkvserver %v: DoOp(FreezeShad) doesn't have the Num for the shard %v\n", kv.me, shid)
		}
		// if the request is outdated, simply reply with its Configuration Num. When the clerk gets the Num, it knows that this request is not executed
		if localCfgNum >= request.Num {
			reply = shardrpc.FreezeShardReply{
				State: nil,
				Num:   localCfgNum,
				Err:   rpc.OK, // if the request is outdated, it must have been done before, otherwise controller wouldn't have incremented the config Num, therefore reply OK is safe
			}
			return reply
			//} else if localCfgNum == request.Num {
			//	// if localCfgNum equals, meaning the request has been executed, reply OK
			//	// and since it's been frozen, the current state will be the same. FreezeShard is idempotent
			//	if kv.frozenByShid[shid] != true {
			//		// the design here: once frozen state is set to true in FreezeShard, never set to false in the same config Num,
			//		// because controller may fail and retry Freeze/Delete unorderly. it breaks idempotency if it is allowed to change the *frozen* state
			//		// independently by Freeze or Delete.  reset *frozen* state only in InstallShard
			//		log.Fatalf("shardkvserver %v: DoOp(FreezeShard) should have frozen the shard %v!\n", kv.me, shid)
			//	}
			//	w := new(bytes.Buffer)
			//	e := labgob.NewEncoder(w)
			//
			//	lastReqByClientId := make(map[uint64]*LastRequest)
			//	Clnts := kv.shardClients[shid] // clients that did Ops on this shard
			//	for client := range Clnts {    // we only care about the lastReq of clients that worked on this shard
			//		lastReqByClientId[client] = kv.lastReqByClientId[client]
			//	}
			//	shardState := ShardState{
			//		Kv:                kv.kvm[shid],
			//		LastReqByClientId: lastReqByClientId,
			//	}
			//	err := e.Encode(shardState)
			//	if err != nil {
			//		log.Fatal(err)
			//	}
			//	marshalledShardState := w.Bytes()
			//	reply = shardrpc.FreezeShardReply{
			//		State: marshalledShardState,
			//		Num:   localCfgNum,
			//		Err:   rpc.OK,
			//	}
			//	return reply
		} else { // case where localCfgNum < Req.Num
			kv.frozenByShid[shid] = true        // set shard frozen
			kv.cfgNumByShid[shid] = request.Num // update config Num

			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)

			lastReqByClientId := make(map[uint64]*LastRequest)
			Clnts := kv.shardClients[shid] // clients that did Ops on this shard
			for client := range Clnts {    // we only care about the lastReq of clients that worked on this shard
				lastReqByClientId[client] = kv.lastReqByClientId[client]
			}
			shardState := ShardState{
				Kv:                kv.kvm[shid],
				LastReqByClientId: lastReqByClientId,
			}
			err := e.Encode(shardState)
			if err != nil {
				log.Fatal(err)
			}
			marshalledShardState := w.Bytes()
			reply = shardrpc.FreezeShardReply{
				State: marshalledShardState,
				Num:   request.Num,
				Err:   rpc.OK,
			}
			return reply
		}
	case shardrpc.InstallShardArgs:
		debug.D5APrintf("shardkvserver %v: DoOp InstallShard(shard: %v, stateSize: %v, Num: %v)", kv.me, request.Shard, len(request.State), request.Num)
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()
		// check config Num
		// if local has higher, reject the request
		shid := request.Shard
		localCfgNum, exists := kv.cfgNumByShid[shid]
		if !exists {
			log.Fatalf("shardkvserver %v: DoOp(InstallShard) doesn't have the Num for the shard %v\n", kv.me, shid)
		}
		if localCfgNum >= request.Num {
			reply = shardrpc.InstallShardReply{
				Err: rpc.OK,
			}
			return reply
		} else { // localCfgNum < request.Num
			kv.cfgNumByShid[shid] = request.Num     // update config Num
			kv.frozenByShid[shid] = false           // set shard unfrozen
			kv.responsibleShards[shid] = struct{}{} // mark shard as responsible

			r := bytes.NewBuffer(request.State)
			d := labgob.NewDecoder(r)
			var shardState ShardState
			err := d.Decode(&shardState)
			if err != nil {
				log.Fatal(err)
			}

			// install shardkv
			kv.kvm[shid] = shardState.Kv

			// install lastReqByClientId (), shardClients
			for clientId, Request := range shardState.LastReqByClientId {
				// shardClients
				kv.shardClients[shid][clientId] = struct{}{}
				// lastReqByClientId: only keep the latest (bigger requestId)
				if existingReq, exists := kv.lastReqByClientId[clientId]; !exists ||
					Request.RequestId > existingReq.RequestId {
					kv.lastReqByClientId[clientId] = Request
				}
			}

			reply = shardrpc.InstallShardReply{
				Err: rpc.OK,
			}
			return reply
		}
	case shardrpc.DeleteShardArgs:
		debug.D5APrintf("shardkvserver %v: DoOp DeleteShard(shard: %v, Num: %v)", kv.me, request.Shard, request.Num)
		// idempotency design:
		// don't check whether it's responsible for the
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()
		shid := request.Shard
		localCfgNum, exists := kv.cfgNumByShid[shid]
		if !exists {
			log.Fatalf("shardkvserver %v: DoOp(DeleteShard) doesn't have the Num for the shard %v\n", kv.me, shid)
		}
		if localCfgNum > request.Num {
			reply = shardrpc.DeleteShardReply{
				Err: rpc.OK,
			}
			return reply
		} else { // localCfgNum <= request.Num  (must execute deletion when the Num matches because Num matches only mean that this shard at least Frozen the shard ?)
			//log.Printf("shardkvserver %v: DoOp(DeleteShard) at case: should delete %v \n", kv.me, shid)
			//debug.D5APrintf("Before DeleteShard: ")
			//kv.Snapshot()

			kv.kvm[shid] = nil          // delete kv for the shard
			kv.shardClients[shid] = nil // delete shardClients,
			// note that for current design, don't change lastReqByClient because we don't know whether the lastReq of a certain client recorded is the record of the deleted shard.
			// If we wish to remove that, we'll need carry the lastReq together with the key that it operated on
			kv.cfgNumByShid[shid] = request.Num // update config Num
			delete(kv.responsibleShards, shid)  // mark as no longer responsible
			reply = shardrpc.DeleteShardReply{
				Err: rpc.OK,
			}
			//debug.D5APrintf("After DeleteShard: ")
			//kv.Snapshot()
		}
	default:
		debug.D5APrintf("shardkvserver %v: DoOp: Unknown req type %T\n", kv.me, req)
		reply = nil
	}
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

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

	// make a copy of LastReqByClientId
	var lastReqByClientId = make(map[uint64]*LastRequest)
	for clientId, lastRequest := range kv.lastReqByClientId {
		copiedReq := &LastRequest{
			RequestId: lastRequest.RequestId,
			Reply:     lastRequest.Reply,
		}
		lastReqByClientId[clientId] = copiedReq
	}

	// make a copy of cfgNumByShid
	var cfgNumByShid = make(map[shardcfg.Tshid]shardcfg.Tnum)
	for shid, num := range kv.cfgNumByShid {
		cfgNumByShid[shid] = num
	}

	// make a copy of frozenByShid
	var frozenByShid = make(map[shardcfg.Tshid]bool)
	for shid, isFrozen := range kv.frozenByShid {
		frozenByShid[shid] = isFrozen
	}

	// make a copy of ResponsibleShards
	var responsibleShards = make(map[shardcfg.Tshid]struct{})
	for shid := range kv.responsibleShards {
		responsibleShards[shid] = struct{}{}
	}

	var shardClients [shardcfg.NShards]map[uint64]struct{}
	for shid := range kv.shardClients {
		clients := make(map[uint64]struct{})
		for client := range kv.shardClients[shid] {
			clients[client] = struct{}{}
		}
		shardClients[shid] = clients
	}
	kv.rwMu.Unlock()

	//var totalSize int
	//kvmBuf := new(bytes.Buffer)
	//gob.NewEncoder(kvmBuf).Encode(kvm)
	//kvmSize := kvmBuf.Len()
	//totalSize += kvmSize
	////log.Printf("[Snapshot] Kvm size: %d bytes", kvmSize)
	//
	//// 2. LastReqByClientId 大小
	//reqBuf := new(bytes.Buffer)
	//gob.NewEncoder(reqBuf).Encode(lastReqByClientId)
	//reqSize := reqBuf.Len()
	//totalSize += reqSize
	////log.Printf("[Snapshot] LastReq size: %d bytes", reqSize)
	//
	//// 3. CfgNumByShid 大小
	//cfgBuf := new(bytes.Buffer)
	//gob.NewEncoder(cfgBuf).Encode(cfgNumByShid)
	//cfgSize := cfgBuf.Len()
	//totalSize += cfgSize
	////log.Printf("[Snapshot] CfgNum size: %d bytes", cfgSize)
	//
	//// 4. FrozenByShid 大小
	//frozenBuf := new(bytes.Buffer)
	//gob.NewEncoder(frozenBuf).Encode(frozenByShid)
	//frozenSize := frozenBuf.Len()
	//totalSize += frozenSize
	////log.Printf("[Snapshot] Frozen size: %d bytes", frozenSize)
	//
	//// 5. ResponsibleShards 大小
	//respBuf := new(bytes.Buffer)
	//gob.NewEncoder(respBuf).Encode(responsibleShards)
	//respSize := respBuf.Len()
	//totalSize += respSize
	////log.Printf("[Snapshot] Responsible size: %d bytes", respSize)
	//
	//log.Printf("kv%v: [Snapshot] KVM: %d, LastReq: %d, CfgNum: %d, Frozen: %d, Resp: %d | Total: %d ",
	//	kv.me,
	//	kvmBuf.Len(),
	//	reqBuf.Len(),
	//	cfgBuf.Len(),
	//	frozenBuf.Len(),
	//	respBuf.Len(),
	//	totalSize,
	//)

	// snapshot
	snapshot := SnapshotData{
		Kvm:               kvm,
		LastReqByClientId: lastReqByClientId,
		CfgNumByShid:      cfgNumByShid,
		FrozenByShid:      frozenByShid,
		ResponsibleShards: responsibleShards,
		ShardClients:      shardClients,
	}

	// print number of keys in each shard
	var keyNum [shardcfg.NShards]int
	for shid, kv := range kvm {
		keyNum[shid] = len(kv)
	}
	debug.D5APrintf("shardkvserver :%v Snapshoted \n keyNum: %v\n", kv.me, keyNum)

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

	debug.D5APrintf("shardkvserver %v: Restore()\n", kv.me)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot SnapshotData
	err := d.Decode(&snapshot)
	if err != nil {
		log.Fatal(err)
		return
	}

	// print number of keys in each shard
	var keyNum [shardcfg.NShards]int
	for shid, kv := range snapshot.Kvm {
		keyNum[shid] = len(kv)
	}
	debug.D5APrintf("shardkvserver %v: Restored from snapshot\n keyNum: %v\n", kv.me, keyNum)

	//var sb strings.Builder
	//for k, v := range kvm {
	//	s := fmt.Sprintf("%v-%v\t", k, v)
	//	sb.WriteString(s)
	//}

	//debug.D5APrintf("shardkvserver %v: Restored from snapshot:%v\n", kv.me, sb.String())

	kv.rwMu.Lock()
	defer kv.rwMu.Unlock()

	kv.kvm = snapshot.Kvm
	kv.lastReqByClientId = snapshot.LastReqByClientId
	kv.cfgNumByShid = snapshot.CfgNumByShid
	kv.frozenByShid = snapshot.FrozenByShid
	kv.responsibleShards = snapshot.ResponsibleShards
	kv.shardClients = snapshot.ShardClients
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm used DoOp(), just use the result DoOp() returns
		*reply = rep.(rpc.GetReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}

	debug.D5APrintf("shardkvserver %v: Get(%+v): %+v \n", kv.me, args, reply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	//log.Printf("shardkvserver %v Put called", kv.me)
	//defer log.Printf("server %v Put done", kv.me)

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(rpc.PutReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
	debug.D5APrintf("shardkvserver %v: Put(%+v): %+v \n", kv.me, args, reply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(shardrpc.FreezeShardReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
	debug.D5APrintf("shardkvserver %v: FreezeShard(%+v): %+v \n", kv.me, args, reply)
}

// Install the supplied state for the specified shard.
// DoOp() only returns OK or WrongLeader
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here

	debug.D5APrintf("shardkvserver %v <-InstallShard(shard: %v, stateSize: %v, Num: %v) \n", kv.me, args.Shard, len(args.State), args.Num)
	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(shardrpc.InstallShardReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
	debug.D5APrintf("shardkvserver %v: InstallShard(%+v): %+v \n", kv.me, args, reply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(shardrpc.DeleteShardReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
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
	labgob.Register(ShardState{})

	kv := &KVServer{gid: gid, me: me}

	var kvm [shardcfg.NShards]map[string]*VersionedValue
	for i := 0; i < shardcfg.NShards; i++ {
		kvm[i] = make(map[string]*VersionedValue)
	}
	kv.kvm = kvm

	kv.lastReqByClientId = make(map[uint64]*LastRequest)
	kv.cfgNumByShid = make(map[shardcfg.Tshid]shardcfg.Tnum)
	// initialise such that Num for each shard is 0 // todo: may be incorrect
	for i := 0; i < shardcfg.NShards; i++ {
		kv.cfgNumByShid[shardcfg.Tshid(i)] = 0
	}
	kv.frozenByShid = make(map[shardcfg.Tshid]bool)
	kv.responsibleShards = make(map[shardcfg.Tshid]struct{})
	// initialise responsibleShards such that the server suppose that it's responsible for all shards, todo:may not be correct
	for i := 0; i < shardcfg.NShards; i++ {
		kv.responsibleShards[shardcfg.Tshid(i)] = struct{}{}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
