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

type SnapshotData struct {
	Kvm               [shardcfg.NShards]map[string]*VersionedValue
	LastReqByClientId map[uint64]*LastRequest
	CfgNumByShid      map[shardcfg.Tshid]shardcfg.Tnum
	FrozenByShid      map[shardcfg.Tshid]bool
	ResponsibleShards map[shardcfg.Tshid]struct{}
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

	rwMu sync.RWMutex
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
	switch typedReq := req.(type) {

	// kv: Get, Put
	case rpc.GetArgs:
		// check whether the shard that the key belongs to is frozen
		shid := shardcfg.Key2Shard(typedReq.Key)
		kv.rwMu.RLock()
		defer kv.rwMu.RUnlock()

		// check whether it's responsible for the shard
		if !kv.isResponsibleForShard(shid) {
			reply = rpc.GetReply{
				Err: rpc.ErrWrongGroup,
			}
			return reply
		}

		// deduplicate request: return saved duplicate request reply
		lastReq, exists := kv.lastReqByClientId[typedReq.ClientId]
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

		// execute the Get operation
		verVal, exists := kv.kvm[shid][typedReq.Key]
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
		kv.lastReqByClientId[typedReq.ClientId] = &LastRequest{
			RequestId: typedReq.RequestId,
			Reply:     reply,
		}
		//debug.D5APrintf("server%v: saved LastReqByClientId {%v:%v}", kv.me, typedReq.ClientId, reply)
	case rpc.PutArgs:
		// check whether the shard that the key belongs to is frozen
		shid := shardcfg.Key2Shard(typedReq.Key)
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
		lastReq, exists := kv.lastReqByClientId[typedReq.ClientId]
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

		// execute the Put operation
		kvshard := kv.kvm[shid] // kv map of the respective shard
		if v, exists := kvshard[typedReq.Key]; exists {
			if v.Version == typedReq.Version { //version matches
				kvshard[typedReq.Key].Value = typedReq.Value
				kvshard[typedReq.Key].Version += 1
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
				kvshard[typedReq.Key] = &VersionedValue{Value: typedReq.Value, Version: 1}
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

	// sharding: Freeze, Install, Delete
	case shardrpc.FreezeShardArgs:
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()
		// check config Num
		// if local has higher, reject the request
		shid := typedReq.Shard
		localCfgNum, exists := kv.cfgNumByShid[shid]
		if !exists {
			log.Fatalf("server:%v DoOp(FreezeShad) doesn't have the Num for the shard %v\n", kv.me, shid)
		}
		// if the request is outdated, simply reply with its Configuration Num. When the clerk gets the Num, it knows that this request is not executed
		if localCfgNum > typedReq.Num {
			reply = shardrpc.FreezeShardReply{
				State: nil,
				Num:   localCfgNum,
				Err:   rpc.OK, // if the request is outdated, it must have been done before, otherwise controller wouldn't have incremented the config Num, therefore reply OK is safe
			}
			return reply
		} else if localCfgNum == typedReq.Num {
			// if localCfgNum equals, meaning the request has been executed, reply OK
			// and since it's been frozen, the current state will be the same. FreezeShard is idempotent
			if kv.frozenByShid[shid] != true {
				// the design here: once frozen state is set to true in FreezeShard, never set to false in the same config Num,
				// because controller may fail and retry Freeze/Delete unorderly. it breaks idempotency if it is allowed to change the *frozen* state
				// independently by Freeze or Delete.  reset *frozen* state only in InstallShard
				log.Fatalf("server%v DoOp(FreezeShard) should have frozen the shard %v!\n", kv.me, shid)
			}
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			err := e.Encode(kv.kvm[shid])
			if err != nil {
				log.Fatal(err)
			}
			marshalledKVShard := w.Bytes()
			reply = shardrpc.FreezeShardReply{
				State: marshalledKVShard,
				Num:   localCfgNum,
				Err:   rpc.OK,
			}
			return reply
		} else { // case where localCfgNum < Req.Num
			kv.frozenByShid[shid] = true         // set shard frozen
			kv.cfgNumByShid[shid] = typedReq.Num // update config Num

			// if not responsible for this shard, don't freeze
			// if controller functions correctly, this would never occur
			if !kv.isResponsibleForShard(shid) {
				reply = shardrpc.FreezeShardReply{
					State: nil,
					Num:   typedReq.Num,
					Err:   rpc.ErrWrongGroup,
				}
			} else { // this kv is responsible for the shard
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				err := e.Encode(kv.kvm[shid])
				if err != nil {
					log.Fatal(err)
				}
				marshalledKVShard := w.Bytes()
				reply = shardrpc.FreezeShardReply{
					State: marshalledKVShard,
					Num:   typedReq.Num,
					Err:   rpc.OK,
				}
			}
			return reply
		}
	case shardrpc.InstallShardArgs:
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()
		// check config Num
		// if local has higher, reject the request
		shid := typedReq.Shard
		localCfgNum, exists := kv.cfgNumByShid[shid]
		if !exists {
			log.Fatalf("server:%v DoOp(InstallShard) doesn't have the Num for the shard %v\n", kv.me, shid)
		}
		if localCfgNum >= typedReq.Num {
			reply = shardrpc.InstallShardReply{
				Err: rpc.OK,
			}
			return reply
		} else { // localCfgNum < typedReq.Num
			kv.cfgNumByShid[shid] = typedReq.Num    // update config Num
			kv.frozenByShid[shid] = false           // set shard unfrozen
			kv.responsibleShards[shid] = struct{}{} // mark shard as responsible

			r := bytes.NewBuffer(typedReq.State)
			d := labgob.NewDecoder(r)
			var shardkv map[string]*VersionedValue
			err := d.Decode(&shardkv)
			if err != nil {
				log.Fatal(err)
			}

			// install shardkv
			kv.kvm[shid] = shardkv
			reply = shardrpc.InstallShardReply{
				Err: rpc.OK,
			}
			return reply
		}
	case shardrpc.DeleteShardArgs:
		// idempotency design:
		// don't check whether it's responsible for the
		kv.rwMu.Lock()
		defer kv.rwMu.Unlock()
		shid := typedReq.Shard
		localCfgNum, exists := kv.cfgNumByShid[shid]
		if !exists {
			log.Fatalf("server:%v DoOp(DeleteShard) doesn't have the Num for the shard %v\n", kv.me, shid)
		}
		if localCfgNum > typedReq.Num {
			reply = shardrpc.DeleteShardReply{
				Err: rpc.OK,
			}
			return reply
		} else { // localCfgNum < typedReq.Num
			//log.Printf("server:%v DoOp(DeleteShard) at case: should delete %v \n", kv.me, shid)
			//debug.D5APrintf("Before DeleteShard: ")
			//kv.Snapshot()

			kv.kvm[shid] = nil                   // delete kv for the shard
			kv.cfgNumByShid[shid] = typedReq.Num // update config Num
			delete(kv.responsibleShards, shid)   // mark as no longer responsible
			reply = shardrpc.DeleteShardReply{
				Err: rpc.OK,
			}
			//debug.D5APrintf("After DeleteShard: ")
			//kv.Snapshot()
		}
	default:
		debug.D5APrintf("DoOp: Unknown req type %T\n", req)
		reply = nil
	}
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

	//debug.D5APrintf("server%v: Snapshot()\n", kv.me)
	// make a copy of kvm
	kv.rwMu.RLock()
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

	kv.rwMu.RUnlock()

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
	kv.cfgNumByShid = snapshot.CfgNumByShid
	kv.frozenByShid = snapshot.FrozenByShid
	kv.responsibleShards = snapshot.ResponsibleShards
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

	debug.D5APrintf("server%v: Get %v, reply: valueSize:%v, %v, %v \n", kv.me, args.Key, len(reply.Value), reply.Version, reply.Err)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	//log.Printf("server %v Put called", kv.me)
	//defer log.Printf("server %v Put done", kv.me)

	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(rpc.PutReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
	debug.D5APrintf("server%v: Put %v: valueSize%v, version:%v, reply: %v \n", kv.me, args.Key, len(args.Value), args.Version, reply.Err)
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
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	rpcErr, rep := kv.rsm.Submit(*args)
	if rpcErr == rpc.OK { // if rpc is ok, meaning rsm called DoOp(), just use the result DoOp() returns
		*reply = rep.(shardrpc.InstallShardReply)
	} else { // if rpc isn't ok, meaning this server is not the leader, the request didn't get through DoOp() and rep is nil
		reply.Err = rpcErr
	}
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
