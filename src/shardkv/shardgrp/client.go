package shardgrp

import (
	"fmt"
	"sync"
	"time"

	"kvstore/debug"
	"kvstore/kvsrv/rpcapi"
	"kvstore/shardkv/shardcfg"
	"kvstore/shardkv/shardgrp/shardrpc"
	"kvstore/tester"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])

	clientId uint64
	//requestId   uint64 // this should increase monotonically. if the client is to reuse the clientId, it must persist requestId.
	maxAttempts int
	backoffTime time.Duration

	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string, clientId uint64) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader = 0
	ck.clientId = clientId
	ck.maxAttempts = len(ck.servers) * 2 //  try one round + one time
	ck.backoffTime = 200 * time.Millisecond
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get return OK, ErrNoKey, ErrWrongGroup as rpcapi.Err
func (ck *Clerk) Get(key string) (string, rpcapi.Tversion, rpcapi.Err) {
	args := rpcapi.GetArgs{
		Key: key,
	}

	ck.mu.Lock()
	leader := ck.leader
	maxAttempt := ck.maxAttempts
	serverNum := len(ck.servers)
	backoffTime := ck.backoffTime
	ck.mu.Unlock()

	attempts := 0
	for attempts <= maxAttempt {
		debug.D5APrintf("shardkvclerk %v -> %v: Get %s\n", ck.servers[leader], ck.clientId, key)
		attempts++
		var reply rpcapi.GetReply
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case rpcapi.OK, rpcapi.ErrNoKey, rpcapi.ErrWrongGroup:
				debug.D5APrintf("shardkvclerk %v <- %v: Get %s, reply: value:%v, Version: %v, Err: %v \n", ck.clientId, ck.servers[leader], key, reply.Value, reply.Version, reply.Err)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Value, reply.Version, reply.Err
			case rpcapi.ErrWrongLeader:
			default:
				panic(fmt.Sprintf("Get: undefined rpc error type: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	// if exceeds maxAttempts, return ErrWrongLeader, so that the client will pull latest config from configStore
	debug.D5APrintf("shardkvclerk %v: Get %s retry exhausted after %d attempts\n", ck.clientId, key, attempts)
	debug.ObserveFaultPrintf("shardgrpclerk%v: Get(%s) retry exhausted after %d attempts (fault: all servers unavailable or partitioned)", ck.clientId, key, attempts)
	return "", 0, rpcapi.ErrRetryExhausted
}

// Put returns OK, ErrNoKey, ErrWrongGroup, ErrMaybe as rpcapi.Err
func (ck *Clerk) Put(requestId uint64, key string, value string, version rpcapi.Tversion) rpcapi.Err {
	args := rpcapi.PutArgs{
		RequestInfo: rpcapi.RequestInfo{
			ClientId:  ck.clientId,
			RequestId: requestId,
		},
		Key:     key,
		Value:   value,
		Version: version,
	}

	ck.mu.Lock()
	leader := ck.leader
	maxAttempt := ck.maxAttempts
	serverNum := len(ck.servers)
	backoffTime := ck.backoffTime
	ck.mu.Unlock()

	attempts := 0
	for attempts <= maxAttempt {
		debug.D5APrintf("shardkvclerk %v -> %v: reqId:%5v, Put %s, value:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value)
		attempts++
		reply := rpcapi.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		if ok {
			//debug.D5APrintf("shardkvclerk %v <- %v: reqId:%5v, Put %s: value: %v, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value, reply
			switch reply.Err {
			case rpcapi.OK, rpcapi.ErrNoKey, rpcapi.ErrWrongGroup, rpcapi.ErrVersion:
				debug.D5APrintf("shardkvclerk %v <- %v: reqId:%5v, Put %s: %v, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value, reply)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Err
			case rpcapi.ErrWrongLeader:
			default:
				panic(fmt.Sprintf("Put: undefined rpc error type: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	// Retries exhausted usually means we failed to discover a reachable/working
	// leader for this shard group in this round.
	// it could mean:
	//	- the group is in partition/election,
	// 	- the group has left
	debug.ObserveFaultPrintf("shardgrpclerk%v: Put(key=%s) retry exhausted after %d attempts (fault: group partitioned or left)", ck.clientId, key, attempts)
	return rpcapi.ErrRetryExhausted
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpcapi.Err) {
	// Your code here

	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	ck.mu.Lock()
	leader := ck.leader
	maxAttempt := ck.maxAttempts
	serverNum := len(ck.servers)
	backoffTime := ck.backoffTime
	ck.mu.Unlock()

	attempts := 0
	for attempts <= maxAttempt {
		debug.D5APrintf("shardkvclerk -> %v: FreezeShard (shard: %v, Num: %v) \n", ck.servers[leader], s, num)
		attempts++
		var reply shardrpc.FreezeShardReply
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)
		if ok {
			//debug.D5APrintf("shardkvclerk <- %v: FreezeShard (shard: %v, Num: %v), reply: StateSize:%v, Num: %v, Err: %v\n", ck.servers[leader], s, num, len(reply.State), reply.Num, reply.Err)
			switch reply.Err {
			case rpcapi.OK, rpcapi.ErrWrongGroup:
				debug.D5APrintf("shardkvclerk <- %v: FreezeShard (shard: %v, Num: %v), reply: StateSize:%v, Num: %v, Err: %v\n", ck.servers[leader], s, num, len(reply.State), reply.Num, reply.Err)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.State, reply.Err
			case rpcapi.ErrWrongLeader:
			default:
				panic(fmt.Sprintf("FreezeShard: undefined rpc error type: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	debug.ObserveFaultPrintf("shardgrpclerk%v: FreezeShard(shard=%v) retry exhausted after %d attempts (fault: group partitioned or left)", ck.clientId, s, attempts)
	return nil, rpcapi.ErrRetryExhausted
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpcapi.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	ck.mu.Lock()
	leader := ck.leader
	maxAttempt := ck.maxAttempts
	serverNum := len(ck.servers)
	backoffTime := ck.backoffTime
	ck.mu.Unlock()

	attempts := 0
	for attempts <= maxAttempt {
		debug.D5APrintf("shardkvclerk -> %v: InstallShard(shard: %v, stateSize: %v,Num: %v) \n", ck.servers[leader], s, len(state), num)
		attempts++
		var reply shardrpc.InstallShardReply
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)
		if ok {
			//debug.D5APrintf("shardkvclerk <- %v: InstallShard(shard: %v, Num: %v), reply: %v \n", ck.servers[leader], s, num, reply)
			switch reply.Err {
			case rpcapi.OK, rpcapi.ErrWrongGroup:
				debug.D5APrintf("clerk <- %v: InstallShard(shard: %v, Num: %v), reply: %v \n", ck.servers[leader], s, num, reply)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Err
			case rpcapi.ErrWrongLeader:
			default:
				panic(fmt.Sprintf("InstallShard: undefined rpc error type: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	debug.ObserveFaultPrintf("shardgrpclerk%v: InstallShard(shard=%v) retry exhausted after %d attempts (fault: group partitioned or left)", ck.clientId, s, attempts)
	return rpcapi.ErrRetryExhausted
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpcapi.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	ck.mu.Lock()
	leader := ck.leader
	maxAttempt := ck.maxAttempts
	serverNum := len(ck.servers)
	backoffTime := ck.backoffTime
	ck.mu.Unlock()

	attempts := 0
	for attempts <= maxAttempt {
		debug.D5APrintf("shardkvclerk -> %v: DeleteShard (shard: %v, Num: %v) \n", ck.servers[leader], s, num)
		attempts++
		var reply shardrpc.DeleteShardReply
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.DeleteShard", &args, &reply)
		if ok {
			//debug.D5APrintf("shardkvclerk <- %v: DeleteShard (shard: %v, Num: %v), reply: %v \n", ck.servers[leader], s, num, reply)
			switch reply.Err {
			case rpcapi.OK, rpcapi.ErrWrongGroup:
				debug.D5APrintf("shardkvclerk <- %v: DeleteShard (shard: %v, Num: %v), reply: %v \n", ck.servers[leader], s, num, reply)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Err
			case rpcapi.ErrWrongLeader:
			default:
				panic(fmt.Sprintf("DeleteShard: undefined rpc error type: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	debug.ObserveFaultPrintf("shardgrpclerk%v: DeleteShard(shard=%v) retry exhausted after %d attempts (fault: group partitioned or left)", ck.clientId, s, attempts)
	return rpcapi.ErrRetryExhausted
}
