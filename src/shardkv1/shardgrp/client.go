package shardgrp

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/debug"
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

func nrand() uint64 {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(b[:])
}

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	clientId    uint64
	requestId   uint64 // this should increase monotonically. if the client is to reuse the clientId, it must persist requestId.
	maxAttempts int
	backoffTime time.Duration

	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader = 0
	ck.clientId = nrand()
	ck.requestId = 0
	ck.maxAttempts = len(ck.servers) * 3
	ck.backoffTime = 500 * time.Millisecond
	return ck
}

func (ck *Clerk) nextReqId() uint64 {
	return atomic.AddUint64(&ck.requestId, 1)
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get return OK, ErrNoKey, ErrWrongGroup as rpc.Err
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{
		RequestInfo: rpc.RequestInfo{
			ClientId:  ck.clientId,
			RequestId: ck.nextReqId(),
		},
		Key: key,
	}

	ck.mu.Lock()
	leader := ck.leader
	maxAttempt := ck.maxAttempts
	serverNum := len(ck.servers)
	backoffTime := ck.backoffTime
	ck.mu.Unlock()

	attempts := 0
	for attempts < maxAttempt {
		debug.D5APrintf("shardkvclerk %v -> %v: reqId:%5v, Get %s\n", args.ClientId, ck.servers[leader], args.RequestId, key)
		attempts++
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		if ok {
			//debug.D5APrintf("shardkvclerk %v <- %v: reqId:%5v, Get %s, reply: value:%v, Version: %v, Err: %v \n", args.ClientId, ck.servers[leader], args.RequestId, key, reply.Value, reply.Version, reply.Err)

			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey, rpc.ErrWrongGroup: // 正常Get结果
				debug.D5APrintf("shardkvclerk %v <- %v: reqId:%5v, Get %s, reply: value:%v, Version: %v, Err: %v \n", args.ClientId, ck.servers[leader], args.RequestId, key, reply.Value, reply.Version, reply.Err)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrWrongLeader: // 需要重试
				leader = (leader + 1) % len(ck.servers)
				continue
			default:
				panic(fmt.Sprintf("undefined rpc error type: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	// if exceeds maxAttempts, return ErrWrongLeader, so that the client will pull latest config from configStore
	return "", 0, rpc.ErrRetryExhausted
}

// Put returns OK, ErrNoKey, ErrWrongGroup, ErrMaybe as rpc.Err
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		RequestInfo: rpc.RequestInfo{
			ClientId:  ck.clientId,
			RequestId: ck.nextReqId(),
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

	firstTry := true

	attempts := 0
	for attempts < maxAttempt {
		debug.D5APrintf("shardkvclerk %v -> %v: reqId:%5v, Put %s, value:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value)
		attempts++
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		if ok {
			//debug.D5APrintf("shardkvclerk %v <- %v: reqId:%5v, Put %s: value: %v, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value, reply)

			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey, rpc.ErrWrongGroup:
				debug.D5APrintf("shardkvclerk %v <- %v: reqId:%5v, Put %s: %v, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value, reply)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Err
			case rpc.ErrVersion:
				debug.D5APrintf("shardkvclerk %v <- %v: reqId:%5v, Put %s: %v, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value, reply)
				if firstTry {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			case rpc.ErrWrongLeader:
				firstTry = false
				leader = (leader + 1) % len(ck.servers)
				continue
			default:
				panic(fmt.Sprintf("undefined rpc error type: %v", reply.Err))
			}
		}
		firstTry = false
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	// if exceeds maxAttempts, return ErrMaybe because it must have gone through at least one rpc failure, and we cannot know whether Put is executed or not.
	// ErrMaybe here isn't very accurate becase in RSM, it didn't distinguish leader fail before Starting a submit or fail after.
	return rpc.ErrMaybe
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
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
	for attempts < maxAttempt {
		debug.D5APrintf("shardkvclerk -> %v: FreezeShard (shard: %v, Num: %v) \n", ck.servers[leader], s, num)
		attempts++
		var reply shardrpc.FreezeShardReply
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)
		if ok {
			//debug.D5APrintf("shardkvclerk <- %v: FreezeShard (shard: %v, Num: %v), reply: StateSize:%v, Num: %v, Err: %v\n", ck.servers[leader], s, num, len(reply.State), reply.Num, reply.Err)
			switch reply.Err {
			case rpc.OK:
				debug.D5APrintf("shardkvclerk <- %v: FreezeShard (shard: %v, Num: %v), reply: StateSize:%v, Num: %v, Err: %v\n", ck.servers[leader], s, num, len(reply.State), reply.Num, reply.Err)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.State, reply.Err
			case rpc.ErrWrongLeader:
				leader = (leader + 1) % len(ck.servers)
				continue
			default:
				panic(fmt.Sprintf("undefined rpc error type: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	return nil, rpc.ErrRetryExhausted
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
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
	for attempts < maxAttempt {
		debug.D5APrintf("shardkvclerk -> %v: InstallShard(shard: %v, stateSize: %v,Num: %v) \n", ck.servers[leader], s, len(state), num)
		attempts++
		var reply shardrpc.InstallShardReply
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)
		if ok {
			//debug.D5APrintf("shardkvclerk <- %v: InstallShard(shard: %v, Num: %v), reply: %v \n", ck.servers[leader], s, num, reply)
			switch reply.Err {
			case rpc.OK:
				debug.D5APrintf("clerk <- %v: InstallShard(shard: %v, Num: %v), reply: %v \n", ck.servers[leader], s, num, reply)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Err
			case rpc.ErrWrongLeader:
				leader = (leader + 1) % len(ck.servers)
				continue
			default:
				panic(fmt.Sprintf("undefined rpc error type: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	return rpc.ErrRetryExhausted
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
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
	for attempts < maxAttempt {
		debug.D5APrintf("shardkvclerk -> %v: DeleteShard (shard: %v, Num: %v) \n", ck.servers[leader], s, num)
		attempts++
		var reply shardrpc.DeleteShardReply
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.DeleteShard", &args, &reply)
		if ok {
			//debug.D5APrintf("shardkvclerk <- %v: DeleteShard (shard: %v, Num: %v), reply: %v \n", ck.servers[leader], s, num, reply)
			switch reply.Err {
			case rpc.OK:
				debug.D5APrintf("shardkvclerk <- %v: DeleteShard (shard: %v, Num: %v), reply: %v \n", ck.servers[leader], s, num, reply)
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Err
			case rpc.ErrWrongLeader:
				leader = (leader + 1) % len(ck.servers)
				continue
			default:
				panic(fmt.Sprintf("undefined error: %v", reply.Err))
			}
		}
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 { // backoff
			time.Sleep(backoffTime)
		}
	}
	return rpc.ErrRetryExhausted
}
