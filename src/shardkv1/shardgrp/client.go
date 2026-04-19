package shardgrp

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/debug"
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
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
	clientId  uint64
	requestId uint64 // this should increase monotonically. if the client is to reuse the clientId, it must persist requestId.
	mu        sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leader = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) nextReqId() uint64 {
	return atomic.AddUint64(&ck.requestId, 1)
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get only return OK, ErrNoKey as rpc.Err
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
	ck.mu.Unlock()
	for {
		debug.D5APrintf("clerk%v -> %v: reqId:%5v, Get %s\n", args.ClientId, ck.servers[leader], args.RequestId, key)

		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		if ok {
			debug.D5APrintf("clerk%v <- %v: reqId:%5v, Get %s, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, reply)

			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrWrongLeader:
				leader = (leader + 1) % len(ck.servers)
				continue
			default:
				panic(fmt.Sprintf("undefined error: %v", reply.Err))
			}
		}

		leader = (leader + 1) % len(ck.servers)
	}
}

// Put only return OK, ErrNoKey, ErrMaybe as rpc.Err
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
	ck.mu.Unlock()

	firstTry := true

	for {
		//debug.D5APrintf("clerk -> %v: Put %s:%s \n", ck.servers[leader], key, value)
		debug.D5APrintf("clerk%v -> %v: reqId:%5v, Put %s:%s \n", args.ClientId, ck.servers[leader], args.RequestId, key, value)

		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		if ok {
			//debug.D5APrintf("clerk <- %v: Put %s:%s, reply:%v \n", ck.servers[leader], key, value, reply)
			debug.D5APrintf("clerk%v <- %v: reqId:%5v, Put %s:%s, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value, reply)

			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Err
			case rpc.ErrVersion:
				if firstTry {
					return rpc.ErrVersion
				} else {
					return rpc.ErrMaybe
				}
			case rpc.ErrWrongLeader:
				firstTry = false
				leader = (leader + 1) % len(ck.servers)
				continue
			default:
				panic(fmt.Sprintf("undefined error: %v", reply.Err))
			}
		} else {
			// RPC fails: it may have been executed or not.
			// Note: whether it's firstTry isn't associated with the specific server
			// but with the command. Once RPC fails, no matter whom the client communicated with,
			// firstTry need to be set to false. Because leadership can quickly change and
			// the Command propagates between the servers.
			firstTry = false
			leader = (leader + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
