package kvraft

import (
	"sync"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.

	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leader = 0
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	ck.mu.Lock()
	leader := ck.leader // ⭐ 本地副本
	ck.mu.Unlock()
	for {
		reply := rpc.GetReply{}

		rsm.D4BPrintf("clerk -> %v: Get %s \n", ck.servers[leader], key)

		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)

		if ok {
			rsm.D4BPrintf("clerk <- %v: Get %s, reply:%v \n", ck.servers[leader], key, reply)
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				// ⭐ 成功才更新全局 leader
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()

				return reply.Value, reply.Version, reply.Err
			}

			if reply.Err == rpc.ErrWrongLeader {
				leader = (leader + 1) % len(ck.servers)
				continue
			}
		}

		// !ok 或其他异常
		time.Sleep(100 * time.Millisecond)
		leader = (leader + 1) % len(ck.servers)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	ck.mu.Lock()
	leader := ck.leader // ⭐ 本地 leader
	ck.mu.Unlock()

	firstTry := true

	for {
		reply := rpc.PutReply{}

		rsm.D4BPrintf("clerk -> %v: Put %s:%s \n", ck.servers[leader], key, value)

		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		if ok {
			rsm.D4BPrintf("clerk <- %v: Put %s:%s, reply:%v \n", ck.servers[leader], key, value, reply)
			if reply.Err == rpc.OK {
				// ⭐ 成功才更新 leader
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()

				return rpc.OK
			}

			if reply.Err == rpc.ErrWrongLeader {
				leader = (leader + 1) % len(ck.servers)
				firstTry = false
				continue
			}

			if reply.Err == rpc.ErrVersion {
				if firstTry {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		} else {
			// ❗ RPC失败 → 可能已经执行
			if !firstTry {
				return rpc.ErrMaybe
			}
		}

		firstTry = false
		time.Sleep(100 * time.Millisecond)
		leader = (leader + 1) % len(ck.servers)
	}
}
