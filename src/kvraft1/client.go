package kvraft

import (
	"fmt"
	"sync"

	"6.5840/debuglog"
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
	leader := ck.leader
	ck.mu.Unlock()
	for {
		debuglog.D4BPrintf("clerk -> %v: Get %s \n", ck.servers[leader], key)

		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		if ok {
			debuglog.D4BPrintf("clerk <- %v: Get %s, reply:%v \n", ck.servers[leader], key, reply)

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

		// if rpc fails, we're not sure whether the server is the leader,
		// so we could try other servers first ?
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
// but the response was lost, and the Clerk doesn't know if
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
	leader := ck.leader
	ck.mu.Unlock()

	firstTry := true

	for {
		debuglog.D4BPrintf("clerk -> %v: Put %s:%s \n", ck.servers[leader], key, value)

		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		if ok {
			debuglog.D4BPrintf("clerk <- %v: Put %s:%s, reply:%v \n", ck.servers[leader], key, value, reply)

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
