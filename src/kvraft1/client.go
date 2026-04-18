package kvraft

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/debug"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
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
	// You can add to this struct.

	clientId  uint64
	requestId uint64 // this should increase monotonically. if the client is to reuse the clientId, it must persist requestId.
	mu        sync.Mutex
}

func (ck *Clerk) nextReqId() uint64 {
	return atomic.AddUint64(&ck.requestId, 1)
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leader = 0
	ck.clientId = nrand()
	ck.requestId = 0
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
		debug.D4BPrintf("clerk%v -> %v: reqId:%5v, Get %s\n", args.ClientId, ck.servers[leader], args.RequestId, key)

		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		if ok {
			debug.D4BPrintf("clerk%v <- %v: reqId:%5v, Get %s, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, reply)

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
		//debug.D4BPrintf("clerk -> %v: Put %s:%s \n", ck.servers[leader], key, value)
		debug.D4BPrintf("clerk%v -> %v: reqId:%5v, Put %s:%s \n", args.ClientId, ck.servers[leader], args.RequestId, key, value)

		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		if ok {
			//debug.D4BPrintf("clerk <- %v: Put %s:%s, reply:%v \n", ck.servers[leader], key, value, reply)
			debug.D4BPrintf("clerk%v <- %v: reqId:%5v, Put %s:%s, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value, reply)

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
