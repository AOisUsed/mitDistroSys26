package kvraft

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	"kvstore/debug"
	"kvstore/kvsrv/rpcapi"
	"kvstore/kvtest"
	"kvstore/tester"
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
	putMutex  sync.Mutex

	backoffTime time.Duration
}

// backoffWithJitter provides exponential backoff with jitter to break thundering herd.
// range between [backoff/2, backoff], where backoff = 2^retry * ck.backoffTime (with a max of 2s)
func (ck *Clerk) backoffWithJitter(retry int) {
	retry = min(retry, 5)
	backoff := ck.backoffTime << retry // this may overflow (if backoffTime is EXTREMELY huge)
	backoff = min(2*time.Second, backoff)
	jitter := time.Duration(mrand.Int63n(int64(backoff / 2)))
	time.Sleep(backoff/2 + jitter)
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leader = 0
	ck.clientId = nrand()
	ck.requestId = 0
	ck.backoffTime = 100 * time.Millisecond
	return ck
}

func (ck *Clerk) nextReqId() uint64 {
	return atomic.AddUint64(&ck.requestId, 1)
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
func (ck *Clerk) Get(key string) (string, rpcapi.Tversion, rpcapi.Err) {
	args := rpcapi.GetArgs{
		Key: key,
	}

	ck.mu.Lock()
	leader := ck.leader
	serverNum := len(ck.servers)
	ck.mu.Unlock()
	attempts := 0
	for {
		debug.D4BPrintf("clerk%v -> %v: Get %s\n", ck.clientId, ck.servers[leader], key)

		reply := rpcapi.GetReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		attempts++
		if ok {
			debug.D4BPrintf("clerk%v <- %v: Get %s, reply:%v \n", ck.clientId, ck.servers[leader], key, reply)

			switch reply.Err {
			case rpcapi.OK, rpcapi.ErrNoKey:
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Value, reply.Version, reply.Err
			case rpcapi.ErrWrongLeader:
			default:
				panic(fmt.Sprintf("undefined error: %v", reply.Err))
			}
		}

		// if rpc fails, we're not sure whether the server is the leader,
		// so we could try other servers first ?
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 {
			ck.backoffWithJitter(attempts)
		}
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
func (ck *Clerk) Put(key string, value string, version rpcapi.Tversion) rpcapi.Err {
	ck.putMutex.Lock()
	defer ck.putMutex.Unlock()

	args := rpcapi.PutArgs{
		RequestInfo: rpcapi.RequestInfo{
			ClientId:  ck.clientId,
			RequestId: ck.nextReqId(),
		},
		Key:     key,
		Value:   value,
		Version: version,
	}

	ck.mu.Lock()
	leader := ck.leader
	serverNum := len(ck.servers)
	ck.mu.Unlock()

	attempts := 0
	for {
		//debug.D4BPrintf("clerk -> %v: Put %s:%s \n", ck.servers[leader], key, value)
		debug.D4BPrintf("clerk%v -> %v: reqId:%5v, Put %s:%s \n", args.ClientId, ck.servers[leader], args.RequestId, key, value)

		reply := rpcapi.PutReply{}
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)
		attempts++
		if ok {
			//debug.D4BPrintf("clerk <- %v: Put %s:%s, reply:%v \n", ck.servers[leader], key, value, reply)
			debug.D4BPrintf("clerk%v <- %v: reqId:%5v, Put %s:%s, reply:%v \n", args.ClientId, ck.servers[leader], args.RequestId, key, value, reply)

			switch reply.Err {
			case rpcapi.OK, rpcapi.ErrNoKey, rpcapi.ErrVersion:
				ck.mu.Lock()
				ck.leader = leader
				ck.mu.Unlock()
				return reply.Err
			case rpcapi.ErrWrongLeader:
			default:
				panic(fmt.Sprintf("undefined error: %v", reply.Err))
			}
		}
		// RPC fails: it may have been executed or not.
		// Note: whether it's firstTry isn't associated with the specific server
		// but with the command. Once RPC fails, no matter whom the client communicated with,
		// firstTry need to be set to false. Because leadership can quickly change and
		// the Command propagates between the servers.
		leader = (leader + 1) % len(ck.servers)
		if attempts%serverNum == 0 {
			ck.backoffWithJitter(attempts)
		}
	}
}
