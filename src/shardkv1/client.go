package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"sync"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.

	cachedConfig *shardcfg.ShardConfig

	mu sync.RWMutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.

	// use key to get the shardId
	shardId := shardcfg.Key2Shard(key)

	// check whether it has cachedConfig
	ck.mu.RLock()
	cachedConfig := ck.cachedConfig
	ck.mu.RUnlock()

	// get the shard config from controller by query, if no cachedConfig exists
	if cachedConfig == nil { // todo: concurrent invocation can result in duplicated unnecessary queries.
		cachedConfig = ck.sck.Query() // should be outside the lock
		ck.mu.Lock()
		ck.cachedConfig = cachedConfig
		ck.mu.Unlock()
	}

	// consult the config to know the shard group responsible for the key
	gid, servers, ok := cachedConfig.GidServers(shardId)
	if !ok {
		log.Fatal("group doesn't exist")
	}

	// check if the clerk communicating to certain group has been cached
	ck.mu.RLock()
	clerk, exists := ck.rcks[gid]
	ck.mu.RUnlock()

	if !exists {
		clerk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.mu.Lock()
		ck.rcks[gid] = clerk
		ck.mu.Unlock()
	}

	return clerk.Get(key)
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	// use key to get the shardId
	shardId := shardcfg.Key2Shard(key)

	// check whether it has cachedConfig
	ck.mu.RLock()
	cachedConfig := ck.cachedConfig
	ck.mu.RUnlock()

	// get the shard config from controller by query, if no cachedConfig exists
	if cachedConfig == nil { // todo: concurrent invocation can result in duplicated unnecessary queries.
		cachedConfig = ck.sck.Query() // should be outside the lock
		ck.mu.Lock()
		ck.cachedConfig = cachedConfig
		ck.mu.Unlock()
	}

	// consult the config to know the shard group responsible for the key
	gid, servers, ok := cachedConfig.GidServers(shardId)
	if !ok {
		log.Fatal("group doesn't exist")
	}

	// check if the clerk communicating to certain group has been cached
	ck.mu.RLock()
	clerk, exists := ck.rcks[gid]
	ck.mu.RUnlock()

	if !exists {
		clerk = shardgrp.MakeClerk(ck.clnt, servers)
		ck.mu.Lock()
		ck.rcks[gid] = clerk
		ck.mu.Unlock()
	}
	return clerk.Put(key, value, version)
}
