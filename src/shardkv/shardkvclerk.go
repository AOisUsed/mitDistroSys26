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
	"math/rand"
	"sync"
	"sync/atomic"

	"kvstore/debug"
	"kvstore/shardkv/shardcfg"
	"kvstore/shardkv/shardgrp"

	"kvstore/kvsrv/rpcapi"
	"kvstore/kvtest"
	"kvstore/shardkv/shardctrler"
	"kvstore/tester"
)

// Clerk shardkv.Clerk is directly used by client to execute PUT, GET
type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk

	clerkId      uint64
	requestId    uint64 // this should increase monotonically. if the client is to reuse the clientId, it must persist requestId.
	cachedConfig *shardcfg.ShardConfig
	fetching     bool          // whether a config Query is in flight
	fetchingDone chan struct{} // channel used to notify that the fetching is done
	mu           sync.RWMutex
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
	ck.requestId = 0
	ck.clerkId = rand.Uint64()
	ck.fetching = false
	ck.fetchingDone = make(chan struct{})
	return ck
}

func (ck *Clerk) nextReqId() uint64 {
	return atomic.AddUint64(&ck.requestId, 1)
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// getConfig get cached config (if no cached one, call refreshConfig() to get from configStore)
func (ck *Clerk) getConfig() *shardcfg.ShardConfig {
	ck.mu.RLock()
	fetching := ck.fetching
	fetchingDone := ck.fetchingDone
	cfg := ck.cachedConfig
	ck.mu.RUnlock()

	// return cached config if it exits and doesn't need to refresh
	if !fetching && cfg != nil {
		return cfg
	}

	// else fetch the latest config from config store
	ck.refreshConfig()
	<-fetchingDone // waiting for fetching done
	ck.mu.RLock()
	cachedConfig := ck.cachedConfig
	ck.mu.RUnlock()
	return cachedConfig
}

// refreshConfig single-flight
func (ck *Clerk) refreshConfig() {
	ck.mu.Lock()
	fetching := ck.fetching
	defer ck.mu.Unlock()

	if fetching { // if one Query is in flight, quit this session
		return
	} else { // if no Query is in flight, fetch config from configStore
		ck.fetching = true
		ck.mu.Unlock()
		cfg := ck.sck.Query()
		ck.mu.Lock()
		fetchingDone := ck.fetchingDone       // 1. get channel
		ck.cachedConfig = cfg                 // 2. save config
		ck.fetching = false                   // 3. mark that no fetching is ongoing
		ck.fetchingDone = make(chan struct{}) // 4. make new channel for next refreshConfig, because using close channel to broadcast can be done only once with one channel
		close(fetchingDone)                   // 5. close channel to notify all waiting getConfig() to acquire freshly updated config
	}
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpcapi.Tversion, rpcapi.Err) {
	// You will have to modify this function.

	// use key to get the shardId
	shardId := shardcfg.Key2Shard(key)

	for {
		cachedConfig := ck.getConfig()
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
			clerk = shardgrp.MakeClerk(ck.clnt, servers, ck.clerkId)
			ck.mu.Lock()
			ck.rcks[gid] = clerk
			ck.mu.Unlock()
		}

		debug.D5APrintf("client -Get(key: %v) in shard %v-> group %v\n", key, shardId, gid)
		val, version, rpcErr := clerk.Get(key)
		debug.D5APrintf("client <-Get(key: %v) in shard %v- group %v (key: %v, version: %v, Err: %v)\n", key, shardId, gid, val, version, rpcErr)
		if rpcErr == rpcapi.ErrWrongGroup || rpcErr == rpcapi.ErrRetryExhausted {
			debug.ObserveFaultPrintf("分片客户端: Get(%s) -> %v, 重试中", key, rpcErr)
			ck.refreshConfig()
		} else {
			debug.ObserveKVRequestPrintf("分片客户端: Get(%s) -> OK (值=%s, 版本=%d)", key, val, version)
			return val, version, rpcErr
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpcapi.Tversion) rpcapi.Err {
	// You will have to modify this function.

	reqId := ck.nextReqId()
	// use key to get the shardId
	shardId := shardcfg.Key2Shard(key)
	for {
		config := ck.getConfig()
		// consult the config to know the shard group responsible for the key
		gid, servers, ok := config.GidServers(shardId)
		if !ok {
			log.Fatal("group doesn't exist")
		}

		// check if the clerk communicating to certain group has been cached
		ck.mu.RLock()
		clerk, exists := ck.rcks[gid]
		ck.mu.RUnlock()

		if !exists {
			clerk = shardgrp.MakeClerk(ck.clnt, servers, ck.clerkId)
			ck.mu.Lock()
			ck.rcks[gid] = clerk
			ck.mu.Unlock()
		}
		debug.D5APrintf("client -Put(key: %v, value: %v, version: %v) in shard %v-> group %v\n", key, value, version, shardId, gid)
		rpcErr := clerk.Put(reqId, key, value, version)
		debug.D5APrintf("client <-Put(key: %v, value: %v, version: %v) in shard %v- group %v (Err: %v)\n", key, value, version, shardId, gid, rpcErr)
		switch rpcErr {
		case rpcapi.ErrWrongGroup, rpcapi.ErrRetryExhausted:
			debug.ObserveFaultPrintf("分片客户端: Put(%s : %s) -> %v, 重试中", key, value, rpcErr)
			ck.refreshConfig()
		case rpcapi.OK, rpcapi.ErrVersion, rpcapi.ErrNoKey:
			debug.ObserveKVRequestPrintf("分片客户端: Put(%s : %s) -> %s", key, value, rpcErr)
			return rpcErr
		default:
			log.Fatalf("undefined rpc Err: %v", rpcErr)
		}
	}
}
