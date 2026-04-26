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

	"6.5840/debug"
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
	ck.fetching = false
	ck.fetchingDone = make(chan struct{})
	return ck
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

	if !fetching {
		if cfg != nil {
			// if not fetching and cachedCfg exists, return config
			return cfg
		} else {
			// if not fetching and cachedCfg doesn't exist, fetch config (by calling refreshConfig)
			ck.refreshConfig()
			<-fetchingDone // waiting for fetching done
			ck.mu.RLock()
			cachedConfig := ck.cachedConfig
			ck.mu.RUnlock()
			return cachedConfig
		}
	} else { // is fetching config
		<-fetchingDone // waiting for fetching done
		ck.mu.RLock()
		cachedConfig := ck.cachedConfig
		ck.mu.RUnlock()
		return cachedConfig
	}
}

// refreshConfig
func (ck *Clerk) refreshConfig() {
	ck.mu.RLock()
	fetching := ck.fetching
	ck.mu.RUnlock()

	if fetching { // if one Query is in flight, ignore
		return
	} else { // if no Query is ongoing, fetch config from configStore
		cfg := ck.sck.Query()
		ck.mu.Lock()
		fetchingDone := ck.fetchingDone       // 1. get channel
		ck.cachedConfig = cfg                 // 2. save config
		ck.fetching = false                   // 3. mark that no fetching is ongoing
		ck.fetchingDone = make(chan struct{}) // 4. make new channel for next refreshConfig, because using close channel to broadcast can be done only once with one channel
		ck.mu.Unlock()
		close(fetchingDone) // 5. close channel to notify all waiting getConfig() to acquire freshly updated config
	}
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
	retryExhaustedCnt := 0

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
			clerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.mu.Lock()
			ck.rcks[gid] = clerk
			ck.mu.Unlock()
		}

		debug.D5APrintf("client -Get(key: %v) in shard %v-> group %v\n", key, shardId, gid)
		val, version, rpcErr := clerk.Get(key)
		debug.D5APrintf("client <-Get(key: %v) in shard %v- group %v (key: %v, version: %v, Err: %v)\n", key, shardId, gid, val, version, rpcErr)
		if rpcErr == rpc.ErrWrongGroup {
			retryExhaustedCnt = 0
			ck.refreshConfig()
		} else if rpcErr == rpc.ErrRetryExhausted {
			// transient no-leader window. periodically refresh config to avoid
			// being stuck on stale group mapping (e.g. contacting an offline group).
			retryExhaustedCnt++
			if retryExhaustedCnt%2 == 0 {
				ck.refreshConfig()
			}
			time.Sleep(100 * time.Millisecond)
		} else {
			retryExhaustedCnt = 0
			return val, version, rpcErr
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	// use key to get the shardId
	shardId := shardcfg.Key2Shard(key)
	retryExhaustedCnt := 0
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
			clerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.mu.Lock()
			ck.rcks[gid] = clerk
			ck.mu.Unlock()
		}
		debug.D5APrintf("client -Put(key: %v, value: %v, version: %v) in shard %v-> group %v\n", key, value, version, shardId, gid)
		rpcErr := clerk.Put(key, value, version)
		debug.D5APrintf("client <-Put(key: %v, value: %v, version: %v) in shard %v- group %v (Err: %v)\n", key, value, version, shardId, gid, rpcErr)
		switch rpcErr {
		case rpc.ErrWrongGroup:
			retryExhaustedCnt = 0
			ck.refreshConfig()
		case rpc.ErrRetryExhausted:
			// transient no-leader window; periodically refresh to escape stale config.
			retryExhaustedCnt++
			if retryExhaustedCnt%2 == 0 {
				ck.refreshConfig()
			}
			time.Sleep(100 * time.Millisecond)
		case rpc.ErrMaybe:
		case rpc.OK, rpc.ErrVersion, rpc.ErrNoKey:
			retryExhaustedCnt = 0
			return rpcErr
		default:
			log.Fatalf("undefined rpc Err: %v", rpcErr)
		}
	}
}
