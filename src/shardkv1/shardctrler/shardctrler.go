package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"sync"

	"6.5840/debug"
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt        *tester.Clnt
	configStore kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	clerkByGid map[tester.Tgid]*shardgrp.Clerk
	mu         sync.Mutex
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.configStore = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.clerkByGid = make(map[tester.Tgid]*shardgrp.Clerk)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {

}

// Clerk create/get clerk from config and groupId. note that if the servers of a group change, this could return outdated value
func (sck *ShardCtrler) Clerk(shardCfg *shardcfg.ShardConfig, gid tester.Tgid) *shardgrp.Clerk {
	sck.mu.Lock()
	clerk, exists := sck.clerkByGid[gid]
	if !exists {
		clerk = shardgrp.MakeClerk(sck.clnt, shardCfg.Groups[gid])
		sck.clerkByGid[gid] = clerk
	}
	sck.mu.Unlock()
	return clerk
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	marshalledCfg := cfg.String()
	sck.configStore.Put("cfg", marshalledCfg, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(newCfg *shardcfg.ShardConfig) {
	// Your code here.
	oldCfg, ver := sck.queryInternal()
	// don't change config if the newCfg has smaller config version (Num)
	if newCfg.Num <= oldCfg.Num {
		debug.D5APrintf("controller ChangeConfig called with older config: %v < current: %v , does nothing\n", oldCfg.Num, newCfg.Num)
		return
	}

	debug.D5APrintf("controller: ChangeConfigTo()\n OldConfig: Num: %v, Shards: %v\n NewConfig: Num: %v, Shards: %v\n", oldCfg.Num, oldCfg.Shards, newCfg.Num, newCfg.Shards)

	var wg sync.WaitGroup
	// compare the oldCfg and newCfg configuration, find the difference, and act accordingly
	for shid, oldGid := range oldCfg.Shards {
		// can be executed concurrently for different shard,
		// but the execution concerning one shard should be sequential (i.e. strictly follow step 1 to 3)
		newGid := newCfg.Shards[shid]

		// if the shard belongs to the same group, ignore
		if newGid == oldGid {
			continue
		}
		debug.D5APrintf("controller start moving shard %v from %v to %v\n", shid, oldGid, newGid)
		wg.Add(1)
		go func(shid shardcfg.Tshid, oldGid tester.Tgid, newGid tester.Tgid) {
			defer wg.Done()

			// 1. freeze the shard of shid in oldGid: oldGrp.freeze(shid, newCfg.Num)
			oldGrpClerk := sck.Clerk(oldCfg, oldGid)
			data, err := oldGrpClerk.FreezeShard(shid, newCfg.Num)
			if err != rpc.OK {
				// could only be 1 of 3 kinds: OK, ErrWrongGroup, ErrVersion
				debug.D5APrintf("controller -FreezeShard(shard: %v, Num: %v)-> %v failed with %v\n", shid, newCfg.Num, oldGid, err)
				return
			}

			// 2. install the shard to the newGid: newGrp.Install(shid, newCfg.Num)
			newGrpClerk := sck.Clerk(newCfg, newGid)
			err = newGrpClerk.InstallShard(shid, data, newCfg.Num)
			if err != rpc.OK {
				debug.D5APrintf("controller -InstallShard(shard: %v, stateSize: %v, Num: %v)-> %v failed with %v\n", shid, len(data), newCfg.Num, newGid, err)
				return
			}

			// 3. delete the frozen shard in oldGid: oldGrp.delete(shid, newCfg.Num)
			err = oldGrpClerk.DeleteShard(shid, newCfg.Num)
			if err != rpc.OK {
				debug.D5APrintf("controller -DeleteShard(shard: %v, Num: %v)-> %v failed with %v\n", shid, newCfg.Num, newGid, err)
				return
			}
		}(shardcfg.Tshid(shid), oldGid, newGid)
	}
	wg.Wait()

	// save newCfg to configStore
	marshalledCfg := newCfg.String()

	err := sck.configStore.Put("cfg", marshalledCfg, ver) // idempotent save
	if err != rpc.OK {
		debug.D5APrintf("controller save newCfg, err:%v\n", err)
	}
}

// queryInternal query the configStore that returns the config and the version
func (sck *ShardCtrler) queryInternal() (*shardcfg.ShardConfig, rpc.Tversion) {
	marshalledCfg, ver, err := sck.configStore.Get("cfg")

	if err != rpc.OK {
		log.Fatal("configStore Get err:", err)
	}
	shardCfg := shardcfg.FromString(marshalledCfg)
	return shardCfg, ver
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	UnmarshalledCfg, _, err := sck.configStore.Get("cfg")
	if err != rpc.OK {
		log.Fatal("configStore Get err:", err)
	}
	shardCfg := shardcfg.FromString(UnmarshalledCfg)
	return shardCfg
}
