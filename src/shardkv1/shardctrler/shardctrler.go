package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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
	controllerId uint64
	clerkByGid   map[tester.Tgid]*shardgrp.Clerk
	mu           sync.Mutex
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.configStore = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.controllerId = rand.Uint64()
	sck.clerkByGid = make(map[tester.Tgid]*shardgrp.Clerk)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	// get both currentConfig and nextConfig
	currentConfig, ver := sck.queryCurrentConfig()
	nextConfig, _ := sck.queryNextConfig()

	// if currentConfig has smaller Num, it means that previous shard reconfiguration wasn't completed, therefore redo it.
	if currentConfig.Num < nextConfig.Num {
		sck.migrateShards(currentConfig, ver, nextConfig)
	}
}

// Clerk create/get clerk from config and groupId. note that if the servers of a group change, this could return wrong value
func (sck *ShardCtrler) clerk(shardCfg *shardcfg.ShardConfig, gid tester.Tgid) *shardgrp.Clerk {
	sck.mu.Lock()
	clerk, exists := sck.clerkByGid[gid]
	if !exists {
		clerk = shardgrp.MakeClerk(sck.clnt, shardCfg.Groups[gid], sck.controllerId)
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
	marshalledCfg := cfg.String()
	// initialise both currentConfig and nextConfig into the config store.
	// when a controller recover from crash, it will find out that currentConfig and nextConfig are identical, meaning this config is applied to the shardkv
	sck.configStore.Put("currentConfig", marshalledCfg, 0)
	sck.configStore.Put("nextConfig", marshalledCfg, 0)
}

// saveNextConfig is called when the controller wants to reconfigure shard, NextConfig indicates an intention. this is an CAS operation
// if it fails to save NextConfig, ChangeConfigTo should exit immediately because another controller has successfully written next.
func (sck *ShardCtrler) saveNextConfig(newCfg *shardcfg.ShardConfig) bool {
	existingNextConfig, ver := sck.queryNextConfig()
	if existingNextConfig.Num >= newCfg.Num { // if someone has saved NextConfig of smaller/equal Num, this should not overwrite its NextConfig
		return false
	}
	marshalledCfg := newCfg.String()
	err := sck.configStore.Put("nextConfig", marshalledCfg, ver)

	switch err {
	case rpc.OK: // successful save
		return true
	case rpc.ErrVersion: // CAS save fails, meaning that other controller changed it.
		return false
	case rpc.ErrMaybe: // if unsure whether save NextConfig was successful, query and compare
		writtenCfg, _ := sck.queryNextConfig()
		if writtenCfg.EqualTo(newCfg) {
			return true
		}
		return false
	default:
		log.Fatalf("undefined rpc err: %v", err)
	}
	return false
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(newCfg *shardcfg.ShardConfig) {
	// Your code here.
	oldCfg, ver := sck.queryCurrentConfig()
	// don't change config if newCfg has smaller config version (Num)
	if newCfg.Num <= oldCfg.Num {
		debug.D5APrintf("controller ChangeConfig called with older config: %v < current: %v , does nothing\n", oldCfg.Num, newCfg.Num)
		return
	}
	if !sck.saveNextConfig(newCfg) { // it fails to save reconfiguration intention
		return
	}

	debug.D5APrintf("controller: ChangeConfigTo():\n OldConfig: Num: %v, Shards: %v\n NewConfig: Num: %v, Shards: %v\n", oldCfg.Num, oldCfg.Shards, newCfg.Num, newCfg.Shards)
	sck.migrateShards(oldCfg, ver, newCfg)
}

// check if the config has been superseded by a newer config
func (sck *ShardCtrler) isSuperseded(newCfg *shardcfg.ShardConfig) bool {
	curCfg, _ := sck.queryCurrentConfig()
	return curCfg.Num >= newCfg.Num
}

// migrateShards is called in ChangeConfigTo and InitController. ver is the version of currentConfig value, for CAS Put
func (sck *ShardCtrler) migrateShards(oldCfg *shardcfg.ShardConfig, ver rpc.Tversion, newCfg *shardcfg.ShardConfig) {
	var wg sync.WaitGroup
	var anyFailed atomic.Bool

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

			// each retry internally does len(servers) round-robin + 100ms backoff,
			//
			// this maximum retry num is introduced as there is a corner case as follows:
			// t0: controller A migrates shards, but crashes soon at the last step - Put "currentConfig" to configStore,
			//     meaning that the shard migration was completed but the currentConfig wasn't updated at the configStore.
			// t1: some groups leave because after all shards of it have been removed.
			// t2: controller B picks up the work undone from InitConfig().
			//	   but since some groups have already left, it gets ErrRetryExhausted indefinitely.
			//
			// therefore we need maximum retries, if the maximum is reached, we can conclude that it's due to the group leave.
			// because kv server is fault-tolerant with raft, in the case that the majority is dead isn't unlikely.
			const shardOpRetries = 5

			// 1. freeze the shard of shid in oldGid: oldGrp.freeze(shid, newCfg.Num)
			oldGrpClerk := sck.clerk(oldCfg, oldGid)
			var data []byte
			var err rpc.Err = rpc.ErrRetryExhausted
			for retries := 0; retries < shardOpRetries && err == rpc.ErrRetryExhausted; retries++ {
				data, err = oldGrpClerk.FreezeShard(shid, newCfg.Num)
				debug.D5APrintf("controller -FreezeShard(shard: %v, Num: %v)-> %v, Err: %v\n", shid, newCfg.Num, oldGid, err)
				if err == rpc.ErrRetryExhausted {
					if sck.isSuperseded(newCfg) {
						anyFailed.Store(true)
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
			if err == rpc.ErrRetryExhausted {
				debug.D5APrintf("controller -FreezeShard(shard:%v) gave up after %v retries\n", shid, shardOpRetries)
				anyFailed.Store(true)
				return
			}

			// 2. install the shard to the newGid: newGrp.Install(shid, newCfg.Num)
			newGrpClerk := sck.clerk(newCfg, newGid)
			err = rpc.ErrRetryExhausted // set to ErrRetryExhausted to trigger action
			for retries := 0; retries < shardOpRetries && err == rpc.ErrRetryExhausted; retries++ {
				err = newGrpClerk.InstallShard(shid, data, newCfg.Num)
				debug.D5APrintf("controller -InstallShard(shard: %v, stateSize: %v, Num: %v)-> %v Err: %v\n", shid, len(data), newCfg.Num, newGid, err)
				if err == rpc.ErrRetryExhausted {
					if sck.isSuperseded(newCfg) {
						anyFailed.Store(true)
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
			if err == rpc.ErrRetryExhausted {
				debug.D5APrintf("controller -InstallShard(shard:%v) gave up after %v retries\n", shid, shardOpRetries)
				anyFailed.Store(true)
				return
			}

			// 3. delete the frozen shard in oldGid: oldGrp.delete(shid, newCfg.Num)
			err = rpc.ErrRetryExhausted // set to ErrRetryExhausted to trigger action
			for retries := 0; retries < shardOpRetries && err == rpc.ErrRetryExhausted; retries++ {
				err = oldGrpClerk.DeleteShard(shid, newCfg.Num)
				debug.D5APrintf("controller -DeleteShard(shard: %v, Num: %v)-> %v Err: %v\n", shid, newCfg.Num, newGid, err)
				if err == rpc.ErrRetryExhausted {
					if sck.isSuperseded(newCfg) {
						anyFailed.Store(true)
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
			if err == rpc.ErrRetryExhausted {
				debug.D5APrintf("controller -DeleteShard(shard:%v) gave up after %v retries\n", shid, shardOpRetries)
				anyFailed.Store(true)
				return
			}
		}(shardcfg.Tshid(shid), oldGid, newGid)
	}
	wg.Wait()

	if anyFailed.Load() {
		return
	}

	// save newCfg to configStore
	marshalledCfg := newCfg.String()
	// we actually don't care about the Err returned. Because in the scenario,
	// there are three possible rpc err: OK, ErrVersion, ErrMaybe (configstore is a simple kv server without FT)
	// 	- ErrVersion means that another controller wrote currentConfig,
	// 	- ErrMaybe means that either this controller successfully wrote currentConfig in the previous rpc, but fail to receive the reply; or another controller did,
	//  - OK means that the PUT is successful.
	// all three cases indicate one truth: currentConfig has been updated, therefore no need to distinguish them.
	// the correctness assurance is that currentConfig updated by no matter which controller is the same.
	// this is guaranteed because currentConfig comes from nextConfig stored in configStore as a universal truth.
	sck.configStore.Put("currentConfig", marshalledCfg, ver)
}

// queryCurrentConfig query the configStore that returns the config and the version
func (sck *ShardCtrler) queryCurrentConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	marshalledCfg, ver, err := sck.configStore.Get("currentConfig")
	if err == rpc.ErrNoKey {
		log.Fatalf("currentConfig doesn't exist")
	}
	shardCfg := shardcfg.FromString(marshalledCfg)
	return shardCfg, ver
}

func (sck *ShardCtrler) queryNextConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	marshalledCfg, ver, err := sck.configStore.Get("nextConfig")
	if err == rpc.ErrNoKey {
		log.Fatalf("nextConfig doesn't exist")
	}
	shardCfg := shardcfg.FromString(marshalledCfg)
	return shardCfg, ver
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	marshalledCfg, _, err := sck.configStore.Get("currentConfig")
	if err != rpc.OK {
		log.Fatal("configStore get currentConfig err:", err)
	}
	shardCfg := shardcfg.FromString(marshalledCfg)
	return shardCfg
}

// QueryNext returns the next configuration (the pending migration intent).
func (sck *ShardCtrler) QueryNext() *shardcfg.ShardConfig {
	marshalledCfg, _, err := sck.configStore.Get("nextConfig")
	if err != rpc.OK {
		log.Fatal("configStore get nextConfig err:", err)
	}
	shardCfg := shardcfg.FromString(marshalledCfg)
	return shardCfg
}

// HasPendingMigration returns true if there is an incomplete reconfiguration.
func (sck *ShardCtrler) HasPendingMigration() bool {
	cur := sck.Query()
	next := sck.QueryNext()
	return next.Num > cur.Num
}
