package rsm

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"kvstore/debug"
	"kvstore/kvsrv/rpcapi"
	"kvstore/raft"
	"kvstore/raftapi"
	"kvstore/rpc"
	"kvstore/tester"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

type applyResult struct {
	opId  int64
	value any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	resultByCommandId map[int]chan applyResult
	opId              int64
	stopSubmit        chan struct{} // stopSubmit is used to notify all pending Submit() to exit when the raft is down / terminated
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*rpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine, groupLabel string) *RSM {
	rsm := &RSM{
		me:                me,
		maxraftstate:      maxraftstate,
		applyCh:           make(chan raftapi.ApplyMsg),
		sm:                sm,
		resultByCommandId: make(map[int]chan applyResult),
		opId:              0,
		stopSubmit:        make(chan struct{}),
	}

	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		debug.D4CPrintf("rsm%v restores Snapshot from persister\n", rsm.me)
		//debug.ObserveSnapshotPrintf("rsm-%v: 从 persister 恢复 snapshot (大小=%d 字节)", rsm.me, len(snapshot))
		rsm.sm.Restore(snapshot)
	}

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh, groupLabel)
	}

	go rsm.readApply()

	return rsm
}

func (rsm *RSM) nextOpId() int64 {
	return atomic.AddInt64(&rsm.opId, 1)
}

func (rsm *RSM) readApply() {
	for applyMsg := range rsm.applyCh {
		// whenever getting new apply message, meaning that either a command or a snapshot has been committed, safe to execute on the server
		if applyMsg.CommandValid { // if it's a command, let the respective Submit() rpc knows about it
			commandId := applyMsg.CommandIndex
			var applyRes applyResult

			opMsg := applyMsg.Command.(Op)
			debug.D4APrintf("%v read apply with opId%v at commandIndex: %v \n", rsm.me, opMsg.Id, commandId)

			result := rsm.sm.DoOp(opMsg.Req)

			// check whether raftstate exceeds the maxraftstate. if so, snapshot
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
				raftstateSizeBefore := rsm.rf.PersistBytes()
				debug.D4CPrintf("rsm%v: raftstate %v exceeds the maxraftstate%v, need to snapshot", rsm.me, raftstateSizeBefore, rsm.maxraftstate)
				//debug.ObserveSnapshotPrintf("rsm-%v: 触发快照 (raftstate %v >= maxraftstate %v)", rsm.me, raftstateSizeBefore, rsm.maxraftstate)
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(commandId, snapshot)
				debug.D5APrintf("rsm%v: exceeded maxraftstate %v, took snapshot raftstateSize %v -> %v, reqId: %v, commandId: %v, with snapshot size: %v \n", rsm.me, rsm.maxraftstate, raftstateSizeBefore, rsm.rf.PersistBytes(), opMsg.Id, commandId, len(snapshot))
			}

			applyRes = applyResult{
				opId:  opMsg.Id,
				value: result,
			}

			// wake up Submit() if there is one waiting for apply
			rsm.mu.Lock()
			ch, exists := rsm.resultByCommandId[commandId]
			rsm.mu.Unlock()

			// and send the result.
			if exists {
				ch <- applyRes
			}

		} else if applyMsg.SnapshotValid { // if it's a snapshot
			debug.D4BPrintf("rsm%v reads snapshot from applyCh, index:%v, term:%v", rsm.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			//debug.ObserveSnapshotPrintf("rsm-%v: 收到 InstallSnapshot(index=%v, term=%v), 正在恢复状态机", rsm.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			rsm.sm.Restore(applyMsg.Snapshot)

			// when snapshot is introduced, it's no longer reliable to know that a Submit() expires based only on the log overwrite
			// because snapshot brings the leap of log index.
			// In order to solve the problem, whenever an external snapshot happens, it must let all pending Submit() know of their expiry
			rsm.mu.Lock()
			for id, ch := range rsm.resultByCommandId {
				if id <= applyMsg.SnapshotIndex {
					ch <- applyResult{opId: -1}
				}
			}
			rsm.mu.Unlock()
		} else {
			log.Fatal("wrong type of apply Message")
		}
	}

	debug.D4BPrintf("%v applyCh closed", rsm.me)
	// let all pending Submit() know that the raft has shutdown, therefore not possible to push forward apply. and they should quit
	close(rsm.stopSubmit)
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
// only returns rpcapi.OK or rpcapi.ErrWrongLeader
func (rsm *RSM) Submit(req any) (rpcapi.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	// note that this id(OpId) is only observable and usable on this server, not a global one.

	OpId := rsm.nextOpId()
	op := Op{Me: rsm.me, Id: OpId, Req: req}

	_, isLeader := rsm.Raft().GetState()
	if !isLeader {
		return rpcapi.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	//D4BPrintf("%v start Submit %v\n", rsm.me, req)
	//defer D4BPrintf("%v done Submit %v\n", rsm.me, req)
	commandId, termAtStart, isLeader := rsm.Raft().Start(op)

	if !isLeader {
		rsm.mu.Unlock()
		return rpcapi.ErrWrongLeader, nil
	}

	debug.D4APrintf("rsm%v Start op %v: %v at %v.\n", rsm.me, op.Id, op.Req, commandId)

	// must lock before writing the channel into rsm. otherwise,
	// rare case may happen that raft commits the log extremely fast and wants to let the corresponding Submit() know of it,
	// but the channel isn't yet created,
	// so the notification is lost. (see details in readApply())
	// lock rsm prevents readApply() from reading the resultByCommandId, so that this case won't happen.

	resultCh := make(chan applyResult, 1)
	rsm.resultByCommandId[commandId] = resultCh
	debug.D4APrintf("rsm%v: resultCh for op %v is prepared", rsm.me, OpId)
	stopSubmit := rsm.stopSubmit
	rsm.mu.Unlock()

	// if the leader is partitioned and the command cannot be committed,
	// this timeout prevents the client from hanging forever.
	// but the value is tentative and should be adjusted according to the network quality (better quality: shorter deadline)
	// nevertheless, no matter what value, this doesn't affect the correctness, and won't impact performance too much.
	submissionDeadline := time.Now().Add(2 * time.Second)

	// waiting for result from raft, term/leadership changed message, or deadline

	for {
		select {
		case <-time.After(time.Millisecond * 100):
			// if the server finds out that it's no longer the leader, it should reply ErrWrongLeader. But the semantics is twofold.
			// the server doesn't know whether the Command actually will get committed or not, and cannot differentiate the two cases:
			// case 1: it loses leadership before log getting duplicated to the majority and the log has no chance to get committed.
			// case 2: it loses leadership before log getting committed, but the log has the potential to be committed (this refers to the case where the log has been duplicated to the majority, and this server loses leadership. the new leader then commits it indirectly in its own new term).
			//
			// so the two cases are different, but from the perspective of the ex-leader, it cannot know which is the case and could only reply ErrWrongLeader
			// in case 1: this server replies ErrWrongLeader, because the operation wasn't successful and it's no longer a leader
			// in case 2: this server replies ErrWrongLeader, while the operation was/will be committed by the new leader and will be eventually applied to state machine.
			// 			  in this sense, the server "lies" because the operation will be successful.
			//			  but we'll leave the correction of semantics to the new leader.
			//			  as it replies ErrWrongLeader, the client will retry sending the same command until it finds the current leader.
			//			  so when the leader sees the command, it has to know whether this command was actually applied. If it did, the leader does nothing and reply OK.
			//			  But if the command wasn't applied, it follows the normal process of a Submit()
			//
			// In conclusion, whenever a server finds out that it's no longer the leader, and has not observed the command applied (which is achieved by receiving result from resultCh), it replies ErrWrongLeader,
			// and it depends on the current leader to amend the possibly misleading information it sends to the client.

			// by the way, 100ms polling is a magic number, but it's not entirely arbitrary:
			// 		we want to know the leadership/term change ASAP, but too frequent check is a waste of CPU!
			//		so i make this the same as raft heartbeat interval.
			//		it's better if raft state change could be pushed to rsm, so we don't have to poll but too sad raft interface doesn't have this method :(
			currentTerm, isStillLeader := rsm.rf.GetState()
			if !isStillLeader || termAtStart != currentTerm || // if it finds out that it's no longer the leader,
				time.Now().After(submissionDeadline) { // or it's still leader but the request hasn't got committed when meeting deadline,
				// suggesting that this server is possibly in network partition,
				// this server gives up and "lie" (saying it's not the leader, but it ACTUALLY is). let [client retry + new leader deduplicate] amend this lie.
				debug.D4APrintf("rsm%v loses leadership, stop Submit %v\n", rsm.me, op)
				rsm.mu.Lock()
				delete(rsm.resultByCommandId, commandId)
				rsm.mu.Unlock()
				return rpcapi.ErrWrongLeader, nil
			}
		case result := <-resultCh:
			debug.D4APrintf("rsm%v Submited %v \n", rsm.me, op)
			rsm.mu.Lock()
			delete(rsm.resultByCommandId, commandId)

			// if the req at the specific log index is replaced by other request (identified by opId),
			// it means that that Submit() log was overwritten, so it won't have any chance to be committed, return ErrWrongLeader
			if result.opId != OpId {
				debug.D4APrintf("%v req doesn't match: expect %v, get %v\n, will reply ErrWrongLeader\n", rsm.me, OpId, result.opId)
				rsm.mu.Unlock()
				return rpcapi.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
			return rpcapi.OK, result.value
		case <-stopSubmit: // whenever get notification of stop submit, quit the Submit immediately
			// this case is triggered when applyCh from the raft is closed, meaning that the raft is shut down.
			// so all pending submit wouldn't be able to finish and must quit quickly as well.
			debug.D4APrintf("rsm%v: Submit %v is stopped\n", rsm.me, op)
			rsm.mu.Lock()
			delete(rsm.resultByCommandId, commandId)
			rsm.mu.Unlock()
			return rpcapi.ErrWrongLeader, nil
		}
	}
}
