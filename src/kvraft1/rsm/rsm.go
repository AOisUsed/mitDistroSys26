package rsm

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/debug"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
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

	resultByLogId map[int]chan applyResult
	opId          int64
	stopSubmit    chan struct{} // stopSubmit is used to notify all pending Submit() to exit when the raft is down / terminated
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
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:            me,
		maxraftstate:  maxraftstate,
		applyCh:       make(chan raftapi.ApplyMsg),
		sm:            sm,
		resultByLogId: make(map[int]chan applyResult),
		opId:          0,
		stopSubmit:    make(chan struct{}),
	}

	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		debug.D4CPrintf("rsm%v restores Snapshot from persister\n", rsm.me)
		rsm.sm.Restore(snapshot)
	}

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.readApply()

	return rsm
}

func (rsm *RSM) nextOpId() int64 {
	return atomic.AddInt64(&rsm.opId, 1)
}

func (rsm *RSM) readApply() {
	for applyMsg := range rsm.applyCh {
		// whenever getting new apply message, meaning that either a command or a snapshot has been commited, safe to execute on the server
		if applyMsg.CommandValid { // if it's a command, let the respective Submit() rpc knows about it
			commandId := applyMsg.CommandIndex
			var applyRes applyResult
			if applyMsg.Command == nil {
				// this is a no-op command from raft
				debug.D4APrintf("rsm%v gets an no-op\n", rsm.me)
				applyRes = applyResult{opId: -1}
			} else {
				// this is a real command from client
				opMsg := applyMsg.Command.(Op)
				debug.D4BPrintf("%v read apply with opId%v at commandIndex: %v \n", rsm.me, opMsg.Id, commandId)

				result := rsm.sm.DoOp(opMsg.Req)

				// check whether raftstate exceeds the maxraftstate. if so, snapshot
				if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
					debug.D4CPrintf("rsm%v: raftstate %v exceeds the maxraftstate%v, need to snapshot", rsm.me, rsm.rf.PersistBytes(), rsm.maxraftstate)
					snapshot := rsm.sm.Snapshot()
					rsm.rf.Snapshot(commandId, snapshot)
				}

				applyRes = applyResult{
					opId:  opMsg.Id,
					value: result,
				}
			}

			// if this server is the leader, wake up Submit()
			rsm.mu.Lock()
			ch, exists := rsm.resultByLogId[commandId]
			rsm.mu.Unlock()

			// if a Submit is listening for it, send the result.
			if exists {
				ch <- applyRes
			}

		} else if applyMsg.SnapshotValid { // if it's a snapshot
			debug.D4BPrintf("rsm%v reads snapshot from applyCh, index:%v, term:%v", rsm.me, applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
			rsm.sm.Restore(applyMsg.Snapshot)

			// todo: need to think where this part should be placed
			// when snapshot is introduced, it's no longer reliable to know that a Submit() expires based only on the log overwrite
			// because snapshot brings the leap of log index.
			// In order to solve the problem, whenever an external snapshot happens, it must check all pending Submit() and let them know of their expiry
			rsm.mu.Lock()
			for id, ch := range rsm.resultByLogId {
				if id <= applyMsg.SnapshotIndex {
					ch <- applyResult{opId: -1}
					//delete(rsm.resultByLogId, id)
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
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	OpId := rsm.nextOpId()
	op := Op{Me: rsm.me, Id: OpId, Req: req}

	_, isLeader := rsm.Raft().GetState()
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	//D4BPrintf("%v start Submit %v\n", rsm.me, req)
	//defer D4BPrintf("%v done Submit %v\n", rsm.me, req)
	logId, _, isLeader := rsm.Raft().Start(op)
	debug.D4BPrintf("rsm%v Start op %v: %v at %v\n", rsm.me, op.Id, op.Req, logId)

	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	// must lock before writing the channel into rsm. otherwise,
	// rare case may happen that raft agrees on the log and wants to let the corresponding Submit() know of it,
	// but the channel isn't yet created,
	// so the notification is lost. (see details in readApply())
	// lock rsm prevents readApply() from reading the resultByLogId, so that this case won't happen.
	// But this may decrease the throughput of the system.

	resultCh := make(chan applyResult, 1)
	rsm.resultByLogId[logId] = resultCh
	debug.D4APrintf("rsm%v: resultCh for %v is prepared", rsm.me, logId)
	stopSubmit := rsm.stopSubmit
	rsm.mu.Unlock()

	// waiting for result from raft, or term changed message

	for {
		select {
		//case <-time.After(time.Millisecond * 300):
		//	// N.B. problem with returning ErrWrongLeader immediately after its leadership expires:
		//	// the server doesn't know whether the Command actually will get committed or not, and cannot differentiate the two cases:
		//	// case 1: it loses leadership before log getting duplicated to the majority and the log has no chance to get committed.
		//	// case 2: it loses leadership before log getting committed, but the log has the potential to be committed (it may sound weird, but it refers to the case where the log has been duplicated to the majority (as well as the upcoming new leader) and this server loses leadership. the new leader then commits it indirectly in its own new term).
		//	//
		//	// and these two cases should be handled differently.
		//	// in case 1: this server should reply ErrWrongLeader, because the operation wasn't successful and it's no longer a leader
		//	// in case 2: this server should reply OK, because the operation was/will be committed by the new leader and will be eventually applied to state machine.
		//	//
		//	// therefore, if it returns ErrWrongLeader after losing leadership in the case that the Command actually got committed (case 2), it delivers wrong message to the client.
		//	// because client will retry the same Command, in believing that this Command never got commited and executed on all raft servers, while it's actually done!
		//	// despite that exact-once semantics can be achieved from the server side to cancel duplicate operations,
		//	// it doesn't mean that rsm should allow this happen.
		//
		//	// final solution: introducing no-op apply in raft. When a server ascends as the leader, it appends a no-op log of its term,
		//	// and this log will be eventually applied to state machine. for the former leader who started a Submit(), but never got committed, this log will overwrite the existing log dedicated to that Submit(), and the result be sent to resultCh,
		//	// So the server will find out by comparing the opId that its Submit() request didn't get committed and thus return ErrWrongLeader to client.
		//	// But if the Submit() it started get committed by other leader, even if it has lost leadership, it knows that its Submit() got committed and thus it should reply OK to client.
		//	// this mechanism allows the server to know how to reply client according to valid evidence but not according to its raft state.
		//
		//	_, isStillLeader := rsm.rf.GetState()
		//	if !isStillLeader {
		//		debug.D4APrintf("rsm%v loses leadership, stop Submit %v\n", rsm.me, op)
		//		rsm.mu.Lock()
		//		if _, exists := rsm.resultByLogId[logId]; !exists {
		//
		//		}
		//		delete(rsm.resultByLogId, logId)
		//		rsm.mu.Unlock()
		//		return rpc.ErrWrongLeader, nil
		//	}
		case result := <-resultCh:
			debug.D4BPrintf("rsm%v Submit %v \n", rsm.me, op)
			rsm.mu.Lock()
			delete(rsm.resultByLogId, logId)

			// if the req at the specific log index is replaced by other request (identified by opId),
			// it means that this server is no longer a leader. so it should stop serving the pending Submits
			if result.opId != OpId {
				debug.D4BPrintf("%v req doesn't match: expect %v, get %v\n, will stop all pending submits", rsm.me, OpId, result.opId)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
			return rpc.OK, result.value
		case <-stopSubmit: // whenever get notification of stop submit, quit the Submit immediately
			// this case is triggered when applyCh from the raft is closed, meaning that the raft is shut down.
			// so all pending submit wouldn't be able to finish and must quit quickly as well.
			debug.D4APrintf("rsm%v: Submit %v is stopped\n", rsm.me, op)
			rsm.mu.Lock()
			delete(rsm.resultByLogId, logId)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}
}
