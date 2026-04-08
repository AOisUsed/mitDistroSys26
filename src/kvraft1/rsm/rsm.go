package rsm

import (
	"sync"
	"sync/atomic"
	"time"

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
			opMsg := applyMsg.Command.(Op)

			D4BPrintf("%v read apply %v at %v \n", rsm.me, opMsg.Id, commandId)

			result := rsm.sm.DoOp(opMsg.Req)

			res := applyResult{
				opId:  opMsg.Id,
				value: result,
			}

			// if this server is the leader, wake up Submit()
			rsm.mu.Lock()
			ch, exists := rsm.resultByLogId[commandId]
			rsm.mu.Unlock()

			// if a Submit is listening for it, send the result.
			if exists {
				ch <- res
			}
		} else { // if it's a snapshot, todo: do later

		}
	}

	D4BPrintf("%v applyCh closed", rsm.me)
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
	D4BPrintf("%v start Submit %v\n", rsm.me, req)
	defer D4BPrintf("%v done Submit %v\n", rsm.me, req)
	logId, _, isLeader := rsm.Raft().Start(op)
	D4BPrintf("rsm%v Start op %v: %v at %v\n", rsm.me, op.Id, op.Req, logId)

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
	stopSubmit := rsm.stopSubmit
	rsm.mu.Unlock()

	// waiting for result from raft, or term changed message

	for {
		select {
		case <-time.After(time.Millisecond * 300):
			_, isStillLeader := rsm.rf.GetState()
			if !isStillLeader {
				D4APrintf("rsm%v loses leadership, stop Submit %v\n", rsm.me, op)
				rsm.mu.Lock()
				delete(rsm.resultByLogId, logId)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		case result := <-resultCh:
			D4BPrintf("rsm%v Submit %v \n", rsm.me, op)
			rsm.mu.Lock()
			delete(rsm.resultByLogId, logId)

			// if the req at the specific log index is replaced by other request (identified by opId),
			// it means that this server is no longer a leader. so it should stop serving the pending Submits
			if result.opId != OpId {
				D4BPrintf("%v req doesn't match: expect %v, get %v\n, will stop all pending submits", rsm.me, OpId, result.opId)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
			return rpc.OK, result.value
		case <-stopSubmit: // whenever get notification of stop submit, quit the Submit immediately
			// this case is triggered when applyCh from the raft is closed, meaning that the raft is shut down.
			// so all pending submit wouldn't be able to finish and must quit quickly as well.
			D4APrintf("rsm%v: Submit %v is stopped\n", rsm.me, op)
			rsm.mu.Lock()
			delete(rsm.resultByLogId, logId)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}
}
