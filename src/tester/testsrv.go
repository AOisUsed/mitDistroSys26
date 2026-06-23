package tester

import (
	//"log"

	"kvstore/debug"
	"kvstore/tester/sockrpc"
)

type TesterRPC struct {
	*sockrpc.RPCSrv
	cfg *Config
}

// Make a RPC server for the tester to receive RPCs from daemons
func newTesterRPCSrv(cfg *Config) *TesterRPC {
	trpc := &TesterRPC{
		cfg: cfg,
	}
	//log.Printf("newTesterRPCSrv %v", cfg.endName)
	trpc.RPCSrv = sockrpc.NewRPCSrv(cfg.endName)
	trpc.RPCSrv.AddService(trpc)
	return trpc
}

func (trpc *TesterRPC) cleanup() {
	//log.Printf("TesterRPCsrv: close %v", trpc.cfg.endName)
	trpc.Close()
}

type ForwardArgs struct {
	Method string
	End    string // client end for server
	Args   []byte
	Id     int64
}

type ForwardReply struct {
	Rep []byte
	Ok  bool
}

// Forward RPC to a deamon through the lab net
func (trpc *TesterRPC) Forward(args *ForwardArgs, reply *ForwardReply) {
	//log.Printf("%v: Forward args %v to end %q %d", trpc.Name(), args.Method, args.End, args.Id)
	end := trpc.cfg.net.LookupEnd(args.End)
	reply.Rep, reply.Ok = end.Forward(args.Method, args.Args)
}

// ----- 观测日志转发（子进程 → 主进程）-----

type PostObserveLogArgs struct {
	Tag  string
	Text string
}

type PostObserveLogReply struct{}

// PostObserveLog 接收子进程转发过来的观测日志，由主进程根据 toggle 决定是否写入环形缓冲区
func (trpc *TesterRPC) PostObserveLog(args *PostObserveLogArgs, reply *PostObserveLogReply) {
	debug.ObservePushTagged(args.Tag, args.Text)
}
