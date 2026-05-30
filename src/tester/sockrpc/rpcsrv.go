package sockrpc

import (
	"log"
	"net"

	"kvstore/rpc"
	"kvstore/tester/demux"
)

func SockName(endName string) string {
	return "/tmp/kvstore-" + endName
}

type RPCSrv struct {
	sock string
	l    net.Listener
	srv  *rpc.Server
}

func NewRPCSrv(sock string) *RPCSrv {
	rpcs := &RPCSrv{sock: sock}
	rpcs.srv = rpc.MakeServer()
	go rpcs.listen()
	return rpcs
}

func (rpcs *RPCSrv) Close() {
	rpcs.l.Close()
}

func (rpcs *RPCSrv) Name() string {
	return rpcs.sock
}

func (rpcs *RPCSrv) AddService(svc any) {
	rpcs.srv.AddService(rpc.MakeService(svc))
}

func (rpcs *RPCSrv) listen() {
	l, err := net.Listen("unix", SockName(rpcs.sock))
	if err != nil {
		log.Fatal("tester listen error:", err)
	}
	//log.Printf("rpcs listen %q", sockName(rpcs.sock))
	rpcs.l = l
	for {
		c, err := l.Accept()
		if err != nil {
			//log.Printf("rpcs accept err %v", err)
			return
		}
		t := demux.NewTransport(c)
		demux.NewDemuxSrv(rpcs.sock, rpcs, t)
	}
}

func (rpcs *RPCSrv) ServeRequest(clntEnd string, b []byte) ([]byte, bool) {
	req := RPCArgs{}
	rpc.Unmarshall(b, &req)
	//log.Printf("%q: dispatch %v", rpcs.sock, req.Method)
	rep, ok := rpcs.srv.Dispatch(rpcs.sock, req.Method, clntEnd, req.Args)
	return rep, ok
}
