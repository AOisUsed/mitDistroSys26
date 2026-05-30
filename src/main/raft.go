package main

import (
	"fmt"
	"os"

	"kvstore/raft"
	"kvstore/tester"
)

func main() {
	if err := tester.InitDaemon(os.Args[1:], raft.NewRfsrv); err != nil {
		fmt.Printf("%v: InitDaemon err %v", os.Args[0], err)
		os.Exit(1)
	}
}
