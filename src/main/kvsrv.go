package main

import (
	"fmt"
	"os"

	"kvstore/kvsrv"
	"kvstore/tester"
)

func main() {
	if err := tester.InitDaemon(os.Args[1:], kvsrv.StartKVServer); err != nil {
		fmt.Printf("%v: InitDaemon err %v", os.Args[0], err)
		os.Exit(1)
	}
}
