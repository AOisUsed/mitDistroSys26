package main

import (
	"fmt"
	"os"

	"kvstore/kvraft"
	"kvstore/tester"
)

func main() {
	if err := tester.InitDaemon(os.Args[1:], kvraft.NewServer); err != nil {
		fmt.Printf("%v: InitDaemon err %v", os.Args[0], err)
		os.Exit(1)
	}
}
