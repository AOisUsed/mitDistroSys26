package main

import (
	"fmt"
	"os"

	"kvstore/shardkv/shardgrp"
	"kvstore/tester"
)

func main() {
	if err := tester.InitDaemon(os.Args[1:], shardgrp.NewServer); err != nil {
		fmt.Printf("%v: InitDaemon err %v", os.Args[0], err)
		os.Exit(1)
	}
}
