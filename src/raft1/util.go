package raft

import "log"

// Debugging
const DebugAll = true
const CancelAllPrint = true

const Debug3A = false
const Debug3B = false
const Debug3C = false
const Debug3D = true

func D3APrintf(format string, a ...interface{}) {
	if !CancelAllPrint && (DebugAll || Debug3A) {
		log.Printf(format, a...)
	}
}

func D3BPrintf(format string, a ...interface{}) {
	if !CancelAllPrint && (DebugAll || Debug3B) {
		log.Printf(format, a...)
	}
}

func D3CPrintf(format string, a ...interface{}) {
	if !CancelAllPrint && (DebugAll || Debug3C) {
		log.Printf(format, a...)
	}
}

func D3DPrintf(format string, a ...interface{}) {
	if !CancelAllPrint && (DebugAll || Debug3D) {
		log.Printf(format, a...)
	}
}
