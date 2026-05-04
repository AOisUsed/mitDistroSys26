package debug

import "log"

// Debugging
const DebugAll3 = true
const CancelAllPrint3 = true

const Debug3A = true
const Debug3B = true
const Debug3C = true
const Debug3D = true

func D3APrintf(format string, a ...interface{}) {
	if !CancelAllPrint3 && (DebugAll3 || Debug3A) {
		log.Printf(format, a...)
	}
}

func D3BPrintf(format string, a ...interface{}) {
	if !CancelAllPrint3 && (DebugAll3 || Debug3B) {
		log.Printf(format, a...)
	}
}

func D3CPrintf(format string, a ...interface{}) {
	if !CancelAllPrint3 && (DebugAll3 || Debug3C) {
		log.Printf(format, a...)
	}
}

func D3DPrintf(format string, a ...interface{}) {
	if !CancelAllPrint3 && (DebugAll3 || Debug3D) {
		log.Printf(format, a...)
	}
}
