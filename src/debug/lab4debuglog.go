package debug

import "log"

const DebugAll4 = false
const Debug4A = false
const Debug4B = true
const Debug4C = true
const CancelAllPrint4 = true

func D4APrintf(format string, a ...interface{}) {
	if !CancelAllPrint4 && (DebugAll4 || Debug4A) {
		log.Printf(format, a...)
	}
}

func D4BPrintf(format string, a ...interface{}) {
	if !CancelAllPrint4 && (DebugAll4 || Debug4B) {
		log.Printf(format, a...)
	}
}

func D4CPrintf(format string, a ...interface{}) {
	if !CancelAllPrint4 && (DebugAll4 || Debug4C) {
		log.Printf(format, a...)
	}
}
