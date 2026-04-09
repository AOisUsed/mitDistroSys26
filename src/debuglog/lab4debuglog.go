package debuglog

import "log"

const DebugAll4 = true
const Debug4A = true
const Debug4B = true
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
