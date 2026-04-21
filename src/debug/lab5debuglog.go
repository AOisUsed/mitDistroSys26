package debug

import "log"

const DebugAll5 = true
const CancelAllPrint5 = false

const Debug5A = true
const Debug5B = true
const Debug5C = true

func D5APrintf(format string, a ...interface{}) {
	if !CancelAllPrint5 && (DebugAll5 || Debug5A) {
		log.Printf(format, a...)
	}
}

func D5BPrintf(format string, a ...interface{}) {
	if !CancelAllPrint5 && (DebugAll5 || Debug5B) {
		log.Printf(format, a...)
	}
}

func D5CPrintf(format string, a ...interface{}) {
	if !CancelAllPrint5 && (DebugAll5 || Debug5C) {
		log.Printf(format, a...)
	}
}
