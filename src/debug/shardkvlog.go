package debug

import "log"

const DebugAll5 = true
const CancelAllPrint5 = true

const Debug5A = true

func D5APrintf(format string, a ...interface{}) {
	if !CancelAllPrint5 && (DebugAll5 || Debug5A) {
		log.Printf(format, a...)
	}
}
