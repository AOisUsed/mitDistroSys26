package rsm

import "log"

const Debug = true
const Debug4A = true
const Debug4B = true
const CancelAllPrint = true

func D4APrintf(format string, a ...interface{}) {
	if !CancelAllPrint && (Debug || Debug4A) {
		log.Printf(format, a...)
	}
}

func D4BPrintf(format string, a ...interface{}) {
	if !CancelAllPrint && (Debug || Debug4B) {
		log.Printf(format, a...)
	}
}
