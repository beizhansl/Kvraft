package raft

import "log"

// Debugging
const OpenDebug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if OpenDebug {
		log.Printf(format, a...)
	}
	return
}
