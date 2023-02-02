package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

//For Job Ask
//what coodinator need?
type CoordAskArgs struct {
}

//what worker need?
// 1. map/reduce?
// 2. argument that map need or reduce need!
type CoordAskReply struct {
	// 0 map 1 reduce 2 no job 3 done
	State         int
	InputfileName string
	JobId         int
	NReduce       int
}

//For job return
// 1. workerId	--	intername
// 2. which job
// 3. jobId
type CoordRetArgs struct {
	WorkerId string
	JobId    int
	State    int
}

//what worker need?
// 1. map/reduce?
// 2. argument that map need or reduce need!
type CoordRetReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
