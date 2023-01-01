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

type TaskType int

const (
	mapType TaskType = iota
	reduceType
	sleep
	done
)

type Args struct {
	X int
}

type Initialization struct {
	buckets int
}

type TaskArgs struct {
	IntermediateFile []string
	TaskNum          int
	TaskType         TaskType
}

type TaskReply struct {
	File     []string
	TaskNum  int
	TaskType TaskType
	Buckets  int
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
