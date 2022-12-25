package mr

/* RPC definitions. */

import (
	"os"
)
import "strconv"

/*
	example to show how to declare the arguments
	and reply for an RPC.

	type ExampleArgs struct {
		X int
	}

	type ExampleReply struct {
		Y int
	}
*/

// TODO Add your RPC definitions here.

type HeartBeatArgs struct {
	Id WorkerId
}

type HeartBeatReply struct {
	Resp string
}

/*
Cook up a unique-ish UNIX-domain socket name
in /var/tmp, for the coordinator.
Can't use the current directory since
Athena AFS doesn't support UNIX-domain sockets.
*/
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
