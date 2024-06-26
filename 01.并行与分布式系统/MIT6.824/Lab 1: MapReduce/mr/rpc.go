package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.
type GetJobArgs struct {
}

type GetJobReply struct {
	MapJobKey    string
	ReduceJobKey int
	HasJob       bool
	MapJobNum    int
}

type FinishJobArgs struct {
	MapJobKey    string
	ReduceJobKey int
}

type FinishJobReply struct {
	Success bool
}

type IsJobFinishedArgs struct {
}

type IsJobFinishedReply struct {
	Finished bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
