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
type FirstHeartBeatReply struct{
	State int
}
type HeartBeatReply struct{
	
}
/*
ret
-1:wait and ask a reduce
0:has set ip  should ask for task (repeat first heart beat)
1:a map task
2:a reduce task
3:exit
*/
type AskForTaskReply struct{
	Instruc int
	//or key
	InputFileName string
}

/*
carrytask 0:no task 1:map task 2:reduce task 3:something about machine
taskname input file name or key
taskstate 0：faild need to retrive 1:doing 2:done
the workersate msg may lose
 the server may aks machine for a task's sate 
*/
type WorkerState struct{
	MachineIP string
	CarryTask int
	TaskName string
	TaskState int
}
type StateReply struct{
	//nothing to reply?
	//ack 1 
	Ack int
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
