package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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

// Add your RPC definitions here.
// 任务类型
const Free int = 0
const Mapper int = 1
const Reducer int = 2
const Finished int = 3

// 任务状态
const Ready int = 0
const Running int = 1
const Blocked int = 2
// padding of RPC arguments
type Padding struct {
	X int
}
// Use an RPC to get a task, and Regist a Mapper or Reducer
type Task struct {
	File []string			// The input files argument of Map or Reduce
	TaskType int			// 0 -- Free | 1 -- Mapper | 2 -- Reducer
	TaskId int				// Task id
	ReducerNum int			// the predifined config parameter
	State int
	StartTime time.Time
	Version int
}

// Mapper 发送产生的中间文件，Reducer 不发送
type Report struct {
	File []string	
	TaskType int
	TaskId int
	TaskVersion int
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
