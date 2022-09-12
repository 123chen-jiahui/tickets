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

// Add your RPC definitions here.

// worker询问task时的args与reply
type AskForTaskArgs struct {
	WorkerId int // 表明自己的身份，若workerId=-1，则分配一个新的，通过reply返回
}

type AskForTaskReply struct {
	WorkerId     int    // 分配的workerId
	Task_kind    int    // task种类，0表示无任务、1表示Map、2表示Reduce
	Map_filename string // 需要执行Map的文件名，只在Task_kind=1时有效
	MapTaskId    int    // Map task编号，只在Task_kind=1时有效
	NReduce      int    // 桶的数量，只在Task_kind=1时有效
	File_num     int    // 输入文件的数量，只在Task_kind=2时有效
	Bucket_num   int    // 需要执行Reduce的桶号，只在Task_kind=2时有效
}

// 为AskForTaskArgs实现error接口
// func (afta *AskForTaskArgs) Error() string {
// 	return fmt.Sprintf("The state of work is not healthy! Got healthy code: %v", afta.Is_healthy)
// }

type CommitTaskArgs struct {
	Task_kind int
	WorkerId  int
	TaskId    int
}

type CommitTaskReply struct {
	Ok bool // 验证是否通过
}

type OkTaskArgs struct {
	Task_kind int
	WorkerId  int
	TaskId    int
}

type OkTaskReply struct {
}

// 所有task完成
type AskDoneArgs struct {
}

type AskDoneReply struct {
	Completed bool // 所有task完成
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
