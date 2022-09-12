package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// 0表示未分配任务
	// 1表示已分配任务但没有收到成功的回复
	// 2表示已完成
	// input_files map[string]int // 存输入文件以及完成情况
	input_files        []string    // 存输入文件
	complete_condition []int       // 完成情况
	workerId_generator int         // workerId生成器
	map_MtaskId_WId    map[int]int // Map task与workerId之间的对应关系
	map_RtaskId_WId    map[int]int // Reduce task与workerId之间的对应关系
	bucket             []int       // bucket以及完成情况
	nReduce            int         // 桶的数量
	map_done           bool        // map工作全部完成
	reduce_done        bool        // reduce工作全部完成
	mux                sync.Mutex  // 这里直接用一把大锁
}

// Your code here -- RPC handlers for the worker to call.

// worker申请任务
func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	// 如果状态码有误
	// if !args.Is_healthy {
	// 	return args
	// }

	c.mux.Lock()
	defer c.mux.Unlock()

	// 检查map是否全部完成
	if !c.map_done {
		for i, v := range c.input_files {
			if c.complete_condition[i] == 0 {
				// 分配task
				if args.WorkerId == -1 {
					reply.WorkerId = c.workerId_generator
					c.workerId_generator++
				} else {
					reply.WorkerId = args.WorkerId
				}
				reply.Task_kind = 1
				reply.Map_filename = v
				reply.MapTaskId = i
				reply.NReduce = c.nReduce
				c.complete_condition[i] = 1           // 改为已分配
				c.map_MtaskId_WId[i] = reply.WorkerId // 建立对应关系
				fmt.Printf("MapTask %v allocated to worker %v\n", i, reply.WorkerId)
				fmt.Printf("map_MtaskId_WId condition: %v\n", c.map_MtaskId_WId)
				// 让我们这样来思考：
				// 被分配任务的worker需要有一个标识符（id），不能是pid，因为在分布式系统
				// 中，多台机器上的worker可能有相同的pid，无法区分。id应该由coordinator
				// 生成并管理，同时worker自身也需要保存这个id，以便coordinator可以识别
				// 所以coordinator和rpc中的数据结构还需要重新设计
				// coordinator需要包含一个id生成器、task和workerId之间的对应关系
				// rpc中的每个都需要包含workerId，初始为-1（非法值）

				// 还需要一个taskId，用来标识中间文件名以及阐明worker和task之间的关系

				// 假设worker1(workerId=1)领取了task1(taskId=1)
				// 在函数返回之前，需要开启一个goroutine，监测worker1是否在规定时间内完成任务
				// 如果没有完成，需要将这个任务分配给其他worker，例如worker2(workerId=2)
				// 但此时worker1可能仍在运行，并且可能最终会完成它的任务。这就出现了一个问题：
				// worker1和worker2都产生了若干份中间文件，并且两组文件的文件名相同（因为
				// 中间文件的命名方式是mr-X-Y，其中X表示Map task编号，Y表示Reduce task编号）
				// 理论上我们需要的worker2产生的中间文件，但是有可能被worker1重新写入
				// 为了避免这种情况，可以使用临时文件，worker首先将内容写入临时文件，一旦
				// 某个worker完成了工作，将这些文件提交给coordinator，如果workerId与taskId
				// 对应，则采纳，并将临时文件重命名为标准格式；如果workerId与taskId不对应
				// 则说明是废弃的worker进行了提交，忽略之

				// 两条线，a线用于sleep，b线用于监听worker完成任务的信号
				// b线阻塞，直到通过a线或完成任务的信号将其唤醒，唤醒后查看任务是否完成，若完成，break
				// 若未完成，切换worker
				go func(taskId int, workerId int, c *Coordinator) {
					// 这里等待任务完成的时间限度不能设的太紧，即sleep的时间不能太短。如果把时间设为1秒，经检验，
					// 以我自己的电脑来跑，pg-metamorphosis.txt和pg-sherlock_holmes.txt两个文件是无法在1秒
					// 内跑完的（因为这两个文件最大，达到一万多行）。这样就会一直超时，一直重新分配任务，如此循环
					time.Sleep(time.Second * 10)
					c.mux.Lock()
					if c.complete_condition[taskId] != 2 {
						// fmt.Println("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
						c.complete_condition[taskId] = 0  // 改为未分配
						delete(c.map_MtaskId_WId, taskId) // 取消映射
						fmt.Printf("worker %v failed! map_MtaskId_WId condition: %v\n", workerId, c.map_MtaskId_WId)
					}
					c.mux.Unlock()
				}(reply.MapTaskId, reply.WorkerId, c)
				return nil
			}
		}
		// 程序运行到这里，说明Map task都已分配，但没有全部完成，所以没有多余的任务分配
		reply.Task_kind = 0
		return nil
	}

	// 分配Reduce task
	for i, v := range c.bucket {
		if v == 0 {
			if args.WorkerId == -1 {
				reply.WorkerId = c.workerId_generator
				c.workerId_generator++
			} else {
				reply.WorkerId = args.WorkerId
			}
			reply.Task_kind = 2
			reply.Bucket_num = i
			reply.File_num = len(c.input_files)
			c.bucket[i] = 1                       // 改为已分配
			c.map_RtaskId_WId[i] = reply.WorkerId // 建立对应关系
			fmt.Printf("ReduceTask %v allocated to worker %v\n", i, reply.WorkerId)
			fmt.Printf("map_RtaskId_WId condition: %v\n", c.map_RtaskId_WId)
			// 同上
			go func(taskId int, workerId int, c *Coordinator) {
				time.Sleep(time.Second * 10)
				c.mux.Lock()
				if c.bucket[taskId] != 2 {
					c.bucket[taskId] = 0              // 改为未分配
					delete(c.map_RtaskId_WId, taskId) // 取消映射
					fmt.Printf("worker %v failed! map_RtaskId_WId condition: %v\n", workerId, c.map_RtaskId_WId)
				}
				c.mux.Unlock()
			}(reply.Bucket_num, reply.WorkerId, c)
			return nil
		}
	}
	// 程序运行到这里，说明Reduce task都已分配，但没有全部完成，所以没有多余的任务分配
	reply.Task_kind = 0
	return nil
}

func (c *Coordinator) CommitTask(args *CommitTaskArgs, reply *CommitTaskReply) error {
	// fmt.Println(args)
	reply.Ok = false
	TaskId := args.TaskId
	WorkerId := args.WorkerId
	if args.Task_kind == 1 { // 提交了Map task
		c.mux.Lock()
		if v, ok := c.map_MtaskId_WId[TaskId]; ok && v == WorkerId {
			reply.Ok = true
		}
		c.mux.Unlock()
	} else { // 提交了Reduce task
		c.mux.Lock()
		if v, ok := c.map_RtaskId_WId[TaskId]; ok && v == WorkerId {
			reply.Ok = true
		}
		c.mux.Unlock()
	}
	return nil
}

func (c *Coordinator) FinishTask(args *OkTaskArgs, reply *OkTaskReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	TaskId := args.TaskId
	WorkerId := args.WorkerId
	if args.Task_kind == 1 {
		if v, ok := c.map_MtaskId_WId[TaskId]; ok && v == WorkerId { // 理论上来说，这个判断肯定能通过
			c.complete_condition[TaskId] = 2
			fmt.Printf("Finish MapTask %v, WorkerId is %v\n", TaskId, WorkerId)
			fmt.Printf("MapTask condition: %v\n", c.complete_condition)
			fmt.Println("---------------------------------------------")
			for _, v := range c.complete_condition {
				if v != 2 {
					return nil
				}
			}
			c.map_done = true
		}
	} else {
		if c.map_RtaskId_WId[TaskId] == WorkerId {
			c.bucket[TaskId] = 2
			fmt.Printf("Finish ReduceTask %v, WorkerId is %v\n", TaskId, WorkerId)
			fmt.Printf("ReduceTask condition: %v\n", c.bucket)
			fmt.Println("---------------------------------------------")
			for _, v := range c.bucket {
				if v != 2 {
					return nil
				}
			}
			c.reduce_done = true
		}
	}
	return nil
}

// 询问是否所有task都已经完成
func (c *Coordinator) AskAllCompleted(args *AskDoneArgs, reply *AskDoneReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	reply.Completed = c.reduce_done
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mux.Lock()
	ret = c.reduce_done
	c.mux.Unlock()

	if ret {
		fmt.Println("All tasks have been done, coordinator exit!")
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		input_files:        make([]string, len(files)),
		complete_condition: make([]int, len(files)),
		map_MtaskId_WId:    make(map[int]int),
		map_RtaskId_WId:    make(map[int]int),
		bucket:             make([]int, nReduce),
		nReduce:            nReduce,
	}

	// Your code here.
	// 保存所有的input files
	c.input_files = files
	fmt.Println(c.input_files)

	c.server()
	return &c
}
