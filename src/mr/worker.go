package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type FileWriter struct {
	enc      *json.Encoder
	filename string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 有一个问题，不知道Worker是并发的还是非并发的
	// 个人认为两种都可以，先实现非并发的版本

	// 声明一个workerId，初始为-1
	workerId := -1

	// 不断地发送RPC来获取task
	// task分为Map task和Reduce task
	// 只有所有的Map task执行完成后才能执行Reduce task
	for {
		// 询问所有任务是否都已经结束
		args_completed := AskDoneArgs{}
		reply_completed := AskDoneReply{}
		success := call("Coordinator.AskAllCompleted", &args_completed, &reply_completed)
		if !success || success && reply_completed.Completed {
			fmt.Printf("All tasks have been done, wroker:%v exit!", workerId)
			break
		}
		// 申请任务
		args := AskForTaskArgs{}
		args.WorkerId = workerId
		reply := AskForTaskReply{}
		ok := call("Coordinator.AskForTask", &args, &reply)
		if ok { // 成功被分配了任务
			if reply.Task_kind == 1 {
				fmt.Printf("got task! MapTaskId is %v and WorkerId is %v\n", reply.MapTaskId, reply.WorkerId)
			} else if reply.Task_kind == 2 {
				fmt.Printf("got task! ReduceTaskId is %v and WorkerId is %v\n", reply.Bucket_num, reply.WorkerId)
			}
			if workerId == -1 {
				workerId = reply.WorkerId // 虽然看上去workerId总是被赋值，实际上它只改变一次
			}
			// 执行任务
			Execute(&reply, mapf, reducef)
		} else {
			// 报错
			fmt.Printf("ask for task failed!\n")
		}
		// time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Execute(task *AskForTaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if task.Task_kind == 0 {
		return
	} else if task.Task_kind == 1 { // Map
		// 模板代码
		filename := task.Map_filename
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		// 生成临时文件
		// 文件格式：tmp-${workerId}-${MapTaskId}-${bucket_num}-*.txt
		m := make(map[int]FileWriter)
		for _, item := range kva {
			bucket_num := ihash(item.Key) % task.NReduce
			if writer, ok := m[bucket_num]; !ok { // 如果没有为该bucket创建临时文件，创建之
				tmp_file_name := fmt.Sprintf("tmp-%v-%v-%v-*.txt", task.WorkerId, task.MapTaskId, bucket_num)
				// tmpFile, err := ioutil.TempFile(os.TempDir(), tmp_file_name)
				// 注意，这里不能创建在linux的临时目录(/tmp)中，至少在我的实现中不行
				// 因为在test-mr.sh中每个测试点都会删除当前目录中的mr-文件，
				// 如果在/tmp中，则无法被删除，导致测试失败
				tmpFile, err := ioutil.TempFile(".", tmp_file_name) // 在当前文件夹下创建临时文件
				if err != nil {
					log.Fatal("cannot create temporary file", err)
				}
				fmt.Println("created file:", tmpFile.Name())

				enc := json.NewEncoder(tmpFile)
				err = enc.Encode(&item)
				if err != nil {
					log.Fatal("cannot encode", err)
				}

				m[bucket_num] = FileWriter{enc, tmpFile.Name()}
			} else { // 已经创建过文件了
				err := writer.enc.Encode(&item)
				if err != nil {
					log.Fatal("cannot encode", err)
				}
			}
		}
		// 创建完成，向coordinator说明自己已经完成了任务，并请求验证
		if commit(task.Task_kind, task.MapTaskId, task.WorkerId) {
			// 重命名文件
			for k, v := range m {
				// tmp_dir := os.TempDir()
				now := fmt.Sprintf("mr-%v-%v", task.MapTaskId, k)
				err := os.Rename(v.filename, now)
				if err != nil {
					log.Fatal("rename failed!", err)
				} else {
					fmt.Printf("rename successfully! from %v to %v\n", v.filename, now)
				}
			}
		} else { // 验证失败，说明task被分配给其他worker了
			fmt.Printf("MapTask %v has changed worker!\n", task.MapTaskId)
			return
		}

		// 完成
		finish(task.Task_kind, task.MapTaskId, task.WorkerId)
	} else { // Reduce
		// 对于Reduce task来说，它需要知道文件名
		// 文件的命名格式为:mr-${MapTaskId}-${Bucket_num}
		// 对于Reduce Task为0来说，需要寻找mr-*-0的文件

		intermediate := []KeyValue{}

		bucket_num := task.Bucket_num
		// 构造文件名
		for i := 0; i < task.File_num; i++ {
			// tmp_dir := os.TempDir()
			filename := fmt.Sprintf("mr-%v-%v", i, bucket_num)
			File, err := os.Open(filename)
			if err != nil {
				// log.Fatalf("cannot open file %v", filename)
				continue // 说明文件不存在
			}
			dec := json.NewDecoder(File)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					File.Close()
					break
				}
				intermediate = append(intermediate, kv)
			}
		}

		sort.Sort(ByKey(intermediate))
		// 这里理论上先写进临时文件
		// 临时文件格式:tmp-out-${Bucket_num}
		oname := fmt.Sprintf("tmp-out-%v", bucket_num)
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		ofile.Close()
		// 提交给coordinator
		if commit(task.Task_kind, task.Bucket_num, task.WorkerId) {
			// 重命名文件
			now := fmt.Sprintf("mr-out-%v", bucket_num)
			err := os.Rename(oname, now)
			if err != nil {
				log.Fatal("rename failed!", err)
			}
		} else {
			fmt.Printf("ReduceTask %v has changed worker!\n", task.Bucket_num)
			return
		}

		// 完成
		finish(task.Task_kind, task.Bucket_num, task.WorkerId)
	}
}

func commit(task_kind, taskId, workerId int) bool {
	args := CommitTaskArgs{
		Task_kind: task_kind,
		TaskId:    taskId,
		WorkerId:  workerId,
	}
	reply := CommitTaskReply{}
	// fmt.Println(args)
	ok := call("Coordinator.CommitTask", &args, &reply)
	if ok {
		return reply.Ok
	} else {
		fmt.Println("This case shouldn't happen")
		return false
	}
}

func finish(task_kind, taskId, workerId int) {
	args := OkTaskArgs{
		Task_kind: task_kind,
		TaskId:    taskId,
		WorkerId:  workerId,
	}
	reply := OkTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		if task_kind == 1 {
			fmt.Printf("finish Map task. MapTaskId is %v\n", taskId)
		} else {
			fmt.Printf("finish Reduce task. Bucket number is %v\n", taskId)
		}
	} else {
		fmt.Println("This case shouldn't happen")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
