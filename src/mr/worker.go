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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func(task *Task) MapHandler(mapf func(string, string) []KeyValue) []string {
	content := Readfile(task.File[0])
	kva := mapf(task.File[0], content)
	ofile_prefix := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(task.Version)
	intermediate := make([][]KeyValue, task.ReducerNum)
	// 针对 key 进行 hash 分组
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.ReducerNum
		intermediate[idx] = append(intermediate[idx], kv)
	}
	// 对分组进行排序
	mid_filename := []string{}
	for idx := 0; idx < task.ReducerNum; idx++ {
		ofile_name := ofile_prefix + "-" + strconv.Itoa(idx)
		if len(intermediate[idx]) == 0 {
			continue
		}
		sort.Sort(ByKey(intermediate[idx]))
		ofile, _ := os.Create(ofile_name)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[idx] {
			if err := enc.Encode(&kv); err != nil {
				break
			}
		}
		ofile.Close()
		mid_filename = append(mid_filename, ofile_name)
	}
	return mid_filename
}

func(task *Task) ReduceHandler(reducef func(string, []string) string) []string {
	var kva []KeyValue
	for _, filename := range task.File {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	i := 0
	oname := "mr-out-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(task.Version)
	ofile, _ := os.Create(oname)
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	return []string{}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	task := CallTask()
	// loop to send RPC to get a task
	for task != nil {
		if task.TaskType == Mapper {
			mid_filename := task.MapHandler(mapf)
			task.SendReport(mid_filename)
		} else if task.TaskType == Reducer {
			task.ReduceHandler(reducef)
			task.SendReport([]string{})
		} else if task.TaskType == Finished {
			break
		}
		time.Sleep(time.Second)
		task = CallTask()
	}

}

func Readfile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

// send a report by RPC
func(task *Task) SendReport(filenames []string) {
	// get the identity of the worker
	padding := Padding{}
	report := Report{
		filenames,
		task.TaskType,
		task.TaskId,
		task.Version,
	}
	// send the RPC request, try to get a task
	call("Coordinator.SendReport", &report, &padding)
}

// get a task by RPC
func CallTask() *Task {
	// get the identity of the worker
	padding := Padding{}
	task := Task{}
	// send the RPC request, try to get a task
	ok := call("Coordinator.GetTask", &padding, &task)
	if ok {
		return &task
	} else {
		return nil
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
