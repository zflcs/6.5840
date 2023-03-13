package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)
const MapPhase bool = false
const ReducePhase bool = true
type Coordinator struct {
	// Your definitions here.
	Phase bool
	MapTask   []Task
	ReduceTask []Task
	ReducerNum int
	Finished bool
	Intermediate_file [][]string
}

// Your code here -- RPC handlers for the worker to call.
var id int = -1
func(c *Coordinator) gen_id() int {
	id++
	return id
}

// 发送处理结果，如果是 Mapper，将中间文件记录到 Coordinator 中
// 如果是 Reducer，计数 + 1
func (c *Coordinator) SendReport(report *Report, padding *Padding) error {
	if report.TaskType == Reducer {
		if report.TaskVersion != c.ReduceTask[report.TaskId].Version {
			// fmt.Println("old reducer task", report)
			return nil
		}
		c.ReduceTask[report.TaskId].State = Finished
		// fmt.Println("finished Reduce task", report)
		newfile := "mr-out-" + strconv.Itoa(report.TaskId)
		file := "mr-out-" + strconv.Itoa(report.TaskId) + "-" + strconv.Itoa(report.TaskVersion)
		os.Rename(file, newfile)
		return nil
	} else if report.TaskType == Mapper  {
		// 旧任务不进行处理
		if report.TaskVersion != c.MapTask[report.TaskId].Version {
			// fmt.Println("old map task", report)
			return nil
		}
		// 更新任务状态标记
		c.MapTask[report.TaskId].State = Finished
		// fmt.Println("finished Map task", report)
		newfile_prefix := "mr-tmp-" + strconv.Itoa(report.TaskId)
		// Coordinator 记录中间文件信息
		for _, file := range report.File {
			idx, _ := strconv.Atoi(file[len(file)-1:])
			newfile := newfile_prefix + "-" + strconv.Itoa(idx)
			os.Rename(file, newfile)
			c.Intermediate_file[idx] = append(c.Intermediate_file[idx], newfile)
		}
		return nil
	} else {
		return nil
	}
}

// response RPC that is from worker
// args is the identity of worker, it will be used when the coordinator register worker
// reply is the task that assigned to worker
func (c *Coordinator) GetTask(padding *Padding, task *Task) error {
	// Map 阶段
	if c.Phase == MapPhase {
		phase_finished := true
		for idx := range c.MapTask {
			if c.MapTask[idx].State == Ready {
				phase_finished = false
				c.MapTask[idx].State = Running
				c.MapTask[idx].StartTime = time.Now()
				*task = c.MapTask[idx]
				return nil
			} else if c.MapTask[idx].State == Running {
				phase_finished = false
				now := time.Now()
				if now.Sub(c.MapTask[idx].StartTime) > 10 * time.Second {
					c.MapTask[idx].StartTime = now
					c.MapTask[idx].Version += 1
					// fmt.Println("map task is out of time", c.MapTask[idx])
					*task = c.MapTask[idx]
					return nil
				}
			}
		}
		if !phase_finished {
			task.TaskType = Free
			return nil
		} else {
			// Map 阶段结束，初始化 Reduce 任务
			c.Phase = ReducePhase
			for idx := range c.Intermediate_file {
				task := Task{
					c.Intermediate_file[idx],
					Reducer,
					idx,
					c.ReducerNum,
					Ready,
					time.Now(),
					0,
				}
				c.ReduceTask = append(c.ReduceTask, task)
			}
		}
	}
	if c.Phase == ReducePhase {
		phase_finished := true
		for idx := range c.ReduceTask {
			if c.ReduceTask[idx].State == Ready {
				phase_finished = false
				c.ReduceTask[idx].State = Running
				c.ReduceTask[idx].StartTime = time.Now()
				// fmt.Println("get reduce task", c.ReduceTask[idx])
				*task = c.ReduceTask[idx]
				return nil
			} else if c.ReduceTask[idx].State == Running {
				phase_finished = false
				now := time.Now()
				if now.Sub(c.ReduceTask[idx].StartTime) > 10 * time.Second {
					c.ReduceTask[idx].StartTime = now
					c.ReduceTask[idx].Version += 1
					// fmt.Println("reduce task is out of time", c.ReduceTask[idx])
					*task = c.ReduceTask[idx]
					return nil
				}
			}
		}
		if !phase_finished {
			task.TaskType = Free
			return nil
		} else {
			task.TaskType = Finished
			c.Finished = true
		}
	}
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
	if c.Finished {
		ret = true
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
		MapPhase,
		[]Task{},
		[]Task{},
		nReduce,
		false,
		make([][]string, nReduce),
	}

	// Your code here.
	for _, filename := range files {
		task := Task{
			[]string{filename},
			Mapper,
			c.gen_id(),
			nReduce,
			Ready,
			time.Now(),
			0,
		}
		c.MapTask = append(c.MapTask, task)
	}

	c.server()
	return &c
}
