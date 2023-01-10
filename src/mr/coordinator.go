package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"reflect"
	"sync"
	"time"
)

type State int

const (
	idle State = iota
	inprogress
	completed
)

type MapTask struct {
	fileName  string
	id        int
	state     State
	beginTime time.Time
}

type ReduceTask struct {
	fileName  []string
	id        int
	state     State
	beginTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	Files             []string
	Buckets           int
	MapTasks          []MapTask
	ReduceTasks       []ReduceTask
	mapLeft           int
	reduceLeft        int
	intermediateFiles [][]string

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) AllocateTask(req *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !reflect.DeepEqual(req, TaskRequest{}) {
		// Not the first time to allocate task
		// We need first process the result sent back
		taskId := req.TaskNum
		if req.TaskType == mapType {
			c.intermediateFiles = append(c.intermediateFiles, req.IntermediateFile)
			c.MapTasks[taskId].state = completed
			c.mapLeft--

		} else if req.TaskType == reduceType {
			c.ReduceTasks[taskId].state = completed
			c.reduceLeft--
		}
	}
	timeNow := time.Now()

	if c.mapLeft != 0 {
		// Map tasks are not finished, so we first need to check whether exsiting task is timeout
		for _, task := range c.MapTasks {
			if task.state == inprogress && timeNow.Sub(task.beginTime) > time.Second*10 {
				// We need to reissue the task. So change the state of it to idle.
				task.state = idle
			}
		}
		// Allocate map task
		for _, task := range c.MapTasks {
			if task.state == idle {
				task.beginTime = time.Now()
				task.state = inprogress

				reply.File[0] = task.fileName
				reply.TaskNum = task.id
				reply.TaskType = mapType
				reply.Buckets = c.Buckets
				return nil
			}
		}
		// Every exsiting map task is inprogress, so we should ask the worker to sleep
		reply.TaskType = sleep
		return nil
	} else if c.reduceLeft != 0 {
		if reflect.DeepEqual(c.ReduceTasks[0].fileName, []string{}) {
			// Reduce task is not initializated
			for _, fileArray := range c.intermediateFiles {
				for j, fileName := range fileArray {
					c.ReduceTasks[j].fileName = append(c.ReduceTasks[j].fileName, fileName)
				}
			}
		}
		for _, task := range c.MapTasks {
			if task.state == inprogress && timeNow.Sub(task.beginTime) > time.Second*10 {
				// We need to reissue the task. So change the state of it to idle.
				task.state = idle
			}
		}

		// Reduce task now is fully initialized
		for _, task := range c.ReduceTasks {
			if task.state == idle {
				task.beginTime = time.Now()
				task.state = inprogress

				reply.File = task.fileName
				reply.TaskNum = task.id
				reply.TaskType = mapType
				reply.Buckets = c.Buckets
				return nil
			}
		}
	}
	reply.TaskType = done
	return nil
}

//func (c *Coordinator) ReportStatus(args *Args, reply *TaskReply)

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
	if c.mapLeft == 0 && c.reduceLeft == 0 {
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
	c := Coordinator{}

	// Your code here.
	copy(c.Files, files)
	c.Buckets = nReduce
	c.mapLeft = len(files)
	c.reduceLeft = nReduce
	c.MapTasks = make([]MapTask, c.mapLeft)
	c.ReduceTasks = make([]ReduceTask, nReduce)
	for index, file := range files {
		mapTask := MapTask{file, index, idle, time.Time{}}
		c.MapTasks[index] = mapTask
	}

	for i := 0; i < nReduce; i++ {
		reduceTask := ReduceTask{[]string{}, i, idle, time.Time{}}
		c.ReduceTasks[i] = reduceTask
	}

	c.server()
	return &c
}
