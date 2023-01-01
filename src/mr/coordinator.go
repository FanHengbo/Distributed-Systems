package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
	Files       []string
	Buckets     int
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	mapLeft     int
	reduceLeft  int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) AllocateTask(args *Args, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapLeft != 0 {
		// Allocate map task
		for _, task := range c.MapTasks {
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
	for index, file := range files {
		task := MapTask{file, index, idle, time.Now()}
		c.MapTasks = append(c.MapTasks, task)
	}
	c.server()
	return &c
}
