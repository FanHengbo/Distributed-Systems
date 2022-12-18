package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	// First ask the coordinator for task
	reply := AskForTask()
	for {
		switch reply.TaskType {
		case mapType:
			doMapTask(mapf, reply)
		case reduceType:
		default:
			fmt.Println("Unknown task type")
		}
	}

}

func doMapTask(mapf func(string, string) []KeyValue, reply TaskReply) {
	// Open returned file
	file, err := os.Open(reply.File)
	if err != nil {
		log.Fatalf("cannot open %v", reply.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.File)
	}
	file.Close()
	kva := mapf(reply.File, string(content))

	sort.Sort(ByKey(kva))

	// Store in temporary files
	i := 0
	reduceTaskFileName := []string{}

	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		recudeTaskNum := ihash(kva[i].Key) % reply.Buckets
		currentDir, _ := os.Getwd()
		temporaryFileName := currentDir + "mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(recudeTaskNum)
		var needToRename bool = false
		if _, err := os.Stat(temporaryFileName); errors.Is(err, os.ErrNotExist) {
			file, err = ioutil.TempFile(currentDir, "")
			if err != nil {
				log.Fatalf("Cannot create a temporary file")
			}
			needToRename = true
		} else {
			file, err = os.OpenFile(temporaryFileName, os.O_APPEND, 0777)
			if err != nil {
				log.Fatalf("Cannot open %v", temporaryFileName)
			}
		}
		enc := json.NewEncoder(file)
		for _, kv := range values {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Cannot write to the temporary file")
			}
		}
		if needToRename {
			os.Rename(file.Name(), temporaryFileName)
			reduceTaskFileName = append(reduceTaskFileName, temporaryFileName)
		}
		file.Close()
	}
}

func AskForTask() TaskReply {
	reply := TaskReply{}
	args := Args{}
	call("Coordinator.AllocateTask", &args, &reply)

	return reply
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
