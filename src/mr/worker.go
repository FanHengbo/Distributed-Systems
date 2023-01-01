package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type IntermediateOutput struct {
	Key    string
	Values []string
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

func doMapTask(mapf func(string, string) []KeyValue, reply *TaskReply, response *TaskArgs) {
	// Open returned file
	filename := reply.File[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.File)
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	intermediateFilesMap := make(map[int][]KeyValue)
	currentDir, _ := os.Getwd()
	nBuckets := reply.Buckets
	intermediateFilesName := make([]string, nBuckets)
	for _, kv := range intermediate {
		bucketIndex := ihash(kv.Key) % nBuckets
		intermediateFilesMap[bucketIndex] = append(intermediateFilesMap[bucketIndex], kv)
	}

	// Store in intermediate files
	for i := 0; i < nBuckets; i++ {
		file, err = ioutil.TempFile(currentDir, "")
		fileName := fmt.Sprintf("mr-%d-%d", reply.TaskNum, i)
		enc := json.NewEncoder(file)
		for _, kv := range intermediateFilesMap[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to file %v", fileName)
			}
		}
		os.Rename(file.Name(), fileName)
		file.Close()
		intermediateFilesName[i] = fileName
	}
	copy(response.IntermediateFile, intermediateFilesName)
	response.TaskNum = reply.TaskNum
	response.TaskType = mapType
}

func AskForTask() TaskReply {
	reply := TaskReply{}
	args := Args{}
	call("Coordinator.AllocateTask", &args, &reply)

	return reply
}

/*
func SendMapResult(result []string) {
	mapResult := MapResult{result}

}
*/

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
