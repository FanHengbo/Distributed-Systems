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
	"time"
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

	// Here we define the process of worker asking coordinator for task as request,
	// and the result of coordinator returned as response. When the worker enter
	// the loop for the first time, it will send an empty request to coordinator,
	// and the coordinator would send the first task back to the worker. After
	// finishing first task, the worker will store its result to request, so
	// when next time worker ask for a task, it will piggyback the last result
	// to the coordinator, and the coordinator would send next task content.
	request := TaskRequest{}
	for {
		reply := AskForTask(&request)
		switch reply.TaskType {
		case mapType:
			doMapTask(mapf, &reply, &request)
		case reduceType:
			doReduceTask(reducef, &reply, &request)
		case sleep:
			time.Sleep(time.Second)
		case done:
			return
		default:
			fmt.Println("Unknown task type")
		}
	}

}

func doReduceTask(reducef func(string, []string) string, reply *TaskResponse, request *TaskRequest) {
	// First gather all the keyvalues from given intermediate files
	kva := []KeyValue{}
	for _, fileName := range reply.File {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("recude error: cannot read intermediate file %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	outputFileName := fmt.Sprintf("mr-out-%d", reply.TaskNum)
	sort.Sort(ByKey(kva))

	ofile, _ := os.Create(outputFileName)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-<ReduceTaskNum>.
	//
	i := 0
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

	request.TaskType = reduceType
	request.TaskNum = reply.TaskNum
}

func doMapTask(mapf func(string, string) []KeyValue, reply *TaskResponse, request *TaskRequest) {
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
	copy(request.IntermediateFile, intermediateFilesName)
	request.TaskNum = reply.TaskNum
	request.TaskType = mapType
}

func AskForTask(req *TaskRequest) TaskResponse {
	reply := TaskResponse{}
	call("Coordinator.AllocateTask", &req, &reply)

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
