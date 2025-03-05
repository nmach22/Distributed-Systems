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

// this is from mrsequential.go
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		request := TaskRequest{}
		response := TaskResponse{}

		success := call("Coordinator.RequestTask", &request, &response)
		if !success {
			fmt.Printf("aaaaaaaaaa")
			break
		}

		taskCompleteRequest := TaskCompleteRequest{}
		taskCompleteReply := TaskCompleteReply{}
		taskCompleteRequest.Task.Index = response.Task.Index
		taskCompleteRequest.Task.TaskType = response.Task.TaskType

		switch response.Task.TaskType {
		case "map":
			doMap(&response, mapf)

			success := call("Coordinator.TaskComplete", &taskCompleteRequest, &taskCompleteReply)
			if !success {
				break
			}
		case "reduce":
			doReduce(&response, reducef)

			success := call("Coordinator.TaskComplete", &taskCompleteRequest, &taskCompleteReply)
			if !success {
				break
			}
		case "wait":
			time.Sleep(time.Second)
		case "exit":
			os.Exit(0)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doReduce(reduceTask *TaskResponse, reducef func(string, []string) string) {
	nMap := reduceTask.NMap
	taskID := reduceTask.Task.Index

	// Read intermediate files from the map tasks
	intermediate := make(map[string][]string) // Map to store key and list of values

	// Loop through the intermediate files produced by each map task
	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("mr-%v-%v", i, taskID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // End of file
			}
			// Append the value to the list corresponding to this key
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
		}
		err = file.Close()
		if err != nil {
			return
		}
	}

	// Create a temporary file for final reduce output
	tmpFile, err := os.CreateTemp("", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp output file for reduce task %d", taskID)
	}

	// Call reducef on each key and write the result to the temp file
	for key, values := range intermediate {
		// Call the user-defined reduce function
		output := reducef(key, values)

		// Write the result to the temporary output file in the required format
		fmt.Fprintf(tmpFile, "%v %v\n", key, output)
	}

	// Close and atomically rename the temp file to the final output file
	tmpFile.Close()

	// Final file name should be mr-out-X where X is taskID (reduce task number)
	finalFileName := fmt.Sprintf("mr-out-%v", taskID)

	// Atomically rename the temporary file to the final file
	err = os.Rename(tmpFile.Name(), finalFileName)
	if err != nil {
		log.Fatalf("cannot rename temp file to %v", finalFileName)
	}
}

func doMap(mapTask *TaskResponse, mapf func(string, string) []KeyValue) {
	nReduce := mapTask.NReduce
	fileName := mapTask.Task.FileName
	var intermediate []KeyValue
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	sort.Sort(ByKey(kva))

	intermediate = append(intermediate, kva...)

	// Step 3: Create temporary files for intermediate results
	tmpFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		// Create a temporary file for each reduce task
		tmpFileName := fmt.Sprintf("mr-tmp-%v-%v", mapTask.Task.Index, i)
		tmpFile, err := os.CreateTemp("", tmpFileName)
		if err != nil {
			log.Fatalf("cannot create temp file for reduce task %d", i)
		}
		tmpFiles[i] = tmpFile
		encoders[i] = json.NewEncoder(tmpFiles[i])
	}

	// Step 4: Distribute key/value pairs into the corresponding temporary files
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % nReduce
		encoders[reduceIndex].Encode(&kv)
	}

	// Step 5: Close and atomically rename each temp file to its final name
	for i := 0; i < nReduce; i++ {
		tmpFiles[i].Close()

		// Final file name should be mr-X-Y where X is taskID (map task), Y is reduceIndex
		finalFileName := fmt.Sprintf("mr-%v-%v", mapTask.Task.Index, i)

		// Atomically rename the temporary file to the final file
		err := os.Rename(tmpFiles[i].Name(), finalFileName)
		if err != nil {
			log.Fatalf("cannot rename temp file to %v", finalFileName)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
