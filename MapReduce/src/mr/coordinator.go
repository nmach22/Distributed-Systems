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

type MapReduceTask struct {
	TaskType  string // "map", "reduce", "exit", "wait"
	Status    int    // 0: Unassigned, 1: Assigned, 2: Finished
	TimeStamp int64
	Index     int // Index in the list of tasks
	FileName  string
}

type Coordinator struct {
	inputFiles []string
	nReduce    int

	mapTasks    []MapReduceTask
	reduceTasks []MapReduceTask

	// Increase by 1 when one mapTask done. The map Phase is done when mapDone == inputFiles
	mapDone int
	// Increase by 1 when one reduceTask done. The reduce Phase is done when reduceDone == nReduce
	reduceDone int

	mutex sync.Mutex
}

func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.mapTasksAllDone() {
		for i, tsk := range c.mapTasks {
			cond := (tsk.Status == 0) || ((tsk.Status == 1) && (time.Now().UnixNano()-tsk.TimeStamp > 10*1e9))
			if cond {
				//fmt.Printf("%v : %v (time start = %v)\n", tsk.Index, tsk.Status, tsk.TimeStamp)
				c.mapTasks[i].Status = 1
				c.mapTasks[i].TimeStamp = time.Now().UnixNano()

				reply.Task.TimeStamp = time.Now().UnixNano()
				reply.Task.TaskType = "map"
				reply.Task.Status = 1
				reply.Task.Index = i
				reply.Task.FileName = tsk.FileName
				reply.NReduce = c.nReduce
				reply.NMap = len(c.mapTasks)
				return nil
			}
		}
		reply.Task.TaskType = "wait"
		return nil
	} else {
		for i, tsk := range c.reduceTasks {
			cond := (tsk.Status == 0) || ((tsk.Status == 1) && (time.Now().UnixNano()-tsk.TimeStamp > 10*1e9))
			if cond {
				c.reduceTasks[i].Status = 1
				c.reduceTasks[i].TimeStamp = time.Now().UnixNano()

				reply.Task.TimeStamp = time.Now().UnixNano()
				reply.Task.TaskType = "reduce"
				reply.Task.Status = 1
				reply.Task.Index = i
				//reply.Task.FileName = tsk.FileName
				reply.NReduce = c.nReduce
				reply.NMap = len(c.mapTasks)
				return nil
			}
		}
		if c.reduceTasksAllDone() {
			reply.Task.TaskType = "exit"
			return nil
		}
		reply.Task.TaskType = "wait"
	}
	return nil
}

func (c *Coordinator) TaskComplete(args *TaskCompleteRequest, reply *TaskCompleteReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.Task.TaskType == "map" {
		// Mark the map task as completed
		//fmt.Println("Done...[%v]", args.Task.Index)
		c.mapTasks[args.Task.Index].Status = 2
	} else if args.Task.TaskType == "reduce" {
		// Mark the reduce task as completed
		c.reduceTasks[args.Task.Index].Status = 2
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret := c.mapTasksAllDone() && c.reduceTasksAllDone()

	return ret
}

func (c *Coordinator) mapTasksAllDone() bool {
	for _, task := range c.mapTasks {
		if task.TaskType == "map" && task.Status != 2 {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceTasksAllDone() bool {
	for _, task := range c.reduceTasks {
		if task.TaskType == "reduce" && task.Status != 2 {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTask := make([]MapReduceTask, len(files))
	reduceTasks := make([]MapReduceTask, nReduce)

	for i := 0; i < len(files); i++ {
		mapTask[i].TaskType = "map"
		mapTask[i].FileName = files[i]
		mapTask[i].Index = i
		mapTask[i].Status = 0
	}

	for i := 0; i < nReduce; i++ {
		reduceTasks[i].TaskType = "reduce"
		reduceTasks[i].Index = i
		reduceTasks[i].Status = 0
	}

	c := Coordinator{
		inputFiles:  files,
		nReduce:     nReduce,
		mapTasks:    mapTask,
		reduceTasks: reduceTasks,
		mapDone:     0,
		reduceDone:  0,
		mutex:       sync.Mutex{},
	}

	// Your code here.

	c.server()
	return &c
}
