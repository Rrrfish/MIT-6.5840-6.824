package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	maps    MapWorkerMapper
	reduces ReduceWorkerMapper
}

type MapWorkerMapper struct {
	mu         sync.Mutex
	cond       sync.Cond
	sumJob     int
	doneJob    int
	exeJobChan map[int](chan bool)
	available  []bool // true is undone
	filename   []string
}

type ReduceWorkerMapper struct {
	mu         sync.Mutex
	cond       sync.Cond
	sumJob     int
	doneJob    int
	exeJobChan map[int](chan bool)
	available  []bool // true is undone
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
	ret := false
	// ret := true

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.maps.cond = *sync.NewCond(&c.maps.mu)
	c.reduces.cond = *sync.NewCond(&c.reduces.mu)
	// Your code here.
	mapTaskNum := len(files)

	c.maps.mu.Lock()
	defer c.maps.mu.Unlock()
	c.maps.sumJob = mapTaskNum
	c.maps.exeJobChan = make(map[int]chan bool)
	c.maps.doneJob = 0
	c.maps.available = make([]bool, mapTaskNum)
	for i := range c.maps.available {
		c.maps.available[i] = true
	}
	c.maps.filename = make([]string, mapTaskNum)
	copy(c.maps.filename, files)

	c.reduces.mu.Lock()
	defer c.reduces.mu.Unlock()
	c.reduces.sumJob = nReduce
	c.reduces.exeJobChan = make(map[int]chan bool)
	c.reduces.available = make([]bool, nReduce)
	for i := range c.reduces.available {
		c.reduces.available[i] = true
	}
	c.reduces.doneJob = 0

	c.server()
	return &c
}

// allocate map job for workers
func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {

	for {
		c.maps.mu.Lock()
		defer c.maps.mu.Unlock()
		// fmt.Println("[test]Coordinator rpc: GetMapTask()")
		if c.maps.sumJob > c.maps.doneJob+len(c.maps.exeJobChan) {

			fmt.Println("[test]map doneJob: ", c.maps.doneJob)

			var taskNumber int
			// fmt.Println("[test]c.maps.available len: ", len(c.maps.available))
			for i, v := range c.maps.available {
				// fmt.Println("[available]get in for loop, i = ", i, " v = ", v)
				if v {
					fmt.Println("[test]get a map Task: ", i)
					c.maps.available[i] = false
					taskNumber = i
					break
				}
			}
			//fmt.Println("[test]c.maps.available is ok! get a Task: ", taskNumber)
			reply.Number = taskNumber
			reply.FileName = c.maps.filename[taskNumber]
			reply.MapOver = false
			c.maps.exeJobChan[taskNumber] = make(chan bool)

			go c.waitWorkerDone(taskNumber)
			break
		} else if c.maps.sumJob == c.maps.doneJob {
			fmt.Println("[!!!]all map tasks are done!")
			reply.MapOver = true
			c.maps.cond.Broadcast()
			break
		} else {
			c.maps.cond.Wait()

			// if c.maps.sumJob == c.maps.doneJob {
			// 	fmt.Println("[!!!]all map tasks are done!")
			// 	reply.MapOver = true
			// 	c.maps.cond.Broadcast()
			// 	break
			// }
		}
	}

	return nil
}

// // return map task number
// func (c *Coordinator) allocateMapTask() int {
// 	// c.maps.mu.Lock()
// 	// defer c.maps.mu.Unlock()

// 	for i, v := range c.maps.available {
// 		if v {
// 			return i
// 		}
// 	}

// 	return -1
// }

// wait worker until it's job is done, and listening worker's heart beat
func (c *Coordinator) waitWorkerDone(taskNumber int) {
	fmt.Println("[test]waitWorkerDone goroutine starts")
	c.maps.mu.Lock()
	waitJobDone := c.maps.exeJobChan[taskNumber]
	c.maps.mu.Unlock()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-waitJobDone:
			c.maps.mu.Lock()
			defer c.maps.mu.Unlock()
			delete(c.maps.exeJobChan, taskNumber)
			// c.maps.doneJob++
			fmt.Println("[test]waitWorkerDone() c.maps.doneJob: ", c.maps.doneJob)
			return
		case <-timeout:
			c.maps.mu.Lock()
			defer c.maps.mu.Unlock()
			fmt.Println("[worker dies]taskNumber: ", taskNumber)
			c.maps.available[taskNumber] = true
			delete(c.maps.exeJobChan, taskNumber)
			c.maps.cond.Signal() // signal a waiting worker for Map task
			return
		}
	}
}

// worker use this to notify coordinator work done
func (c *Coordinator) MapJobDone(taskNumber *TaskNumber, reply *MapJobDoneReply) error {
	c.maps.mu.Lock()
	defer c.maps.mu.Unlock()
	// fmt.Println("[test]Coordinator rpc, get a map done signal, from taskNumber: ", *taskNumber)
	value, ok := c.maps.exeJobChan[int(*taskNumber)]
	if ok { // worker may be considered as dead
		// fmt.Println("[test]pass signal to chan")
		c.maps.doneJob++
		fmt.Println("[test]map doneJob: ", c.maps.doneJob)
		value <- true
	}
	return nil
}
