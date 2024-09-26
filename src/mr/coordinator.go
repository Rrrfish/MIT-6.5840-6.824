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
	copy(c.maps.filename, files)

	c.reduces.mu.Lock()
	defer c.reduces.mu.Unlock()
	c.reduces.sumJob = nReduce
	c.reduces.exeJobChan = make(map[int]chan bool)

	c.server()
	return &c
}

// allocate map job for workers
func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.maps.mu.Lock()

	for {
		if c.maps.sumJob > c.maps.doneJob+len(c.maps.exeJobChan) {
			var taskNumber int
			for i, v := range c.maps.available {
				if v {
					taskNumber = i
				}
			}
			reply.number = taskNumber
			reply.fileName = c.maps.filename[taskNumber]
			reply.mapOver = false
			c.maps.mu.Unlock()

			go c.waitWorkerDone(taskNumber)
			break
		} else {
			c.maps.cond.Wait()

			if c.maps.sumJob == c.maps.doneJob {
				reply.mapOver = true
				c.maps.cond.Broadcast()
				break
			}
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
			c.maps.doneJob++
			c.maps.available[taskNumber] = false
			return
		case <-timeout:
			c.maps.mu.Lock()
			defer c.maps.mu.Unlock()
			delete(c.maps.exeJobChan, taskNumber)
			c.maps.cond.Signal() // signal a waiting worker for Map task
			return
		}
	}
}

// worker use this to notify coordinator work done
func (c *Coordinator) MapJobDone(taskNumber *taskNumber, reply *MapJobDoneReply) error {
	c.maps.mu.Lock()
	defer c.maps.mu.Unlock()
	c.maps.exeJobChan[int(*taskNumber)] <- true
	return nil
}
