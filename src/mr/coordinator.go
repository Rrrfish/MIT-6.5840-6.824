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
	fmt.Println("splits number: ", mapTaskNum)

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

		if c.maps.sumJob == c.maps.doneJob {
			fmt.Println("[!]map stage is over!")
			reply.MapOver = true
			c.maps.cond.Broadcast()
			c.maps.mu.Unlock()
			return nil
		}

		for i, v := range c.maps.available {
			if v {
				c.maps.available[i] = false
				reply.Number = i
				reply.FileName = c.maps.filename[i]
				reply.MapOver = false
				c.maps.exeJobChan[i] = make(chan bool)

				go c.waitWorkerDone(i)

				c.maps.mu.Unlock()
				return nil
			}
		}

		c.maps.cond.Wait()
		c.maps.mu.Unlock()
	}
}

// wait worker until it's job is done, and listening worker's heart beat
func (c *Coordinator) waitWorkerDone(taskNumber int) {
	// fmt.Println("[test]waitWorkerDone goroutine starts")
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

	value, ok := c.maps.exeJobChan[int(*taskNumber)]
	if ok { // worker may be considered as dead
		c.maps.doneJob++
		value <- true
	}
	return nil
}
