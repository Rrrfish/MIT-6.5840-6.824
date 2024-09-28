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
	done    chan bool
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
	// ret := true
	ret := <-c.done

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.done = make(chan bool)

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

//
// Map stage
//

// allocate map job for workers
func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {

	for {
		c.maps.mu.Lock()

		if c.maps.sumJob == c.maps.doneJob {
			// fmt.Println("[!]map stage is over!")
			reply.MapOver = true
			c.maps.cond.Broadcast()
			c.maps.mu.Unlock()
			c.reduces.cond.Broadcast()
			return nil
		}

		for i, v := range c.maps.available {
			if v {
				c.maps.available[i] = false
				reply.Number = i
				reply.NReduce = c.reduces.sumJob
				reply.FileName = c.maps.filename[i]
				reply.MapOver = false
				c.maps.exeJobChan[i] = make(chan bool)

				go c.waitMapWorkerDone(i)

				c.maps.mu.Unlock()
				return nil
			}
		}

		c.maps.cond.Wait()
		c.maps.mu.Unlock()
	}
}

// wait worker until it's job is done, and listening worker's heart beat
func (c *Coordinator) waitMapWorkerDone(taskNumber int) {
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
			// fmt.Println("[worker dies]taskNumber: ", taskNumber)
			c.maps.available[taskNumber] = true
			delete(c.maps.exeJobChan, taskNumber)
			c.maps.cond.Broadcast() // signal a waiting worker for Map task
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

//
// reduce stage
//

// allocate a reduce task for a worker
func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	// fmt.Println("[rpc]GetReduceTask() invoked")
	for {
		// fmt.Println("for loop again!")
		c.reduces.mu.Lock()
		if c.maps.doneJob == c.maps.sumJob { // map stage is over

			if c.reduces.doneJob == c.reduces.sumJob {
				reply.ReduceOver = true
				// fmt.Println("reduce stage is over!")
				c.done <- true
				return nil
			}

			// fmt.Println("[test]get a reduce task!")
			for i, v := range c.reduces.available {
				if v {
					reply.Number = i
					reply.ReduceOver = false
					c.reduces.available[i] = false
					c.reduces.exeJobChan[i] = make(chan bool)
					c.reduces.mu.Unlock()
					// fmt.Println("reduces.available is ok! fetch task: ", i)
					go c.waitReduceWorkerDone(i)

					return nil
				}
			}

		}
		// fmt.Println("[reduce]waiting for reduce task...")
		c.reduces.cond.Wait()
		c.reduces.mu.Unlock()
		// fmt.Println("[reduce]awake!")
	}

}

// wait worker until it's job is done, and listening worker's heart beat
func (c *Coordinator) waitReduceWorkerDone(taskNumber int) {
	// fmt.Println("waiting reduce task done...")
	c.reduces.mu.Lock()

	waitJobDone := c.reduces.exeJobChan[taskNumber]
	c.reduces.mu.Unlock()
	timeout := time.After(10 * time.Second)

	for {
		// fmt.Println("waiting...")
		select {
		case <-waitJobDone:
			c.reduces.mu.Lock()
			defer c.reduces.mu.Unlock()
			// fmt.Println("[done]reduce task ", taskNumber, " done!")
			delete(c.reduces.exeJobChan, taskNumber)
			return
		case <-timeout:
			c.reduces.mu.Lock()
			defer c.reduces.mu.Unlock()
			// fmt.Println("[worker dies]task number: ", taskNumber)
			delete(c.reduces.exeJobChan, taskNumber)
			c.reduces.available[taskNumber] = true
			c.reduces.cond.Broadcast()
			return
		}

	}
}

// worker use this to notify coordinator work done
func (c *Coordinator) ReduceJobDone(taskNumber *TaskNumber, reply *ReduceJobDoneReply) error {
	c.reduces.mu.Lock()
	defer c.reduces.mu.Unlock()

	value, ok := c.reduces.exeJobChan[int(*taskNumber)]

	if ok {
		// fmt.Println("signal to reduce done job!")
		c.reduces.doneJob++
		value <- true
	} else {
		// fmt.Println("wait! no executing job?")
	}

	return nil
}
