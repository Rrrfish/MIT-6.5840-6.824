package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// Map task

	// fmt.Println("[test]Worker() starts!")
	// go func() {
	// fmt.Println("[test]map goroutine starts!")
	for {
		getMapTaskArgs := GetMapTaskArgs{}
		getMapTaskReply := GetMapTaskReply{}
		err := call("Coordinator.GetMapTask", getMapTaskArgs, &getMapTaskReply)
		if !err {
			log.Fatal("fetching task err: ", err)
		}
		if getMapTaskReply.MapOver {
			fmt.Println("[the map stage is over]")
			// break
			return
		} else {
			filename := getMapTaskReply.FileName
			taskNumber := getMapTaskReply.Number

			file, e := os.Open(filename)
			if e != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, e := io.ReadAll(file)
			if e != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			kva = sortAndCombine(kva)

			wirteIntermediate(kva, taskNumber, getMapTaskReply.NReduce)
			mapTaskDone(taskNumber)
		}
	}
	// }()

	// Reduce task

	// go func() {
	// 	GetReduceTaskArgs := GetReduceTaskArgs{}
	// 	GetReduceTaskReply := GetReduceTaskReply{}

	// }()

}

//
// map task
//

// sort and combine
func sortAndCombine(intermediate []KeyValue) []KeyValue {
	sort.Sort(ByKey(intermediate))
	ret := []KeyValue{}
	// fmt.Println("[test]before sorting and combination, intermediate len: ", len(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		key := intermediate[i].Key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		ret = append(ret, KeyValue{Key: key, Value: strconv.Itoa(j - i)})
		i = j
	}
	// fmt.Println("[test]after sorting and combination, intermediate len: ", len(ret))

	return ret
}

// wirte intermediate output of map task
func wirteIntermediate(intermediate []KeyValue, taskNumber int, nReduce int) error {
	taskNumberString := strconv.Itoa(taskNumber)
	// fmt.Println("[test] writeIntermediate(): taskNumber: ", taskNumberString)

	for _, kv := range intermediate {
		oname := "intermediateFile/mr-" + taskNumberString + "-" + strconv.Itoa(ihash(kv.Key)%nReduce)
		//fmt.Println("[test]intermediate filename: ", oname)
		ofile, err := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("cannot open file %v: %v", oname, err)
			return err
		}
		defer ofile.Close()

		enc := json.NewEncoder(ofile)
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("cannot encode kv: %v", err)
			return err
		}
	}

	return nil
}

// give coordinator a signal that map task has done
func mapTaskDone(taskNumber int) {
	reply := MapJobDoneReply{}

	call("Coordinator.MapJobDone", taskNumber, &reply)
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
