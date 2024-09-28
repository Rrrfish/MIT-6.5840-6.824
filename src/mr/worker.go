package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	var wg sync.WaitGroup
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// Reduce task

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			getReduceTaskArgs := GetReduceTaskArgs{}
			getReduceTaskReply := GetReduceTaskReply{}

			err := call("Coordinator.GetReduceTask", getReduceTaskArgs, &getReduceTaskReply)
			if !err {
				log.Fatal("fetching task err: ", err)
			}

			if getReduceTaskReply.ReduceOver {
				// fmt.Println("[reducer]receive over signal!")
				return
			}

			taskNumber := getReduceTaskReply.Number

			// fmt.Println("[reduce] reduce stage starts! taskNumber: ", taskNumber)
			kva := readIntermediateFiles(taskNumber)
			combineAndWrite(taskNumber, kva, reducef)

			reduceTaskDone(taskNumber)
		}

	}()

	// Map task
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			getMapTaskArgs := GetMapTaskArgs{}
			getMapTaskReply := GetMapTaskReply{}
			err := call("Coordinator.GetMapTask", getMapTaskArgs, &getMapTaskReply)
			if !err {
				log.Fatal("fetching task err: ", err)
			}
			if getMapTaskReply.MapOver {
				// fmt.Println("[the map stage is over]")
				break
				// return
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

				sortAndWrite(kva, taskNumber, getMapTaskReply.NReduce)

				// wirteIntermediate(kva, taskNumber, getMapTaskReply.NReduce)
				mapTaskDone(taskNumber)
			}
		}
	}()
	// }()
	wg.Wait()
}

//
// map task
//

// sort and write
func sortAndWrite(intermediate []KeyValue, taskNumber int, nReduce int) {
	taskNumberString := strconv.Itoa(taskNumber)

	sort.Sort(ByKey(intermediate))
	// ret := []KeyValue{}
	// fmt.Println("[test]before sorting and combination, intermediate len: ", len(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i
		key := intermediate[i].Key
		// oname := "intermediateFile/mr-" + taskNumberString + "-" + strconv.Itoa(ihash(key)%nReduce)
		oname := "./mr-" + taskNumberString + "-" + strconv.Itoa(ihash(key)%nReduce)
		ofile, err := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open file %v: %v", oname, err)
		}
		defer ofile.Close()

		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			enc := json.NewEncoder(ofile)
			if err := enc.Encode(&intermediate[j]); err != nil {
				log.Fatalf("cannot encode kv: %v", err)
			}
			j++
		}
		i = j
	}

}

// give coordinator a signal that map task has done
func mapTaskDone(taskNumber int) {
	reply := MapJobDoneReply{}

	call("Coordinator.MapJobDone", taskNumber, &reply)
}

//
// Reduce
//

// read corresponding intermediate files
func readIntermediateFiles(taskNumber int) []KeyValue {
	kva := []KeyValue{}
	// dir := "./intermediateFile"
	dir := "./"

	// 读取目录下的所有文件
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalf("无法读取目录: %v", err)
	}
	// taskName := fmt.Sprintf("mr-*- %d", taskNumber)

	for _, file := range files {
		// fmt.Println("[reduce]go into files range")
		// 检查文件是否匹配模式
		// if strings.Contains(file.Name(), taskName) {
		if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), fmt.Sprintf("-%d", taskNumber)) {
			filePath := filepath.Join(dir, file.Name())
			// fmt.Println("find file:", filePath)

			// 这里可以添加打开文件的逻辑
			// 例如，使用 os.Open(filePath) 打开文件
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatal("can not open file.")
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
	}

	return kva
}

// reducer combines data and writes output
func combineAndWrite(taskNumber int, intermediate []KeyValue, reducef func(string, []string) string) {
	sort.Sort(ByKey(intermediate))
	i := 0
	ofile, err := os.CreateTemp("./", "example-")
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Println("temp file name: ", ofile.Name())
	defer os.Remove(ofile.Name())

	for i < len(intermediate) {
		// fmt.Println("writing on temp file!")
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// oname := "intermediateFile/mr-out-" + strconv.Itoa(taskNumber)
	oname := "mr-out-" + strconv.Itoa(taskNumber)
	if err := os.Rename(ofile.Name(), oname); err != nil {
		log.Fatal("rename error: ", err)
	}
}

// give coordinator a signal that reduce task has done
func reduceTaskDone(taskNumber int) {
	reply := ReduceJobDoneReply{}

	// fmt.Println("[reduce]send a done signal")

	call("Coordinator.ReduceJobDone", taskNumber, &reply)
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
