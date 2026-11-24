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
	"time"
)

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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			return
		}
		task := reply.Task
		switch task.Type {
		case MapTask:
			doMap(task, mapf)
			reportDone(MapTask, task.ID)
		case ReduceTask:
			doReduce(task, reducef)
			reportDone(ReduceTask, task.ID)
		case WaitTask:
			time.Sleep(50 * time.Millisecond)
		case ExitTask:
			return
		}
	}
}

func reportDone(t int, id int) {
	args := ReportDoneArgs{Type: t, ID: id}
	reply := ReportDoneReply{}
	call("Coordinator.ReportDone", &args, &reply)
}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.File)
	if err != nil {
		log.Fatalf("map %d: read %s: %v", task.ID, task.File, err)
	}
	kvs := mapf(task.File, string(content))

	files := make([]*os.File, task.NReduce)
	encs := make([]*json.Encoder, task.NReduce)
	for r := 0; r < task.NReduce; r++ {
		f, err := os.CreateTemp(".", "mr-map-*tmp")
		if err != nil {
			log.Fatalf("map %d: createtemp: %v", task.ID, err)
		}
		files[r] = f
		encs[r] = json.NewEncoder(f)
	}

	for _, kv := range kvs {
		r := ihash(kv.Key) % task.NReduce
		if err := encs[r].Encode(kv); err != nil {
			log.Fatalf("map %d: encode: %v", task.ID, err)
		}
	}

	for r := 0; r < task.NReduce; r++ {
		tmpName := files[r].Name()
		if err := files[r].Close(); err != nil {
			log.Fatalf("map %d: close: %v", task.ID, err)
		}
		final := fmt.Sprintf("mr-%d-%d", task.ID, r)
		if err := os.Rename(tmpName, final); err != nil {
			log.Fatalf("map %d: rename %s: %v", task.ID, final, err)
		}
	}
}

func doReduce(task Task, reducef func(string, []string) string) {
	groups := make(map[string][]string)

	for m := 0; m < task.NMap; m++ {
		name := fmt.Sprintf("mr-%d-%d", m, task.ID)
		f, err := os.Open(name)
		if err != nil {
			log.Fatalf("reduce %d: open %s: %v", task.ID, name, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("reduce %d: decode %s: %v", task.ID, name, err)
			}
			groups[kv.Key] = append(groups[kv.Key], kv.Value)
		}
		f.Close()
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out, err := os.CreateTemp(".", "mr-out-*tmp")
	if err != nil {
		log.Fatalf("reduce %d: createtemp: %v", task.ID, err)
	}
	for _, k := range keys {
		v := reducef(k, groups[k])
		if _, err := fmt.Fprintf(out, "%v %v\n", k, v); err != nil {
			log.Fatalf("reduce %d: write: %v", task.ID, err)
		}
	}
	tmpName := out.Name()
	if err := out.Close(); err != nil {
		log.Fatalf("reduce %d: close: %v", task.ID, err)
	}
	if err := os.Rename(tmpName, fmt.Sprintf("mr-out-%d", task.ID)); err != nil {
		log.Fatalf("reduce %d: rename: %v", task.ID, err)
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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
