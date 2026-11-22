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

const TaskTimeout = 10 * time.Second

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Done
)

type TaskMeta struct {
	status    TaskStatus
	startTime time.Time
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

type Coordinator struct {
	mu      sync.Mutex
	files   []string
	nMap    int
	nReduce int
	maps    []TaskMeta
	reduces []TaskMeta
	phase   Phase
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
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func pickTask(tasks []TaskMeta) int {
	now := time.Now()
	for i := range tasks {
		if tasks[i].status == Idle || (tasks[i].status == InProgress && now.Sub(tasks[i].startTime) > TaskTimeout) {
			return i
		}
	}
	return -1
}

func allDone(tasks []TaskMeta) bool {
	for i := range tasks {
		if tasks[i].status != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == MapPhase {
		if id := pickTask(c.maps); id >= 0 {
			c.maps[id].status = InProgress
			c.maps[id].startTime = time.Now()
			reply.Task = Task{
				Type:    MapTask,
				ID:      id,
				File:    c.files[id],
				NReduce: c.nReduce,
				NMap:    c.nMap}
			return nil
		}
		if allDone(c.maps) {
			c.phase = ReducePhase
		} else {
			reply.Task = Task{Type: WaitTask}
			return nil
		}
	}
	if c.phase == ReducePhase {
		if id := pickTask(c.reduces); id >= 0 {
			c.reduces[id].status = InProgress
			c.reduces[id].startTime = time.Now()
			reply.Task = Task{
				Type:    ReduceTask,
				ID:      id,
				NReduce: c.nReduce,
				NMap:    c.nMap}
			return nil
		}
		if allDone(c.reduces) {
			c.phase = AllDone
		} else {
			reply.Task = Task{Type: WaitTask}
			return nil
		}
	}
	reply.Task = Task{Type: WaitTask}
	return nil
}

func (c *Coordinator) ReportDone(args *ReportDoneArgs, reply *ReportDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case MapTask:
		if args.ID >= 0 && args.ID < len(c.maps) && c.maps[args.ID].status != Done {
			c.maps[args.ID].status = Done
		}
	case ReduceTask:
		if args.ID >= 0 && args.ID < len(c.reduces) && c.reduces[args.ID].status != Done {
			c.reduces[args.ID].status = Done
		}
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == AllDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nMap:    len(files),
		nReduce: nReduce,
		maps:    make([]TaskMeta, len(files)),
		reduces: make([]TaskMeta, nReduce),
		phase:   MapPhase,
	}
	c.server(sockname)
	return &c
}
