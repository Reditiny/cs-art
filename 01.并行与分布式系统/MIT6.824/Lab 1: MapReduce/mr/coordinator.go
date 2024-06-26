package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	// 0 means not start, 1 means in progress
	MapJobs            sync.Map
	ReduceJobs         sync.Map
	CurDir             string
	RemainMapJobCount  int
	RemainRedJobCount  int
	RemainMapJobThread int
	RemainRedJobThread int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) IsMapJobFinished(args *IsJobFinishedArgs, reply *IsJobFinishedReply) error {
	reply.Finished = c.RemainMapJobCount == 0
	return nil
}

func (c *Coordinator) IsReduceJobFinished(args *IsJobFinishedArgs, reply *IsJobFinishedReply) error {
	reply.Finished = c.RemainRedJobCount == 0
	return nil
}

func (c *Coordinator) GetReduceJob(args *GetJobArgs, reply *GetJobReply) error {
	if c.RemainRedJobThread == 0 {
		return nil
	}

	c.ReduceJobs.Range(func(key, value interface{}) bool {
		if value.(int) == 0 {
			reply.ReduceJobKey = key.(int)
			reply.HasJob = true
			c.RemainRedJobThread--
			c.ReduceJobs.Store(key, 1)
			fmt.Printf("reduce job %d assigned\n", reply.ReduceJobKey)
			return false
		}
		return true

	})
	if reply.HasJob {
		go c.wait10s(&c.ReduceJobs, strconv.Itoa(reply.ReduceJobKey), false)
	}
	return nil
}

func (c *Coordinator) GetMapJob(args *GetJobArgs, reply *GetJobReply) error {
	if c.RemainMapJobThread == 0 {
		return nil
	}

	c.MapJobs.Range(func(key, value interface{}) bool {
		if value.(int) == 0 {
			reply.MapJobNum = c.RemainMapJobThread
			c.RemainMapJobThread--
			reply.MapJobKey = key.(string)
			c.MapJobs.Store(key, 1)
			reply.HasJob = true
			fmt.Printf("map job %s assigned\n", reply.MapJobKey)
			return false
		}
		return true

	})
	if reply.HasJob {
		go c.wait10s(&c.MapJobs, reply.MapJobKey, true)
	}
	return nil
}

func (c *Coordinator) wait10s(m *sync.Map, key string, isMap bool) {
	time.Sleep(time.Second * 10)
	if value, ok := m.Load(key); ok && value.(int) == 1 {
		fmt.Printf("job %s timeout, reset job status\n", key)
		m.Store(key, 0)
		if isMap {
			c.RemainMapJobThread++
		} else {
			c.RemainRedJobThread++
		}
	}
}

func (c *Coordinator) FinishReduceJob(args *FinishJobArgs, reply *FinishJobReply) error {
	if value, ok := c.ReduceJobs.Load(args.ReduceJobKey); ok && value.(int) == 1 {
		c.ReduceJobs.Delete(args.ReduceJobKey)
		reply.Success = true
		c.RemainRedJobCount--
		c.RemainRedJobThread++
		fmt.Printf("reduce job %d finished\n", args.ReduceJobKey)
	}
	return nil
}

func (c *Coordinator) FinishMapJob(args *FinishJobArgs, reply *FinishJobReply) error {
	if value, ok := c.MapJobs.Load(args.MapJobKey); ok && value.(int) == 1 {
		c.MapJobs.Delete(args.MapJobKey)
		reply.Success = true
		c.RemainMapJobCount--
		c.RemainMapJobThread++
		fmt.Printf("map job %s finished\n", args.MapJobKey)
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println("receive Example RPC")
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
	ret = c.RemainMapJobCount == 0 && c.RemainRedJobCount == 0

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return nil
	}
	c.CurDir = currentDir

	targetFiles := make([]string, 0)
	for _, file := range files {
		matches, _ := filepath.Glob(filepath.Join(currentDir, file))
		targetFiles = append(targetFiles, matches...)
	}
	c.RemainMapJobCount = len(targetFiles)
	for _, file := range targetFiles {
		c.MapJobs.Store(file, 0)
	}

	c.RemainRedJobCount = nReduce
	for i := 1; i <= nReduce; i++ {
		c.ReduceJobs.Store(i, 0)
	}

	c.RemainMapJobThread = 4
	c.RemainRedJobThread = nReduce

	c.server()
	return &c
}
