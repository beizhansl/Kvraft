package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// 1. mapjob
	// 2. reducejob
	// 3. pharse 0 map 1 reduce 2 done
	pharse            int
	mapJob            []bool
	mapJobDone        []bool
	leftMapJob        int
	leftMapJobDone    int
	fileName          []string
	reduceJob         []bool
	reduceJobDone     []bool
	leftReduceJob     int
	leftReduceJobDone int
	mutex             sync.RWMutex
	nReduce           int
	nMap              int
}

// Your code here -- RPC handlers for the worker to call.
//Ask rpc
func (c *Coordinator) CoordAskCall(args *CoordAskArgs, reply *CoordAskReply) error {
	if c.pharse == 2 {
		reply.State = 3
		return nil
	}
	// is map work
	if c.pharse == 0 {
		c.mutex.Lock()
		// no Job can be deliver, tell worker to sleep and try again.
		if c.leftMapJob == 0 {
			reply.State = 2
			c.mutex.Unlock()
			return nil
		}
		for i := 0; i < len(c.mapJob); i++ {
			// deliver map job
			if c.mapJob[i] == false {
				reply.State = 0
				reply.NReduce = c.nReduce
				reply.JobId = i
				reply.InputfileName = c.fileName[i]
				c.leftMapJob -= 1
				c.mapJob[i] = true
				c.mutex.Unlock()
				go c.waitMR(i, 0)
				return nil
			}
		}
	}
	// is reduce work
	if c.pharse == 1 {
		c.mutex.Lock()
		if c.leftReduceJob == 0 {
			reply.State = 2
			c.mutex.Unlock()
			return nil
		}
		for i := 0; i < len(c.reduceJob); i++ {
			// deliver reduce job
			if c.reduceJob[i] == false {
				reply.State = 1
				reply.JobId = i
				reply.NReduce = c.nMap
				c.reduceJob[i] = true
				c.leftReduceJob -= 1
				c.mutex.Unlock()
				go c.waitMR(i, 1)
				return nil
			}
		}
	}
	return nil
}

//Return rpc
func (c *Coordinator) CoordRetCall(args *CoordRetArgs, reply *CoordRetReply) error {
	//Coord
	c.mutex.Lock()
	if args.State != c.pharse {
		c.mutex.Unlock()
		return nil
	}
	// map
	if c.pharse == 0 {
		jobId := args.JobId
		if c.mapJobDone[jobId] == false {
			workId := args.WorkerId
			//rename为中间文件 --> mr-X-Y
			for i := 0; i < c.nReduce; i++ {
				fileName := "/root/tmp/" + workId + "-" + strconv.Itoa(jobId) + "-" + strconv.Itoa(i)
				fileNewName := "/root/tmp/mr" + "-" + strconv.Itoa(jobId) + "-" + strconv.Itoa(i)
				err1 := os.Rename(fileName, fileNewName)
				// error broken!
				if err1 != nil {
					c.mutex.Unlock()
					return nil
				}
			}
			c.leftMapJobDone -= 1
			c.mapJobDone[jobId] = true
			if c.leftMapJobDone == 0 {
				c.pharse = 1
				fmt.Println("now into pharse 1")
			}
			c.mutex.Unlock()
			return nil
		} else {
			// now it's slow worker
			c.mutex.Unlock()
			return nil
		}
		// reduce
	} else if c.pharse == 1 {
		jobId := args.JobId
		if c.reduceJobDone[jobId] == false {
			c.leftReduceJobDone -= 1
			c.reduceJobDone[jobId] = true
			if c.leftReduceJobDone == 0 {
				c.pharse = 2
				fmt.Println("now into pharse 2")
			}
			c.mutex.Unlock()
			return nil
		} else {
			// now it's a slow worker
			c.mutex.Unlock()
			return nil
		}
	}
	c.mutex.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	if c.pharse == 2 {
		ret = true
	}
	c.mutex.Unlock()
	return ret
}

//
// use timer to look worker
func (c *Coordinator) waitMR(jobId int, ismr int) {
	timer := time.NewTicker(time.Second * 1)
	times := 0
	for times < 10 {
		select {
		case <-timer.C:
			times++
			c.mutex.Lock()
			if ismr == 0 {
				//done , return
				if c.mapJobDone[jobId] == true {
					c.mutex.Unlock()
					timer.Stop()
					return
				}
				c.mutex.Unlock()
			} else if ismr == 1 {
				if c.reduceJobDone[jobId] == true {
					c.mutex.Unlock()
					timer.Stop()
					return
				}
				c.mutex.Unlock()
			}
		}
	}
	timer.Stop()
	// over time! release the job.
	c.mutex.Lock()
	if ismr == 0 && c.mapJobDone[jobId] == false {
		c.mapJob[jobId] = false
		c.leftMapJob++
	}
	if ismr == 1 && c.reduceJobDone[jobId] == false {
		c.reduceJob[jobId] = false
		c.leftReduceJob++
	}
	c.mutex.Unlock()
	return
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.pharse = 0

	lenf := len(files)
	c.nMap = lenf
	c.fileName = make([]string, lenf)
	c.mapJob = make([]bool, lenf)
	c.mapJobDone = make([]bool, lenf)
	c.leftMapJob = lenf
	c.leftMapJobDone = lenf
	for i, filename := range files {
		c.fileName[i] = filename
		c.mapJob[i] = false
		c.mapJobDone[i] = false
	}
	c.reduceJob = make([]bool, nReduce)
	c.reduceJobDone = make([]bool, nReduce)
	c.leftReduceJob = nReduce
	c.leftReduceJobDone = nReduce
	for i := 0; i < nReduce; i++ {
		c.reduceJob[i] = false
		c.reduceJobDone[i] = false
	}

	c.server()
	return &c
}
