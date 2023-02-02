package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/rs/xid"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		reply, ok := CallAsk()
		if !ok {
			//	 if cont connect, return
			log.Fatalf("callAsk error")
		}
		state := reply.State
		//no job sleep
		if state == 2 {
			time.Sleep(100 * time.Millisecond)
		} else if state == 3 {
			return
		}
		//deliver and do it!
		// 1. map
		if state == 0 {
			nReduce := reply.NReduce
			fileName := reply.InputfileName
			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
			}
			file.Close()
			kva := mapf(fileName, string(content))

			workId := xid.New().String()

			// write to file workid-jobid-x
			// first split to n intermediate
			intermediate := make([][]KeyValue, nReduce)
			x := 0
			for _, KV := range kva {
				//get hash code
				x = ihash(KV.Key) % nReduce
				intermediate[x] = append(intermediate[x], KV)
			}
			//open out file and write workId-jobid-i
			oname := "/root/tmp/" + workId + "-" + strconv.Itoa(reply.JobId)
			for i := 0; i < nReduce; i++ {
				xname := oname + "-" + strconv.Itoa(i)
				ofile, err := os.Create(xname)
				if err != nil {
					log.Fatalf("Create %v file fail,error:%v", xname, err)
				}
				enc := json.NewEncoder(ofile)
				for _, kv := range intermediate[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot write to %v", xname)
					}
				}
				ofile.Close()
			}
			// then ret to coodinator
			ret := CallRet(reply.JobId, workId, state)
			if !ret {
				log.Fatal("CallRet fail")
			}
			// 2. reduce   /tmp/mr- i -jobid
		} else if state == 1 {
			nMap := reply.NReduce
			jobId := reply.JobId
			// read ./tmp/mr and process

			//need a kva
			kva := make([]KeyValue, 0)
			ifileprex := "/root/tmp/mr-"
			//for each file
			for i := 0; i < nMap; i++ {
				ifilename := ifileprex + strconv.Itoa(i) + "-" + strconv.Itoa(jobId)
				ifile, err := os.Open(ifilename)
				if err != nil {
					log.Fatalf("cannot open %v", ifilename)
				}
				dec := json.NewDecoder(ifile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				ifile.Close()
			}
			// now kva has all kv process it by reduce
			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(jobId)
			ofile, _ := os.Create(oname)
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
			//all finished call CallRet
			ret := CallRet(jobId, "0", state)
			if !ret {
				log.Fatalf("callRet error in reduce!")
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func CallAsk() (CoordAskReply, bool) {
	// declare an argument structure.
	args := CoordAskArgs{}
	// declare a reply structure.
	reply := CoordAskReply{}

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.CoordAskCall", &args, &reply)
	if ok {
		return reply, ok
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply, ok
}

func CallRet(jobId int, workId string, state int) bool {
	// declare an argument structure.
	args := CoordRetArgs{}
	// declare a reply structure.
	reply := CoordRetReply{}

	args.JobId = jobId
	args.WorkerId = workId //workId
	args.State = state

	// send the RPC request, wait for the reply.
	ok := call("Coordinator.CoordRetCall", &args, &reply)
	if ok {
		return ok
	} else {
		fmt.Printf("call failed!\n")
	}
	return ok
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
