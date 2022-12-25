package mr

import (
	. "6.824/logging"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"
)

/*
	example function to show how to make an RPC call to the coordinator.

	the RPC argument and reply types are defined in rpc.go.
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
*/

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the `reduce`
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0
	}
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapFunc func(string, string) []KeyValue,
	reduceFunc func(string, []string) string) {
	// TODO Your worker implementation here.
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workId := fmt.Sprintf("%s:%v", hostname, pid)
	Logger.Info(fmt.Sprintf("Worker %s starts running......", workId))

	hbArgs := HeartBeatArgs{
		Id: WorkerId(workId),
	}
	hbReply := HeartBeatReply{}
	call("Coordinator.HeartBeat", &hbArgs, &hbReply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcName string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockName := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		Logger.Fatal("dialing:" + err.Error())
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	Logger.Error(err.Error())
	return false
}
