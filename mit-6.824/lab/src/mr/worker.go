package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import (
	"os"
	"io/ioutil"
	"encoding/json"
	"time"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// some const used by worker
const maxRPCRetry = 3
const workerSleepTime = 2 * time.Second

// global variable for worker
var debugWorkerFlag bool = false
var workerLogger *log.Logger = log.New(log.Writer(), "worker node: ", log.LstdFlags)

var workerLogPreFix string = "Worker %d: "
var intermediateFileName string = "mr-%d-%d"
var outputFileName string = "mr-out-%d"

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// initial worker
	workerID := os.Getpid()
	currDirPath, err := os.Getwd()
    if err != nil {
		workerLogger.Fatalf(workerLogPreFix + "worker terminate - fail to get current work dir path cause: %s.\n", workerID, err.Error())
	}

	configArgs := WorkerConfigRequestArgs{workerID}
	configReply := WorkerConfigRequestReply{}
	nPartition := -1
	if call("Master.WorkerConfig", &configArgs, &configReply) {
		nPartition = configReply.NumOfPartition
	} else {
		workerLogger.Fatalf(workerLogPreFix + "worker terminate - fail to worker config master.\n", workerID)
	}

	retry := maxRPCRetry
	var lastCompletedTask *Task = nil
	completedTaskResults := []string{}

	workerLogger.Printf(workerLogPreFix + "worker initialized with nPartition: %d, curDir: %s.\n", workerID, nPartition, currDirPath)

	// periodically request next task, if request goes wrong after max retry, terminate worker assume master die
	for {
		// declare an argument structure.
		args := TaskRequestArgs{workerID, lastCompletedTask, completedTaskResults}

		// declare a reply structure.
		reply := TaskRequestReply{}

		// within maxRPCretry, try next task request
		for retry > 0 {
			if call("Master.NextTask", &args, &reply) {
				retry = maxRPCRetry;
				break
			} else {
				retry--
			}
		}

		// fail for max retry
		if retry != maxRPCRetry {
			workerLogger.Fatalf(workerLogPreFix + "worker terminate - fail next task request excess max retry\n", workerID)
		}

		lastCompletedTask = reply.NextTask
		completedTaskResults = []string{}
		
		if reply.JobState == Completed {
			// if job finished terminate
			workerLogger.Printf(workerLogPreFix + "worker terminate - job has completed.\n", workerID)
			return
		} else if reply.WaitFlag {
			workerLogger.Printf(workerLogPreFix + "worker sleep - no available task.\n", workerID)
			time.Sleep(workerSleepTime)
		} else {
			switch reply.NextTask.Type {
				case Map:
					completedTaskResults = handleMapTask(workerID, reply.NextTask, mapf, nPartition, currDirPath)
				case Reduce:
					completedTaskResults = handleReduceTask(workerID, reply.NextTask, reducef, currDirPath)
			}
		}
		if len(completedTaskResults) == 0 {
			lastCompletedTask = nil
		}
	}

}

// if error happen in the middle, return empty result, do not return partial result
func handleMapTask(workerID int, task *Task, mapf func(string, string) []KeyValue, nPartition int, dstDir string) []string {
	workerLogger.Printf(workerLogPreFix + "worker handler map task - start map task ID: %d.\n", workerID, task.ID)
	empty := []string{}
	results := []string{}

	// read file then apply mapf get intermediate key/value
	intermediate := []KeyValue{}
	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			workerLogger.Printf(workerLogPreFix + "worker handler map task - can not open file: %s.\n", workerID, filename)
			return empty
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			workerLogger.Printf(workerLogPreFix + "worker handler map task - can not read file: %s.\n", workerID, filename)
			return empty
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	workerLogger.Printf(workerLogPreFix + "worker handler map task - get all intermediate key/value.\n", workerID)

	// partition intermediate key/value by ihash(key) % NReduce 
	intermediateMap := map[int] []KeyValue {}

	for _, kv := range intermediate {
		partitionID := ihash(kv.Key) % nPartition
		arr, ok := intermediateMap[partitionID]
		if !ok {
			intermediateMap[partitionID] = []KeyValue{kv}
		} else {
			intermediateMap[partitionID] = append(arr, kv)
		}	
	}

	workerLogger.Printf(workerLogPreFix + "worker handler map task - partitioned all intermediate key/value.\n", workerID)

	// save each partition to tmp file first then rename, using json encode
	for k, arr := range intermediateMap {
		fileName := fmt.Sprintf(intermediateFileName, task.ID, k)
		tmpfile, err := ioutil.TempFile("", fileName+"-*")
		if err != nil {
			workerLogger.Printf(workerLogPreFix + "worker handler map task - fail to create tmp file: %s. Casuse: %s.\n", workerID, fileName, err.Error())
			return empty
		}
		enc := json.NewEncoder(tmpfile)
		defer os.Remove(tmpfile.Name())

		for _, kv := range arr {
			if err := enc.Encode(&kv); err != nil {
				workerLogger.Printf(workerLogPreFix + "worker handler map task - fail to encode kv pair: %+v. Cause: %s\n", workerID, kv, err.Error())
				return empty
			}
		}

		if err := tmpfile.Close(); err != nil {
			workerLogger.Printf(workerLogPreFix + "worker handler map task - fail to close tmp file: %s. Casuse: %s.\n", workerID, fileName, err.Error())
			return empty
		}
		// rename 
		dstPath :=  dstDir + "/" + fileName;
		if err := os.Rename(tmpfile.Name(), dstPath); err != nil {
			workerLogger.Printf(workerLogPreFix + "worker handler map task - fail to rename tmp file: %s to dst file: %s. Casuse: %s.\n", workerID, fileName, dstPath, err.Error())
			return empty
		}
		results = append(results, fileName)
	}

	task.State = Completed
	task.Timestamp = time.Now().Unix()
	workerLogger.Printf(workerLogPreFix + "worker handler map task - Completed map task ID: %d with results: %+v.\n", workerID, task.ID, results)
	return results
}

func handleReduceTask(workerID int, task *Task, reducef func(string, []string) string, dstDir string) []string {
	workerLogger.Printf(workerLogPreFix + "worker handler reduce task - start redcue task ID: .\n", workerID, task.ID)
	empty := []string{}
	results := []string{}

	// decode from file get intermediate key/value
	intermediate := []KeyValue{}
	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			workerLogger.Printf(workerLogPreFix + "worker handler reduce task - can not open file: %s.\n", workerID, filename)
			return empty
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				workerLogger.Printf(workerLogPreFix + "worker handler reduce task - can decode file: %s.\n", workerID, filename)
				return empty
			}
			intermediate = append(intermediate, kv)
		}
	}

	workerLogger.Printf(workerLogPreFix + "worker handler reduce task - load all intermediate key/value.\n", workerID)

	sort.Sort(ByKey(intermediate))

	fileName := fmt.Sprintf(outputFileName, task.ID)
	tmpfile, err := ioutil.TempFile("", fileName+"-*")
	if err != nil {
		workerLogger.Printf(workerLogPreFix + "worker handler reduce task - fail to create tmp file: %s. Casuse: %s.\n", workerID, fileName, err.Error())
		return empty
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
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
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpfile.Close()

	// rename tmp file
	dstPath :=  dstDir + "/" + fileName;
	if err := os.Rename(tmpfile.Name(), dstPath); err != nil {
		workerLogger.Printf(workerLogPreFix + "worker handler reduce task - fail to rename tmp file: %s to dst file: %s. Casuse: %s.\n", workerID, fileName, dstPath, err.Error())
		return empty
	}
	
	results = append(results, fileName)
	task.State = Completed
	task.Timestamp = time.Now().Unix()
	workerLogger.Printf(workerLogPreFix + "worker handler reduce task - Completed map task ID: %d with results: %+v.\n", workerID, task.ID, results)
	return results
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
