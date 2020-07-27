package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// mr state for task and task manager: InitialState | InProcess | Completed
type MrState uint

// mr type: Map | Reduce
type MrType uint

// task/stage status
const (
	InitialState MrState = iota
	InProcess    MrState = iota
	Completed    MrState = iota
)

// task/stage status
const (
	Map    MrType = iota
	Reduce MrType = iota
)

const taskTimeout = 10 * time.Second

var masterLogger *log.Logger = log.New(log.Writer(), "Master node : ", log.LstdFlags)

// abstruction for task, store metadata of task
type Task struct {
	ID        int
	Type      MrType
	Files     []string
	State     MrState
	WorkerID  int
	Timestamp int64
}

type tasksManager struct {
	state         MrState
	taskType      MrType
	tasksMap      map[int]*Task
	waitList      map[int]*Task
	completedList map[int]*Task
}

type Master struct {
	listener          net.Listener
	nPartition        int
	mapTasksManger    tasksManager
	reduceTasksManger tasksManager
	jobPhase          MrType
	jobState          MrState
	// mux for job
	jobMux sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// The RPC handler for to send worker initial config
// return initial config
func (m *Master) WorkerConfig(args *WorkerConfigRequestArgs, reply *WorkerConfigRequestReply) error {
	masterLogger.Printf("WorkerConfig: recived config request with args : %+v\n", *args)
	reply.NumOfPartition = m.nPartition
	return nil
}

// The RPC handler for worker next task request
// return next task or error indicate need wait for other worker or all tasks completed
func (m *Master) NextTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	// initialize default reply
	masterLogger.Printf("NextTask: recived next task request with args : %+v\n", *args)
	reply.JobState = InProcess
	reply.WaitFlag = true
	reply.NextTask = nil

	// check previous task
	workerID := args.WorkerID
	preTask := args.LastCompletedTask
	taskResults := args.CompletedTaskResults

	if preTask != nil {
		m.ackTask(preTask, taskResults)
	}

	m.jobMux.Lock()

	// check current job status, if job completed indicate worker to terminate
	if m.jobState == Completed {
		reply.JobState = Completed
		reply.WaitFlag = false
		masterLogger.Printf("NextTask: job state completed, request worker to stop. reply: %+v\n", *reply)
		return nil
	}

	m.jobMux.Unlock()

	// find next task, assgin to worker
	reply.NextTask = m.nextAvailableTask(workerID)

	// if no task available set wait flag
	if reply.NextTask == nil {
		reply.WaitFlag = true
		masterLogger.Printf("NextTask: all map tasks in process, request worker to wait. reply: %+v\n", *reply)
	} else {
		reply.WaitFlag = false
		go m.setTaskTimer(reply.NextTask.ID, reply.NextTask.Type)
		masterLogger.Printf("NextTask: find available task, assgin to worker. reply: %+v\n", *reply)
	}

	return nil
}

func (m *Master) ackTask(completedTask *Task, taskResults []string) {

	id := completedTask.ID
	taskType := completedTask.Type
	manager := m.getTaskManager(taskType)

	m.jobMux.Lock()
	defer m.jobMux.Unlock()
	task := manager.tasksMap[id]

	masterLogger.Printf("ackTask: completedTask from worker: %+v, task in manager: %+v\n", *completedTask, *task)

	if task.State == Completed {
		masterLogger.Println("ackTask: task alread finished")
		return
	}

	if completedTask.State != Completed {
		masterLogger.Println("ackTask: return task has not finished")
		return
	}

	// check worker id, if worker id do not equal means task was re-assgined to other
	if task.WorkerID != completedTask.WorkerID {
		masterLogger.Println("ackTask: worker ID does not match, ignore completed task")
		return
	}

	// update task status, and add to completedList
	task.State = Completed
	task.Timestamp = time.Now().Unix()

	// task result for map task, add intermediate files to reduce task
	if taskType == Map {

		for _, file := range taskResults {
			reduceTaskID := getIDFromFileName(file)
			if reduceTaskID >= 0 {
				reduceTask := m.reduceTasksManger.tasksMap[reduceTaskID]
				reduceTask.Files = append(reduceTask.Files, file)
				masterLogger.Printf("ackTask: add intermediate files %s to reduce task - %d\n", file, reduceTaskID)
			}
		}

	}

	manager.completedList[id] = task
	masterLogger.Printf("ackTask: ack %v task - %d\n", taskType, id)

	// check stage status, promote to next stage if current stage completed
	if len(manager.tasksMap) == len(manager.completedList) {
		manager.state = Completed

		if manager.taskType == Map {
			m.jobPhase = Reduce
			masterLogger.Println("ackTask: switch to reduce phase")
		} else {
			m.jobState = Completed
			masterLogger.Println("ackTask: job completed")
		}
	}
}

func getIDFromFileName(fileName string) int {
	// intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task numbe
	tokens := strings.Split(fileName, "-")
	if len(tokens) != 3 {
		return -1
	}
	id, err := strconv.Atoi(tokens[2])
	if err != nil {
		return -1
	}
	return id
}

func (m *Master) nextAvailableTask(workerID int) (nextTask *Task) {
	m.jobMux.Lock()
	defer m.jobMux.Unlock()

	manager := m.getTaskManager(m.jobPhase)

	if m.jobState == Completed {
		masterLogger.Println("nextAvailableTask: job completed")
		return
	}

	// all tasks currently in process, return empty task
	if len(manager.waitList) == 0 {
		masterLogger.Printf("nextAvailableTask: all %v tasks in process\n", manager.taskType)
		return
	}

	var id int
	for id, nextTask = range manager.waitList {
		break
	}
	delete(manager.waitList, id)
	nextTask.WorkerID = workerID
	nextTask.State = InProcess
	nextTask.Timestamp = time.Now().Unix()
	masterLogger.Printf("nextAvailableTask: next task - id: %d, type: %v\n", nextTask.ID, nextTask.Type)
	return
}

func (m *Master) setTaskTimer(taskID int, taskType MrType) {

	// wait for timeout, if not completed in time, reset task
	masterLogger.Printf("setTaskTimer: start timer for task - id: %d, type: %v\n", taskID, taskType)
	time.Sleep(taskTimeout)

	m.jobMux.Lock()
	defer m.jobMux.Unlock()
	masterLogger.Printf("setTaskTimer: timer timeout for task - id: %d, type: %v\n", taskID, taskType)

	manager := m.getTaskManager(taskType)
	task := manager.tasksMap[taskID]

	// still in process then reset task, assumme worker has some problem
	if task.State != Completed {
		task.WorkerID = -1
		task.State = InitialState
		task.Timestamp = time.Now().Unix()
		manager.waitList[taskID] = task
		masterLogger.Printf("setTaskTimer: task - id: %d, type: %v - timeout reset to initial state \n", taskID, taskType)
	} else {
		masterLogger.Printf("setTaskTimer: task - id: %d, type: %v - completed in time\n", taskID, taskType)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	m.listener = l
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.jobMux.Lock()

	if m.jobState == Completed {
		ret = true
		m.listener.Close()
	}

	m.jobMux.Unlock()
	masterLogger.Printf("Done: job completed : %v \n", ret)

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	masterLogger.Println("MakeMaster: initilizing...")
	m := Master{}
	// initilize master
	m.nPartition = nReduce
	m.jobState = InitialState
	m.jobPhase = Map
	m.initializeMapTaskManager(files)
	m.initializeReduceTaskManager(nReduce)
	m.server()
	masterLogger.Println("MakeMaster: server start")
	m.jobState = InProcess
	masterLogger.Println("MakeMaster: initilization finised.")
	return &m
}

func (m *Master) initializeMapTaskManager(files []string) {
	masterLogger.Println("initializeMapTaskManager: initilizing master map task manager...")
	size := len(files)
	m.mapTasksManger = tasksManager{InitialState, Map, make(map[int]*Task, size), make(map[int]*Task, size), make(map[int]*Task, size)}

	// build map task map and add to queue
	for i, file := range files {
		mapTask := Task{i, Map, []string{file}, InitialState, -1, time.Now().Unix()}
		m.mapTasksManger.tasksMap[i] = &mapTask
		m.mapTasksManger.waitList[i] = &mapTask
	}
	m.mapTasksManger.state = InProcess
}

func (m *Master) initializeReduceTaskManager(nReduce int) {
	masterLogger.Println("initializeReduceTaskManager: initilizing master reduce task manager...")
	m.reduceTasksManger = tasksManager{InitialState, Reduce, make(map[int]*Task, nReduce), make(map[int]*Task, nReduce), make(map[int]*Task, nReduce)}

	// build reduce task map and queue
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{i, Reduce, []string{}, InitialState, -1, time.Now().Unix()}
		m.reduceTasksManger.tasksMap[i] = &reduceTask
		m.reduceTasksManger.waitList[i] = &reduceTask
	}
	m.reduceTasksManger.state = InProcess
}

func (m *Master) getTaskManager(taskType MrType) *tasksManager {
	if taskType == Map {
		return &(m.mapTasksManger)
	} else {
		return &(m.reduceTasksManger)
	}
}
