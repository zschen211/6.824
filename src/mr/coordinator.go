package mr

import (
	. "6.824/logging"
	"container/list"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

/*
	an example RPC handler.

	the RPC argument and reply types are defined in rpc.go.
	func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
		reply.Y = args.X + 1
		return nil
	}
*/

type WorkerId string
type TaskId string
type TaskState int
type TaskType string

const timeout = 30 * time.Second

const (
	Map    TaskType = "map"
	Reduce          = "reduce"
)

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	taskType        TaskType
	state           TaskState
	inputFilenames  []string
	resultFilenames []string
}

type WorkerState struct {
	workerId WorkerId
	timer    *time.Timer
}

type WorkerPool struct {
	mtx            sync.Mutex
	list           *list.List
	nextToSchedule *list.Element
	assignment     map[WorkerId][]TaskId
}

type Coordinator struct {
	// TODO Your definitions here.
	workerPool     *WorkerPool
	tasks          map[TaskId]*Task
	inputs         []string
	taskQueue      chan *Task
	taskCompletion chan *Task
	Finished       chan bool
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workerPool:     &WorkerPool{list: list.New(), nextToSchedule: nil, assignment: make(map[WorkerId][]TaskId)},
		tasks:          make(map[TaskId]*Task),
		inputs:         files,
		taskQueue:      make(chan *Task, len(files)+nReduce),
		taskCompletion: make(chan *Task, len(files)+nReduce),
		Finished:       make(chan bool, 1),
	}

	// start the thread that serve RPC handlers
	c.server()

	// start the thread that schedule tasks to workers
	c.schedule()

	// start the thread that manage tasks
	c.manageTasks(len(files), nReduce)

	return &c
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		return
	}
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "127.0.0.1:1234")
	if e != nil {
		Logger.Fatal("Listen error:" + e.Error())
	}
	go http.Serve(l, nil)
	Logger.Info("Start serving RPC handlers......")
}

// schedule starts a new thread that schedule tasks to workers with round-robin strategy
func (c *Coordinator) schedule() {
	go func() {
		for task := range c.taskQueue { // get task from taskQueue

			c.workerPool.mtx.Lock()

			// TODO: conditional wait - blocks if there is no worker in the pool

			// assign the task to the worker stored in `nextToSchedule`
			workerState := c.workerPool.nextToSchedule.Value.(WorkerState)
			c.assignTaskToWorker(task, workerState.workerId)
			if c.workerPool.nextToSchedule.Next() == nil {
				c.workerPool.nextToSchedule = c.workerPool.list.Front()
			} else {
				c.workerPool.nextToSchedule = c.workerPool.nextToSchedule.Next()
			}
			Logger.Info(fmt.Sprintf("%s", c))

			c.workerPool.mtx.Unlock()
		}
	}()
}

// checkFinish starts a new thread manage map & reduce tasks
func (c *Coordinator) manageTasks(nMap int, nReduce int) {
	go func() {
		// register Map tasks and add to task queue
		for i, file := range c.inputs {
			c.addNewTask(&Task{
				taskType:        Map,
				state:           Idle,
				inputFilenames:  []string{file},
				resultFilenames: []string{fmt.Sprintf("map-out-%v", i)},
			})
		}
		Logger.Info(fmt.Sprintf("Registered all `Map` tasks in Coordinator !\n%s", c))

		mapCompleted, reduceCompleted := 0, 0

		for task := range c.taskCompletion {
			switch task.taskType {
			case Map:
				mapCompleted += 1
			case Reduce:
				reduceCompleted += 1
			}
			Logger.Info(fmt.Sprintf("A task is completed. Completion status: [map]:%v/%v; [reduce]:%v/%v",
				mapCompleted, nMap, reduceCompleted, nReduce))

			// send `finish` message to channel if all tasks are completed
			if mapCompleted >= nMap && reduceCompleted >= nReduce {
				Logger.Info("Finished MapReduce ! Exiting......")
				c.Finished <- true
				return
			}

			if mapCompleted >= nMap {
				// TODO register Map tasks and add to task queue
			}
		}
	}()
}

// addWorker adds a new worker to `assignment` map
// do nothing if the workerId is already in the map
func (c *Coordinator) addWorker(workerId WorkerId) {
	// TODO
	c.workerPool.mtx.Lock()
	defer c.workerPool.mtx.Unlock()

	if _, exist := c.workerPool.assignment[workerId]; !exist {
		Logger.Info(fmt.Sprintf("Adding worker %v to the worker pool.....", workerId))
		c.workerPool.assignment[workerId] = make([]TaskId, 0)
		newWorker := &WorkerState{
			workerId: workerId,
			timer:    time.NewTimer(timeout),
		}
		if c.workerPool.list.Front() == nil {
			c.workerPool.list.PushBack(newWorker)
			c.workerPool.nextToSchedule = c.workerPool.list.Front()
		} else {
			c.workerPool.list.PushBack(newWorker)
		}
		Logger.Info(fmt.Sprintf("%s", c))
	} else {
		Logger.Info(fmt.Sprintf("WorkerId %v is already in the worker pool !", workerId))
	}
}

// removeWorker removes the worker if Coordinator receives no heartbeat from it
func (c *Coordinator) removeWorker(workerId WorkerId) {
	// TODO
}

// addNewTask adds a new task to the Coordinator
func (c *Coordinator) addNewTask(task *Task) {
	c.tasks[TaskId(fmt.Sprintf("%v-%s-%p", time.Now().UnixNano(), task.taskType, task))] = task
	c.taskQueue <- task
}

// assignTaskToWorker assigns a new task (either map or reduce) to an available worker
func (c *Coordinator) assignTaskToWorker(task *Task, workerId WorkerId) {
	// TODO
}

// rescheduleTask re-schedules a task processed by crashed worker to an available worker
func (c *Coordinator) rescheduleTask(task *Task) {
	// TODO
}

// HeartBeat implements the heartbeat RPC handler
func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {
	Logger.Info(fmt.Sprintf("Received heartbeat from worker [%s]", args.Id))

	// add the new worker
	c.addWorker(args.Id)

	// TODO make response

	return nil
}

func (c *Coordinator) String() string {
	result := "\n######## Coordinator's state ########\n"
	result += "/* assignment */\n"
	for WorkerId, taskIds := range c.workerPool.assignment {
		result += fmt.Sprintf("{\n\t[WorkerId]: %s\n\t[#Tasks]: %v\n\t[TaskId list]: %v\n}\n", WorkerId, len(taskIds), taskIds)
	}
	result += "\n/* tasks */\n"
	for taskId, tasks := range c.tasks {
		result += fmt.Sprintf("{\n\t[TaskID]: %v\n\t[Task list]: %+v\n}\n", taskId, tasks)
	}
	return result
}
