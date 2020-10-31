package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	mapTasks    []Task
	reduceTasks []Task
	nReduce int
	lock       sync.Mutex
	masterStatus MasterStatus
}

func getTaskSafely(lock *sync.Mutex, tasks *[]Task) *Task {
	lock.Lock()
	var task *Task
	for id, t := range *tasks {
		if t.taskStatus == Task_Ready {
			task = &(*tasks)[id]
			task.taskStatus = Task_Running
			go checkout(task)
			break
		}
	}
	lock.Unlock()
	return task
}

//不敢保证并发安全？暂时不确定
func checkout(task *Task) {
	time.Sleep(time.Duration(10) * time.Second)
	if task.taskStatus == Task_Running {
		task.taskStatus = Task_Ready
	}
}

//记录任务完成了
func (m *Master) TaskComplete(args *RpcArgs, reply *RpcReply) error {

	var tasks *[]Task
	if args.Type == Task_Map {
		tasks = &m.mapTasks
	} else {
		tasks = &m.reduceTasks
	}

	done := true
	m.lock.Lock()
	for id, t := range *tasks {
		if t.TaskId == args.TaskId {
			if t.taskStatus == Task_Running {
				(*tasks)[id].taskStatus = Task_Finished
				reply.Type = Task_Done
			} else {
				reply.Type = Task_Fail
			}
		}
		if (*tasks)[id].taskStatus != Task_Finished {
			done = false
		}
	}

	if done {
		if args.Type == Task_Map {
			m.masterStatus = Do_Reduce
		} else {
			m.masterStatus = Finished
		}
	}
	m.lock.Unlock()
	return nil
}

//分配Map或者Reduce任务，维护状态
func (m *Master) GetTask(args *RpcArgs, reply *RpcReply) error {
	status := m.masterStatus
	if status == Finished {
		return nil
	}

	var task *Task = nil
	if status == Do_Map {
		task = getTaskSafely(&m.lock, &m.mapTasks)
		reply.Type = Task_Map
		reply.NReduce = m.nReduce
	}
	if status == Do_Reduce {
		task = getTaskSafely(&m.lock, &m.reduceTasks)
		reply.Type = Task_Reduce
	}
	if task == nil {
		reply.Type = Task_Wait
	}else{
		reply.Task = *task
	}
	return nil
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
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.masterStatus == Finished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	//记录好信息
	m.nReduce = nReduce
	m.masterStatus = Do_Map
	m.mapTasks = make([]Task, len(files))
	for id, f := range files {
		m.mapTasks[id] = Task{strconv.Itoa(id), f, Task_Ready}
	}
	m.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = Task{strconv.Itoa(i), "", Task_Ready}
	}
	m.server()
	return &m
}
