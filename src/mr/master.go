package mr

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	mapTasks     []Task
	reduceTasks  []Task
	masterStatus MasterStatus
}

func getTaskSafely(tasks *[]Task) *Task {

	var task *Task
	for id := range *tasks {
		//获取最新的值
		taskStatusPtr := &(*tasks)[id].taskStatus
		//如果是Ready状态,尝试抢占并修改为Running
		//至于外层循环嘛，我这里暂时没考虑，因为反正在worker那里也会有wait机制来做二次寻找，这里就不cas了
		if atomic.CompareAndSwapInt32(taskStatusPtr, Task_Ready, Task_Running) {
			task = &(*tasks)[id]
			go func(task *Task) {
				time.Sleep(time.Duration(10) * time.Second)
				atomic.CompareAndSwapInt32(taskStatusPtr, Task_Running, Task_Ready)
			}(task)
			break
		}
	}
	return task
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
	//其实是可以用下标找到的，但是这里就算是每次有人提交了就去检测全局，更新状态
	for id, t := range *tasks {
		taskStatusPtr := &(*tasks)[id].taskStatus
		if t.TaskId == args.TaskId {
			//这里如果不是变成Finished则是因为超时变成Ready
			//那么，为了写代码简单，还是算他踩线过吧，所以就直接sotre了
			if atomic.CompareAndSwapInt32(taskStatusPtr,Task_Running,Task_Finished){
				reply.Type = Task_Done
			} else {
				reply.Type = Task_Fail
			}
		}

		if atomic.LoadInt32(taskStatusPtr) != Task_Finished {
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
	return nil
}

func (m *Master) GetTask(args *RpcArgs, reply *RpcReply) error {
	status := m.masterStatus
	if status == Finished {
		return nil
	}

	var task *Task = nil
	if status == Do_Map {
		task = getTaskSafely(&m.mapTasks)
		reply.Type = Task_Map
		reply.NReduce = len(m.reduceTasks)
	}
	if status == Do_Reduce {
		task = getTaskSafely(&m.reduceTasks)
		reply.Type = Task_Reduce
		reply.NReduce = len(m.mapTasks)
	}
	if task == nil {
		reply.Type = Task_Wait
	} else {
		reply.Task = *task
	}
	return nil
}

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

func (m *Master) Done() bool {
	return m.masterStatus == Finished
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	//记录好信息
	m.masterStatus = Do_Map
	m.mapTasks = make([]Task, len(files))
	//一个微妙的关系：taskId正好是和数组的下标是对应的
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
