package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


type RpcArgs struct{
	//返回的时候告诉他我成功完成了什么任务
	Type TaskType
	//返回的是哪个任务完成了
	TaskId string
}

type TaskType string

const (
	Task_Map TaskType = "Task_Map"
	Task_Reduce TaskType = "Task_Reduce"
	Task_Wait TaskType = "Task_Wait"
	Task_Done TaskType = "Task_Done"
	Task_Fail TaskType = "Task_Fail"
)

type RpcReply struct{
	//任务类型
	Type TaskType
	//Map要处理的文件类型 艹要大写才能导出....
	NReduce int

	Task Task

}


type Task struct {
	TaskId     string
	TaskFile   string
	taskStatus TaskStatus
}

type MasterStatus string

const (
	Do_Map    MasterStatus = "Do_Map"
	Do_Reduce MasterStatus = "Do_Reduce"
	Finished  MasterStatus = "Finished"
)

type TaskStatus string

const (
	Task_Ready    TaskStatus = "Task_Ready"
	Task_Running  TaskStatus = "Task_Running"
	Task_Finished TaskStatus = "Task_Finished"
)



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
