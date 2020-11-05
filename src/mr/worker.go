package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


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
	// send the RPC request, wait for the reply.
	for {
		args := RpcArgs{}
		reply := RpcReply{}
		call("Master.GetTask", &args, &reply)

		if reply.Type==Task_Wait {
			time.Sleep(time.Duration(1)*time.Second)
			continue
		}else if reply.Type==Task_Map{
			mapTask(&reply.Task,reply.NReduce,mapf)
		}else if reply.Type==Task_Reduce{
			reduceTask(reply.Task.TaskId,reply.NReduce,reducef)
		}else {
			break
		}
		//任务完成，反馈情况
		args.Type=reply.Type
		args.TaskId = reply.Task.TaskId
		call("Master.TaskComplete",&args,&reply)
	}

}

type ByKey []KeyValue

// for sorting by key.
//ByKey的一些辅助方法
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


func reduceTask(reduceId string,nMap int,reducef func(string, []string) string) {

	//读取目标文件
	tar:=make([]*os.File,nMap)
	for i := range tar {
		oname := "mr-"+strconv.Itoa(i)+"-"+reduceId
		tar[i], _ = os.Open(oname)
	}

	//现在开始读取
	intermediate := []KeyValue{}

	for _,f := range tar {

		dec := json.NewDecoder(f)
		kva := []KeyValue{}
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))

	tmp, err := ioutil.TempFile("", "reduce-*.tmp")
	if err != nil {
		log.Fatal(err)
		//log.Fatalf("cantnot create tmpFile")
	}
	defer os.Remove(tmp.Name())

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
		fmt.Fprintf(tmp, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	oname := "mr-out-"+reduceId
	os.Rename(tmp.Name(), oname)
	defer tmp.Close()

}


func mapTask(task *Task,nReduce int,mapf func(string, string) []KeyValue) {

	filename := task.TaskFile
	mapId := task.TaskId

	intermediate := []KeyValue{}
	tar, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	//内容读到content里去
	content, err := ioutil.ReadAll(tar)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	tar.Close()

	kva := mapf(filename, string(content))

	intermediate = append(intermediate, kva...)

	//先输出至临时文件最后再重命名，保证在crash的时候文件的拥有合法文件名的内容是正确的

	files := make([]*os.File,nReduce)

	for i:=0;i<nReduce;i++ {
		files[i], err = ioutil.TempFile("", "map-*.tmp")
		if err != nil {
			log.Fatal(err)
			//log.Fatalf("cantnot create tmpFile")
		}
		defer os.Remove(files[i].Name())
	}

	for _,kv := range intermediate {
		output := files[ihash(kv.Key)%nReduce]
		json.NewEncoder(output).Encode(&kv)
	}

	for i := 0; i < nReduce; i++ {
		oname := "mr-"+mapId+"-"+strconv.Itoa(i)
		os.Rename(files[i].Name(), oname)
		defer files[i].Close()
	}

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


