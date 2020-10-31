package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
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
			//todo ?
			time.Sleep(time.Duration(1)*time.Second)
			continue
		}else if reply.Type==Task_Map{
			mapTask(reply.Task.TaskFile,reply.NReduce,reply.Task.TaskId,mapf)

		}else if reply.Type==Task_Reduce{

			reduceTask(reply.Task.TaskId,reducef)

		}else {
			break
		}

		//任务完成，反馈情况
		arg := RpcArgs{}
		arg.Type=reply.Type
		arg.TaskId = reply.Task.TaskId
		call("Master.TaskComplete",&arg,&reply)
	}

}

type ByKey []KeyValue

// for sorting by key.
//ByKey的一些辅助方法
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


func reduceTask(reduceId string,reducef func(string, []string) string) {

	//可变数组？？？
	tar:=[]*os.File{}
	dirname := "."
	files, _ := ioutil.ReadDir(dirname)
	for _,f := range files {
		name := f.Name()
		if strings.HasPrefix(name,"mr-") {
			len:=len(name)
			s := name[len-1 : len]
			if strings.EqualFold(s,reduceId) {
				target, _ := os.Open(dirname + "/" + f.Name())
				defer target.Close()
				tar = append(tar, target)
			}
		}
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

	//创建输出文件
	oname := "mr-out-"+reduceId
	ofile, _ := os.Create(oname)
	defer ofile.Close()
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
}


func mapTask(filename string,nReduce int,mapId string,mapf func(string, string) []KeyValue) {


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
	//todo 还没做


	files := make([]*os.File,nReduce)
	encs := make([]*json.Encoder,nReduce)

	for i:=0;i<nReduce;i++ {
		oname := "mr-"+mapId+"-"+strconv.Itoa(i)
		f, _ := os.Create(oname)
		defer f.Close()
		files[i] = f
		encs[i] = json.NewEncoder(f)
	}

	for _,kv := range intermediate {
		y:=ihash(kv.Key)%nReduce
		encs[y].Encode(&kv)
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


