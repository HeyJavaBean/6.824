package kvraft

import (
	"mit6.824/labrpc"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	clientId int
	seqGenerator SeqGenerator
}

type SeqGenerator struct{
	seqNum int32
}

//用cas生成序列号,好像可以用atmoic的方法来优化
func (sg *SeqGenerator) getSeq() int{

	seq := atomic.LoadInt32(&sg.seqNum)

	for !atomic.CompareAndSwapInt32(&sg.seqNum,seq,seq+1){
		seq = atomic.LoadInt32(&sg.seqNum)
	}

	return int(seq)

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}


var idxer int = 0
var locker sync.Mutex

func getId() int{
	locker.Lock()
	defer locker.Unlock()
	idxer++
	return idxer
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.seqGenerator = SeqGenerator{seqNum: 1}
	ck.clientId = getId()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	index := ck.lastLeader

	args := GetArgs{ck.clientId,ck.seqGenerator.getSeq(),key}
	reply := GetReply{}

	ck.DPrintf(1,"Request To Get Key %v with cid:%v/%v",key,args.ClientId,args.Seq)
	for  {
		ok := ck.servers[index].Call("KVServer.Get",&args,&reply)
		if !ok{
			ck.DPrintf(1,"Request Out Of Time To Call Server.%v",index)
		}
		if ok && reply.IsLeader {
			ck.lastLeader = index
			return reply.Value
		}
		index = (index +1)% len(ck.servers)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	index := ck.lastLeader

	args := PutAppendArgs{ck.clientId,ck.seqGenerator.getSeq(),key,value,op}
	reply := PutAppendReply{}

	ck.DPrintf(1,"Request To %v Key %v and Value |%v| with cid:%v/%v",op,key,value,args.ClientId,args.Seq)
	for  {
		ok := ck.servers[index].Call("KVServer.PutAppend",&args,&reply)
		if ok && reply.IsLeader {
			ck.lastLeader = index
			return
		}
		index = (index +1)% len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
