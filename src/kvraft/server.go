package kvraft

import (
	"mit6.824/labgob"
	"mit6.824/labrpc"
	"mit6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	OpType   string
	Key      string
	Value    string
	ClientId int
	Seq      int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {

	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	mu      sync.Mutex
	// Your definitions here.
	db    Database
	chMap map[int]chan Op

	seqMap map[int]int

	threshold int

	persister *raft.Persister

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{"Get", args.Key, "",args.ClientId,args.Seq}

	//todo 感觉这里可以删掉
	if !kv.rf.IsLeader(){
		return
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	kv.DPrintf(1, "Request To Get Key %v with cid:%v/%v", args.Key,args.ClientId,args.Seq)

	if !kv.beNotified(index,op){
		return
	}

	reply.IsLeader = true
	reply.Value = kv.db.Get(op.Key)

	kv.DPrintf(1, "Successfully Get Key %v and Value is %v", args.Key,reply.Value)

}

func (kv *KVServer) beNotified(index int,op Op) bool{

	//等待同步完成
	ch := kv.indexChan(index)

	//检查同步结果
	select {
		case nop:=<-ch:
			if nop.Equal(op){
				return true
			}
			kv.DPrintf(1, "Execute Changed!")
			return false
		case <-time.After(time.Second*2):
			kv.DPrintf(1, "Time Out!")
			return false
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := Op{args.Op, args.Key, args.Value,args.ClientId,args.Seq}

	if !kv.rf.IsLeader(){
		return
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return
	}

	kv.DPrintf(1, "Request To %v Key %v and Value |%v|  with cid:%v/%v", args.Op, args.Key, args.Value,args.ClientId,args.Seq)

	if !kv.beNotified(index,op){
		return
	}

	reply.IsLeader = true

	kv.DPrintf(1, "Successfully %v Key %v",op.OpType, args.Key)

}

func (kv *KVServer) indexChan(idx int) chan Op {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.chMap[idx]; !ok {
		kv.chMap[idx] = make(chan Op, 1)
	}
	return kv.chMap[idx]
}

func (op *Op) Equal(op2 Op) bool{
	return op.Key==op2.Key && op.Value==op2.Value &&
		op.OpType==op2.OpType &&
		op.ClientId==op2.ClientId && op.Seq==op2.Seq
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = Database{db:make(map[string]string)}
	kv.chMap = make(map[int]chan Op)
	kv.seqMap = make(map[int]int)

	kv.threshold = 10

	//加载快照内容
	kv.loadSnapshot(kv.persister.ReadSnapshot())

	go func() {
		for msg := range kv.applyCh {

			//如果从leader接受到了快照，需要应用
			if msg.UseSnapShot{
				kv.mu.Lock()
				kv.loadSnapshot(msg.SnapShot)
				kv.mu.Unlock()
				continue
			}

			//正常情况的指令则是用状态机执行
			op := msg.Command.(Op)

			//fixme 感觉这个地方好像不需要加锁啊？？？
			//kv.mu.Lock()

			if kv.seqMap[op.ClientId]<op.Seq{
				//应用到状态机
				kv.db.Apply(op)
				//给seq做记录
				kv.seqMap[op.ClientId] = op.Seq
				//日志打印
				kv.DPrintf(1, "For Key %v, Do %v, now Value is |%v|", op.Key,op.OpType, kv.db.Get(op.Key))
			}else{
				kv.DPrintf(1, "Unexpect Repeat Commit!")
			}

			//kv.mu.Unlock()

			//通知执行完成，响应client
			kv.indexChan(msg.CommandIndex)<-op

			//检查是否需要做快照记录
			if kv.needSnapshot(){
				kv.doSnapshot(msg.CommandIndex)
			}

		}
	}()

	return kv
}
