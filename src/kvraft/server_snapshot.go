package kvraft

import (
	"bytes"
	"mit6.824/labgob"
)


//开机->KV加载snapshot->连带raft启动->raft自加载快照
//下层接受到新的快照->raft自处理->apply msg上传->kv加载
//leader kv发起压缩->kv 压缩->通知下层raft压缩

func (kv *KVServer) loadSnapshot(snapshot []byte) {

	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	//
	if snapshot==nil || len(snapshot)<1{
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var seqMap map[int]int

	if d.Decode(&db) != nil ||
		d.Decode(&seqMap) != nil{

	} else {
		kv.db.Load(db)
		kv.seqMap = seqMap
	}
}

func (kv *KVServer) doSnapshot(index int){
	//
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	//
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db.Save())
	e.Encode(kv.seqMap)

	//上层记录数据变化后后给raft,让raft压缩日志
	kv.rf.DoSnapShot(index,w.Bytes())
}


func (kv *KVServer) needSnapshot() bool {

	//todo 这里暂时还不太get到他的阈值算法
	return kv.maxraftstate>0 &&
		kv.maxraftstate - kv.persister.RaftStateSize() < kv.maxraftstate/kv.threshold

}