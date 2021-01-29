package kvraft

import (
	"bytes"
	"mit6.824/labgob"
)

func (kv *KVServer) readSnapshot(snapshot []byte) {

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
		kv.seqMap = seqMap
		kv.db.Load(db)
	}
}

func (kv *KVServer) doSnapshot(index int){
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//todo 注意这个lock
	kv.mu.Lock()
	e.Encode(kv.db.Save())
	e.Encode(kv.seqMap)
	kv.mu.Unlock()
	kv.rf.DoSnapShot(index,w.Bytes())
}


func (kv *KVServer) needSnapshot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	thre := 10

	return kv.maxraftstate>0 && kv.maxraftstate - kv.persister.RaftStateSize() < kv.maxraftstate/thre

}