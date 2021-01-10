package raft

import (
	"bytes"
	"mit6.824/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.voteFor)
		e.Encode(rf.log)
		data := w.Bytes()
		rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var log []Log

	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil{
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
	}

}

