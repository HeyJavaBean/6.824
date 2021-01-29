package raft

import (
	"bytes"
	"mit6.824/labgob"
)

func (rf *Raft) DoSnapShot(curIdx int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if curIdx<=rf.lastIncludedIndex{
		return
	}

	newLog:= make([]Log,0)
	newLog = append(newLog,rf.log[curIdx-rf.lastIncludedIndex:]...)

	rf.lastIncludedTerm = rf.log[curIdx-rf.lastIncludedIndex].Term
	rf.lastIncludedIndex = curIdx
	rf.log = newLog

	rf.persister.SaveStateAndSnapshot(rf.EncodingState(),snapshot)

}

func (rf *Raft) EncodingState() []byte{

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)


	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

func (rf *Raft) InstallSnapshot(args* InstallSnapshotArgs, reply* InstallSnapshotReply){

	rf.rpc.Lock()
	defer rf.rpc.Unlock()

	if args.Term < rf.currentTerm{
		return
	}

	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	send(rf.appendLogCh)

	if args.LastIncludedIndex <= rf.lastIncludedIndex{
		return
	}

	applyMsg := ApplyMsg{UseSnapShot: true,SnapShot: args.Data}

	rf.log = []Log{{args.LastIncludedTerm,nil}}

	if args.LastIncludedIndex< len(rf.log)-1{
		rf.log = append(make([]Log,0),rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
	}

	

}

type InstallSnapshotArgs struct{
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct{
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int,args* InstallSnapshotArgs, reply* InstallSnapshotReply) bool{
	ok := rf.peers[server].Call("Raft.InstallSnapshot",args,reply)
	return ok
}