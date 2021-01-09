package raft

import (
	"mit6.824/labrpc"
	"sync/atomic"
)

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}


func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = NULL

	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{-1, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0
	//只有master才会去初始化处理
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.appendLogCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.startUp()

	return rf
}

