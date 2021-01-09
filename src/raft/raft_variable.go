package raft

import (
	"mit6.824/labrpc"
	"sync"
)

type Log struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	state       State
	currentTerm int
	voteFor     int
	log         []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
	voteCh      chan bool
	appendLogCh chan bool
}


func (rf *Raft) getState() State{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state

}

func (rf *Raft) setState(state State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state

}

func (rf *Raft) getCurrentTerm() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setCurrentTerm(currentTerm int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = currentTerm
}



//界数自增
func (rf *Raft) currentTermAdd(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
}

func (rf *Raft) vote(idx,leaderTerm int) bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if leaderTerm>rf.currentTerm{
		rf.voteFor = idx
		return true
	}
	if rf.voteFor!=NULL{
		return false
	}
	rf.voteFor = idx
	return true
}

//是否已经为这个人投过票了
func (rf *Raft) votedFor(candidateId int) bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.voteFor != NULL && rf.voteFor == candidateId
}

func (rf *Raft) hasVoted() bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.voteFor != NULL
}


func (rf *Raft) getNextIndex(idx int) int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[idx]
}

func (rf *Raft) getCommitIndex() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) setNextIndex(idx,value int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[idx] = value
}




func (rf *Raft) leaderInit(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log)
	}

}


func (rf *Raft) lastLogTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) - 1
	if index < 0 {
		return -1
	}
	return rf.log[index].Term
}

func (rf *Raft) lastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log) - 1
}


func (rf *Raft) getLastLog() (log Log,term int,index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.log) - 1

	if index < 0 {
		return Log{},-1,index
	}

	return rf.log[index],rf.log[index].Term,index

}
