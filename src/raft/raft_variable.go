package raft

import (
	"mit6.824/labrpc"
	"sort"
	"sync"
)

type Log struct {
	Term    int
	Command interface{}
}

type Raft struct {
	rpc          sync.Mutex          // Lock to protect shared access to this peer's state
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

func (rf *Raft) isLeader() bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state==LEADER
}

func (rf *Raft) isFollower() bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state==FOLLOWER
}

func (rf *Raft) isCandidate() bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state==CANDIDATE
}

func (rf *Raft) setState(state State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.state = state
}

func (rf *Raft) getCurrentTerm() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getLeaderCommitIdx() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) getLog() []Log{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

func (rf *Raft) setLog(logs []Log) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.log = logs
}

func (rf *Raft) appendLog(command interface{}) (index,term int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.log = append(rf.log, Log{
		rf.currentTerm,command,
	})
	return rf.currentTerm,len(rf.log)-1
}

func (rf *Raft) getLogLen() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

//日志匹配后对next和match做修改
func (rf *Raft) logMatch(idx,matchIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[idx] = matchIdx
	rf.nextIndex[idx] = rf.matchIndex[idx] + 1
}


func (rf *Raft) setCurrentTerm(currentTerm int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.currentTerm = currentTerm
}



//界数自增
func (rf *Raft) currentTermAdd(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.currentTerm++
}



func (rf *Raft) vote(idx,leaderTerm int) bool{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
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

func (rf *Raft) getAppendEntriesArgs(idx int) *AppendEntriesArgs{
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nextIndex := rf.nextIndex[idx]
	log := rf.log
	entry := append([]Log{}, log[nextIndex:]...)

	prevLogIndex := rf.nextIndex[idx] - 1
	prevLogTerm := log[prevLogIndex].Term

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		Entries:      entry,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}

	return &args

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

func (rf *Raft) updateLastApplied() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		curLog := rf.log[rf.lastApplied]
		msg := ApplyMsg{
			true,
			curLog.Command,
			rf.lastApplied,
		}
		rf.applyCh <- msg
	}
	rf.persist()
}

func (rf *Raft) updateCommitIndexLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.matchIndex[rf.me] = len(rf.log)-1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
	}
	rf.persist()
}

func (rf *Raft) updateCommitIndexFollower(leaderCommit int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.commitIndex = Min(leaderCommit,len(rf.log)-1)

	rf.persist()
}

func (rf *Raft) nextIdxRollBack(idx,conflictTerm,conflictIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if conflictIndex!=0{
		rf.nextIndex[idx] = conflictIndex+1
	}else{
		rf.nextIndex[idx]--
		for rf.log[rf.nextIndex[idx]].Term > conflictTerm {
			rf.nextIndex[idx]--
		}

		//边界处理 todo 这个我看下可以怎么优化
		if rf.nextIndex[idx] <= 0 {
			rf.nextIndex[idx] = 1
		}
	}


	rf.DPrintf(3, "Now leader for %v roll back to %v and term is %v",idx,rf.nextIndex[idx],rf.log[rf.nextIndex[idx]].Term)

}