package raft

import (
	"math/rand"
	"time"
)


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}



func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) startUp() {

	heartbeatTime := time.Duration(150) * time.Millisecond
	for {
		switch rf.state {
		case FOLLOWER, CANDIDATE:
			select {
			case <-rf.voteCh:

			case <-rf.appendLogCh:

			//case <-time.After(time.Duration(rand.Intn(300)+150) * time.Millisecond):
			case <-time.After(time.Duration(rand.Intn(400)+400) * time.Millisecond):
				rf.beCandidate()
			}
		case LEADER:

			rf.doHeartBreak()
			time.Sleep(heartbeatTime)
		}
	}

}


//用于携程之间的通信的一个辅助方法
func send(ch chan bool) {
	select {
	case <-ch:
		//大概是，有状态的话清理掉积累的
	default:

	}
	ch <- true
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	isLeader :=  rf.getState()==LEADER

	if isLeader{
		term,index := rf.appendLog(command)
		rf.DPrintf(2,"Leader Get Command:%v",command)
		return index,term,true
	}

	return index, rf.getCurrentTerm(),false
}

func Min(a,b int) int{
	if a<b{
		return a
	}
	return b
}


func Max(a,b int) int{
	if a>b{
		return a
	}
	return b
}
