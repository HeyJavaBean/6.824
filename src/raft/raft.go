package raft

import (
	"math/rand"
	"strconv"
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

			case <-time.After(time.Duration(rand.Intn(300)+150) * time.Millisecond):
				rf.beCandidate()
			}
		case LEADER:

			rf.doHeartBreak()
			time.Sleep(heartbeatTime)
		}
	}

}

func (rf *Raft) DPrint(str string){
	str = "#"+strconv.Itoa(rf.me)+": "+str
	DPrintf(str)
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