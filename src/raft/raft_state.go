package raft

type State string

const (
	//RAFT的三个状态
	FOLLOWER  State = "Follower"
	LEADER    State = "Leader"
	CANDIDATE       = "Candidate"
	//Candidate Id 默认没有投票的空的情况
	NULL = -1
)


func (rf *Raft) beCandidate() {


	rf.DPrintf(1,"from %v to be Candidate and Term is %v",rf.state,rf.getCurrentTerm()+1)

	rf.setState(CANDIDATE)
	rf.currentTermAdd()
	rf.vote(rf.me,rf.getCurrentTerm()+1)

	go rf.askVotes()
}


func (rf *Raft) beFollower(term int) {



	if rf.isFollower(){
		return
	}

	rf.DPrintf(1,"from %v force to be Follower",rf.state)

	rf.setState(FOLLOWER)
	rf.vote(NULL,rf.getCurrentTerm())
	rf.setCurrentTerm(term)

}


func (rf *Raft) followOther(leaderId,leaderTerm int) bool{

	if rf.vote(leaderId,leaderTerm){
		rf.DPrintf(1,"from %v agree to be Follower",rf.state)
		rf.setState(FOLLOWER)
		rf.setCurrentTerm(leaderTerm)
		send(rf.voteCh)
		return true
	}

	return false

}

func (rf *Raft) beLeader() {

	//emm非要合适下是不是CANDIDATE，因为可能竞争先变成Follower的情况
	if rf.getState() != CANDIDATE {
		return
	}

	rf.DPrintf(2,"from %v to be Leader",rf.state)

	rf.setState(LEADER)
	//(Reinitialized after election)
	rf.leaderInit()
	//确保自己不要卡在candidate 的 select那里 马上把心跳包发出去
	send(rf.voteCh)
}

