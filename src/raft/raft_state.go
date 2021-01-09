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

	rf.DPrint(string("from " + rf.state + " to be Candidate"))

	rf.setState(CANDIDATE)
	rf.currentTermAdd()
	rf.vote(rf.me,rf.getCurrentTerm())
	rf.persist()

	go rf.askVotes()
}


func (rf *Raft) beFollower(term int) {


	rf.DPrint(string("from " + rf.state + " force to be Follower"))

	rf.setState(FOLLOWER)
	rf.vote(NULL,rf.getCurrentTerm())
	rf.setCurrentTerm(term)
	rf.persist()
	send(rf.voteCh)

}


func (rf *Raft) followOther(leaderId,leaderTerm int) bool{

	if rf.vote(leaderId,leaderTerm){
		rf.DPrint(string("from " + rf.state + " agree to be Follower"))
		rf.setState(FOLLOWER)
		rf.setCurrentTerm(leaderTerm)
		rf.persist()
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

	rf.DPrint(string("from " + rf.state + " to be Leader"))


	rf.setState(LEADER)
	//(Reinitialized after election)
	rf.leaderInit()
	rf.persist()
	//确保自己不要卡在candidate 的 select那里 马上把心跳包发出去
	send(rf.voteCh)
}

