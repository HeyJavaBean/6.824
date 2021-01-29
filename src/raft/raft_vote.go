package raft

import (
	"strconv"
	"sync/atomic"
)


type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.rpc.Lock()
	defer rf.rpc.Unlock()

	rf.DPrintf(1,string(strconv.Itoa(args.CandidateId)+" ask me to vote him"))
	rf.DPrintf(1,"check1=> args Term:%v >= My Term:%v",args.Term,rf.getCurrentTerm())
	rf.DPrintf(1,"check2=> args Last Index:%v >= My Last Index:%v",args.LastLogIndex,rf.lastLogIndex())
	rf.DPrintf(1,"check3=> args Last Term:%v >= My Last Term:%v",args.LastLogTerm,rf.lastLogTerm())


	success := false
	//如果别人比他低一届，不行
	if args.Term < rf.getCurrentTerm() {
		rf.DPrintf(1,string("args Term:"+strconv.Itoa(args.Term)+" vs My Term:"+strconv.Itoa(rf.getCurrentTerm())))
		//如果他已经投过票了，而且没有给他投票,也叭行 但是如果别人任期比他高就可以投他
	} else if args.Term == rf.getCurrentTerm() && rf.hasVoted()&&!rf.votedFor(args.CandidateId) {
		rf.DPrintf(1,"Has Already vote for %v",rf.voteFor)
		//如果别人记录的东西还没他的早，滚
	} else if args.LastLogIndex < rf.lastLogIndex() {
		rf.DPrintf(1,string("No MOre"))
		//如果别人的记录还没他多，那他leader也不用当了...
	} else if args.LastLogTerm < rf.lastLogTerm(){
		rf.DPrintf(1,"args Last Index:%v vs my Last Index:%v",args.LastLogIndex,rf.lastLogIndex())
		rf.DPrintf(1,"No New")
	} else {
		//变成那人的follower,同时通过term来判断是否可以覆盖vote
		success = rf.followOther(args.CandidateId,args.Term)
	}

	if success{
		rf.DPrintf(1,"vote for %v",args.CandidateId)
	}


	reply.Term = rf.getCurrentTerm()
	reply.VoteGranted = success

}

func (rf *Raft) askVotes() {

	_, lastTerm, lastIndex := rf.getLastLog()

	args := RequestVoteArgs{
		rf.getCurrentTerm(),
		rf.me,
		lastIndex,
		lastTerm,
	}

	//代表自己收到的票数
	var votes int32 = 1

	rf.DPrintf(1,"everybody please vote me!")

	//开协程执行
	for i := 0; i < len(rf.peers); i++ {
		//除了自己，让别人投票
		if i != rf.me {
			go func(idx int) {
				reply := &RequestVoteReply{}
				ret := rf.sendRequestVote(idx, &args, reply)
				if ret {
					if rf.getState() != CANDIDATE {
						rf.DPrintf(1,string(strconv.Itoa(idx)+" vote me but useless"))
						return
					}
					if reply.Term > rf.currentTerm {
						rf.beFollower(reply.Term)
						send(rf.voteCh)
						return
					}
					if reply.VoteGranted {
						atomic.AddInt32(&votes, 1)
					}
					if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
						rf.beLeader()
					}
				}
			}(i)
		}
	}
}


