package raft

import (
	"strconv"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Entries      []Log
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	ConflictTerm int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) doHeartBreak() {

	for i:=0;i<len(rf.peers);i++{
		if i!=rf.me{
			go func(idx int){
				for {

					if rf.getState()!=LEADER{
						return
					}

					args := AppendEntriesArgs{
						Term:rf.getCurrentTerm(),
						LeaderId: rf.me,
					}


					reply := &AppendEntriesReply{}

					ret := rf.sendAppendEntries(idx,&args,reply)

					if !ret{
						return
					}

				}
			}(i)
		}
	}

}



//心跳接受方，需要同步日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {


	defer send(rf.appendLogCh)


	//这里currentTerm要单独提出来
	currentTerm := rf.getCurrentTerm()
	if args.Term > currentTerm {
		rf.DPrint(string("ticked by "+strconv.Itoa(args.LeaderId)))
		rf.beFollower(args.Term)
	}
	reply.Term = currentTerm

	//不能胜任领导人的情况
	if args.Term < currentTerm {
		return
	}


	//匹配了则进行拼接
	reply.Success = true


}

func (rf *Raft) updateLastApplied(){
	for rf.lastApplied<rf.commitIndex{
		rf.lastApplied++
		curLog:= rf.log[rf.lastApplied]
		msg := ApplyMsg{
			true,
			curLog.Command,
			rf.lastApplied,
		}

		rf.applyCh<-msg
	}
}
