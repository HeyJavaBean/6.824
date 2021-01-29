package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Entries      []Log
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictTerm int
	ConflictIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) doHeartBreak() {

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(idx int) {
				for {

					if !rf.isLeader(){
						return
					}

					//主要是currentTerm和commitIndex后续会变
					args := rf.getAppendEntriesArgs(idx)
					reply := &AppendEntriesReply{}

					rf.DPrintf(2, "Server Log: %v",rf.getLog())
					rf.DPrintf(2, "Leader Tick to %v with %v", idx,args.Entries)

					ret := rf.sendAppendEntries(idx, args, reply)

					//如果超时了,过会儿重新发送
					if !ret {
						//rf.DPrintf(2, "No.%v is not online", idx)
						return
					}

					//todo 这里之前是用的上面获取的currentTerm但是我想了想似乎觉得好像没有影响?
					//如果对方版本比你之前的报文高,那么必然是不成功的
					if reply.Term > args.Term {
						//如果对方的版本比你高,需要变成follower
						//这里发报文的这段时间currentTerm是有可能增加的
						if reply.Term>rf.getCurrentTerm(){

							//考虑并发?
							rf.DPrintf(2, "from %v force to be Follower cuz Other is High",rf.getState())

							lastIdx := Max(reply.ConflictIndex,rf.getCommitIndex())
							lastIdx = Min(lastIdx,rf.lastLogIndex())
							rf.setLog(rf.getLog()[:lastIdx+1])

							rf.DPrintf(2, "now log shorten to %v",rf.getLog())

							rf.beFollower(reply.Term)
						}
						return
					}


					//如果成功,则修改信息就ok了
					if reply.Success {
						//修改next index 和 match index
						rf.logMatch(idx, args.PrevLogIndex+len(args.Entries))
						//更新commit idx,需要用到commit idx和log,如果发生变化了这里的更新应该作废?
						rf.updateCommitIndexLeader()
						//根据commit idx 做提交 同时更新applied idx
						rf.updateLastApplied()
						//rf.DPrintf(2, "tick %v successfully",args.LeaderId)
						return
					} else {
						//如果失败了,需要快速回退到特定版本
						rf.DPrintf(2, "update denied for %v and his pos is term %v",idx,reply.ConflictTerm)
						rf.nextIdxRollBack(idx, reply.ConflictTerm, reply.ConflictIndex)
					}
				}
			}(i)
		}
	}

}

//心跳接受方，需要同步日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.rpc.Lock()
	defer rf.rpc.Unlock()


	rf.DPrintf(2, "ticked by %v",args.LeaderId)
	//这里currentTerm要单独提出来
	currentTerm := rf.getCurrentTerm()
	if args.Term >= currentTerm {
		//rf.DPrintf(1, "ticked by %v",args.LeaderId)
		rf.beFollower(args.Term)
	}
	reply.Term = currentTerm

	//对方不能胜任领导人的情况
	if args.Term < currentTerm {
		rf.DPrintf(2, "%v cant be leader cuz his term is %v while mine is %v",args.LeaderId,args.Term,currentTerm)
		//todo 这里是有人发现过气leader之后,让他去删除多的内容?
		//#2: Server Log: [{-1 <nil>} {1 101} {1 102} {1 103} {1 104} {1 104}]          term:1
		//#0: Server Log: [{-1 <nil>} {1 101} {2 103}]                                  term:2
		//
		//#1: Server Log: [{-1 <nil>} {1 101} {2 103}]         mleader                  term:2

		reply.ConflictIndex = rf.getCommitIndex()
		return
	}

	//超出了长度，必然不能同步
	if args.PrevLogIndex >= rf.getLogLen() {
		rf.DPrintf(2, "Sync Log out of lenth while my log len is %v and last term is %v",rf.getLogLen(),rf.lastLogTerm())
		reply.ConflictTerm = rf.lastLogTerm()
		reply.ConflictIndex = rf.lastLogIndex()
		return
	}

	log := rf.getLog()

	//如果日志条目不匹配
	if log[args.PrevLogIndex].Term != args.PrevLogTerm {
		//让leader的日志可以快速回滚
		rf.DPrintf(2, "Log Not Match At Index %v while his term is %v and mine is %v",args.PrevLogIndex,args.PrevLogTerm,log[args.PrevLogIndex].Term)
		reply.ConflictTerm = log[args.PrevLogIndex].Term
		return
	}

	//匹配了则进行日志拼接
	reply.Success = true

	if len(args.Entries)==0 && args.PrevLogIndex<rf.getLogLen()-1{
		rf.DPrintf(2, "Log Cut to %v",log[:args.PrevLogIndex+1])
		newLog := []Log{}
		newLog = append(newLog, log[:args.PrevLogIndex+1]...)
		rf.setLog(newLog)
	}

	//添加目录
	//这里似乎忽略了如果是我的长,leader的短,虽然他没有发东西但是需要清楚信息的情况?
	if len(args.Entries) > 0 {
		rf.DPrintf(2, "Log Attached %v + %v",log[:args.PrevLogIndex+1],args.Entries)
		newLog := []Log{}
		newLog = append(newLog, log[:args.PrevLogIndex+1]...)
		newLog = append(newLog, args.Entries...)
		rf.setLog(newLog)
	}

	//修改提交
	if args.LeaderCommit > rf.getCommitIndex() {
		//commit变成可以提交的那个index
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		rf.updateCommitIndexFollower(args.LeaderCommit)
		rf.updateLastApplied()
	}



	if reply.Success{
		send(rf.appendLogCh)
	}


}


















