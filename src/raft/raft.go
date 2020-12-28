package raft

import (
	"bytes"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State string

const (
	//RAFT的三个状态
	FOLLOWER  State = "Follower"
	LEADER    State = "Leader"
	CANDIDATE       = "Candidate"
	//Candidate Id 默认没有投票的空的情况
	NULL = -1
)

//会收到的指令，指令会去给状态机，然后还有收到的term
type Log struct {
	Term int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state State
	currentTerm int
	voteFor     int
	log         []Log
	commitIndex int
	lastApplied int
	nextIndex  []int
	matchIndex []int
	applyCh chan ApplyMsg
	voteCh chan bool
	appendLogCh chan bool
}

func (a *AppendEntriesArgs) String() string{
	es := "=>"
	for _, e := range a.Entries {
		s := strconv.Itoa(e.Term)
		es  = es + s + ","
	}
	str :="=>Term:"+strconv.Itoa(a.Term)+";LC:"+strconv.Itoa(a.LeaderCommit)+";PreTerm:"+strconv.Itoa(a.PrevLogTerm)+
		";PreIndex:"+strconv.Itoa(a.PrevLogIndex)+"\n"+es
	return str
}

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
}

func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)



}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voteFor int
	var log []Log

	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil{
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = log
		rf.mu.Unlock()
	}
}


type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}


//在这个场景里是labrpc的模拟网络场景给调用的，给别人投票的
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()

	currentTerm := rf.currentTerm
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()
	voteFor := rf.voteFor

	rf.mu.Unlock()

	success := false
	//如果别人比他低一届，不行
	if args.Term < currentTerm {
		//如果他已经投过票了，也叭行
	} else if voteFor != NULL && voteFor != args.CandidateId{
		//如果别人记录的东西还没他的早，滚
	} else if args.LastLogTerm < lastLogTerm{
		//如果别人的记录还没他多，那他leader也不用当了...
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {

	} else{
		//变成那人的follower

		rf.mu.Lock()

		rf.voteFor = args.CandidateId
		rf.state = FOLLOWER
		rf.persist()

		rf.mu.Unlock()

		success = true
		send(rf.voteCh)
		//fmt.Println("#",rf.me," vote ",args.CandidateId)

	}

	reply.Term = currentTerm
	reply.VoteGranted = success

}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//心跳接受方，需要同步日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//

	rf.mu.Lock()

	currentTerm := rf.currentTerm
	log := rf.log
	commitIndex := rf.commitIndex

	rf.mu.Unlock()

	defer send(rf.appendLogCh)

	if args.Term >= currentTerm {
		rf.beFollower(args.Term)
	}
	reply.Term = currentTerm

	if args.Term < currentTerm {

		//fmt.Println("==================================")
		//fmt.Println(args.Term)
		//fmt.Println(rf.me," Reply To ",args.LeaderId,"  Term: ",reply.Term,"  Success:",reply.Success)
		//fmt.Println(rf.me," info=>"," commitIdx:",rf.commitIndex," last Applied:",rf.lastApplied," Current Term:",rf.currentTerm)
		//fmt.Println("Log:",rf.log)
		//fmt.Println("==================================")
		//fmt.Println("#",rf.me," ticked by ",args.LeaderId," and",reply.Success)

		return
	}

	//超出了长度，必然不能同步
	if args.PrevLogIndex >= len(log) || log[args.PrevLogIndex].Term != args.PrevLogTerm {

		//fmt.Println("==================================")
		//fmt.Println(rf.me, " Reply To ", args.LeaderId, "  Term: ", reply.Term, "  Success:", reply.Success)
		//fmt.Println(rf.me, " info=>", " commitIdx:", rf.commitIndex, " last Applied:", rf.lastApplied, " Current Term:", rf.currentTerm)
		//fmt.Println("Log:", rf.log)
		//fmt.Println("==================================")
		//fmt.Println("#",rf.me," ticked by ",args.LeaderId," and",reply.Success)

		return
	}

	//匹配了则进行拼接
	reply.Success = true

	//添加目录
	if len(args.Entries)>0{

		newLog := []Log{}
		newLog = append(newLog,log[:args.PrevLogIndex+1]...)
		newLog = append(newLog,args.Entries...)

		rf.mu.Lock()

		rf.log = newLog
		rf.persist()

		rf.mu.Unlock()
	}

	//修改提交
	if args.LeaderCommit > commitIndex{
		//commit变成可以提交的那个index
		rf.mu.Lock()

		rf.commitIndex = Min(args.LeaderCommit,len(rf.log)-1)
		rf.updateLastApplied()

		rf.mu.Unlock()
	}

	//
	//fmt.Println("==================================")
	//fmt.Println(rf.me," Reply To ",args.LeaderId,"  Term: ",reply.Term,"  Success:",reply.Success)
	//fmt.Println(rf.me," info=>"," commitIdx:",rf.commitIndex," last Applied:",rf.lastApplied," Current Term:",rf.currentTerm)
	//fmt.Println("Log:",rf.log)
	//fmt.Println("==================================")
	//

	//fmt.Println("#",rf.me," ticked by ",args.LeaderId," and",reply.Success)

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

func Min(a,b int) int{
	if a<b{
		return a
	}
	return b
}

//搞快发心跳包，不然follower们要造反了
func (rf *Raft) startAppendLog() {

	for i:=0;i<len(rf.peers);i++{
		if i!=rf.me{
			go func(idx int){
				//一直需要去同步核对节点?

				for {
					//这里他发送的时候又去检测了一下本机是不是leader
					//这种边界检查真的很多很多....

					//fmt.Println("#",rf.me," gonna tick ",idx)

					rf.mu.Lock()
					state := rf.state
					nextIndex := rf.nextIndex[idx]
					logLen := len(rf.log)
					prevLogIndex := rf.nextIndex[idx]-1
					prevLogTerm := rf.log[prevLogIndex].Term
					currentTerm := rf.currentTerm
					entry := append([]Log{},rf.log[nextIndex:]...)
					commitIndex := rf.commitIndex
					rf.mu.Unlock()

					if state!=LEADER{
						return
					}


					if nextIndex>logLen{
						nextIndex = logLen
					}



					////fmt.Println("=!=!=prevLog Indx:",prevLogIndex," +=>offset:",offset)



					////fmt.Println("Master Next Idx for ",idx, " is ",rf.nextIndex[idx] )

					args := AppendEntriesArgs{
						currentTerm,
						rf.me,

						entry,
						commitIndex,

						prevLogIndex,
						prevLogTerm,
					}


					//
					//fmt.Println("==================================")
					//fmt.Println(rf.me," Send To ",idx,"\n",args.String())
					//fmt.Println(args.Entries)
					//fmt.Println("Server:",rf.log)
					//fmt.Println("==================================")
					//

					reply := &AppendEntriesReply{}

					ret := rf.sendAppendEntries(idx,&args,reply)



					//
					//0       1       2       3       4       5       6
					//Master
					//
					//[{-1 nil} {1 101} {1 102} {1 103} {1 104} {1 105} {1 106}]
					//
					//Follower 1
					//
					//[{-1 nil} {1 101} {1 102} {1 103} {1 104} {1 105} {1 106}]
					//
					//Follower 2
					//
					//[{-1 nil} {1 101}]


					if !ret{
						return
					}


					if reply.Term>currentTerm{
						//rf.mu.Lock()
						//fmt.Println("#",rf.me," now i have to follow them")
						rf.beFollower(reply.Term)
						//rf.mu.Unlock()
						return
					}

					if reply.Success{

						rf.mu.Lock()

						rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[idx] = rf.matchIndex[idx] + 1
						rf.updateCommitIndex()
						rf.updateLastApplied()

						rf.mu.Unlock()

						return
					}else{

						failTerm := args.PrevLogTerm

						//fmt.Println("failed:\n",args,"\n",reply," ",reply.Term," vs ",rf.currentTerm)

						rf.mu.Lock()

						rf.nextIndex[idx]--
						for rf.log[rf.nextIndex[idx]].Term==failTerm{
							rf.nextIndex[idx]--
						}
						//边界处理
						if rf.nextIndex[idx]<=0{
							rf.nextIndex[idx]=1
						}

						rf.mu.Unlock()

					}


				}
			}(i)


		}
	}

}

//检查是否大部分节点收到了信息，如果是的话，则做一次提交
func (rf *Raft) updateCommitIndex(){
	rf.matchIndex[rf.me] = len(rf.log)
	copyMatchIndex:=make([]int,len(rf.matchIndex))
	copy(copyMatchIndex,rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N:=copyMatchIndex[len(copyMatchIndex)/2]
	if N >rf.commitIndex && rf.log[N].Term ==rf.currentTerm{
		rf.commitIndex = N
	}
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//这里就是Candidate把请求发出去了的过程
	//之所以要用labrpc是因为如果你直接本地调用就不能模拟丢包的情况了
	//所以他帮你封装了一下帮你模拟了
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader :=  rf.state==LEADER

	if isLeader{


		index = rf.getLastLogIndex()+1


		//fmt.Println("==================================")
		//fmt.Println(rf.me,"reviced command=>",command)
		//fmt.Println("==================================")


		//todo
		rf.log = append(rf.log, Log{
			rf.currentTerm,command,
		})

		rf.persist()
	}

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//Make之后，创建了一个节点，节点一开始是follower状态，等随机计时到了之后就进入后续选举过程了
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//首先初始化，创建一个Raft节点
	rf := &Raft{}
	//设置peers
	rf.peers = peers
	//做持久化的东西，先不管
	rf.persister = persister
	//一个int，代表自己是在peers的哪个位置
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	//初始状态，是个Follower
	rf.state = FOLLOWER
	//一开始的时候选举届是0
	rf.currentTerm = 0
	//他谁都没投
	rf.voteFor = NULL
	//日志里也什么都没有
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{-1,nil})
	//这个似乎是给自动机用的
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	//这个是用来接受通知的？
	rf.applyCh = applyCh
	//这个是用来处理收到append的
	rf.appendLogCh = make(chan bool, 1)
	//这个是用来处理收到vote的
	rf.voteCh = make(chan bool, 1)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//开始进入工作状态，不断监听自己的状态然做相应的事情





	go rf.startUp()

	//返回这个rf
	return rf
}

func (rf *Raft) startUp() {



	heartbeatTime := time.Duration(150) * time.Millisecond
	for {

		//根据状态判断该干什么
		switch rf.state {

		case FOLLOWER, CANDIDATE:
			select {
			//如果自己给别人投票了
			case <-rf.voteCh:


			//如果收到master的心跳
			case <-rf.appendLogCh:

			case <-time.After(time.Duration(rand.Intn(300)+150) * time.Millisecond):

				rf.beCandidate()
			}
		//如果是Leader
		case LEADER:

			rf.startAppendLog()
			time.Sleep(heartbeatTime)
		}
	}

}

//On conversion to candidate, start election:
//• Increment currentTerm
//• Vote for self
//• Reset election timer
//• Send RequestVote RPCs to all other servers
//变为Candidate
func (rf *Raft) beCandidate() {

	rf.mu.Lock()
	defer rf.mu.Unlock()


	//fmt.Println("#",rf.me,": be candidate")

	//自己的状态
	rf.state = CANDIDATE
	//• Increment currentTerm
	rf.currentTerm++
	//• Vote for self
	rf.voteFor = rf.me
	//• Reset election timer
	rf.persist()
	//• Send RequestVote RPCs to all other servers
	go rf.startElection()
}

//开始找别人要票
//开始选举，也就是找别人要票
func (rf *Raft) startElection() {

	//构造投票请求，也就是：我这届投票给我！
	rf.mu.Lock()
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	//代表自己收到的票数
	var votes int32 = 1

	//fmt.Println("#",rf.me," everybody please vote me!")


	//开协程执行
	for i := 0; i < len(rf.peers); i++ {
		//除了自己，让别人投票
		if i != rf.me {
			go func(idx int) {
				//构造返回值
				reply := &RequestVoteReply{}
				//让某个机子给自己投票
				ret := rf.sendRequestVote(idx, &args, reply)

				//如果对方成功响应了你
				if ret {

					rf.mu.Lock()
					state := rf.state
					currentTerm := rf.currentTerm
					rf.mu.Unlock()

					//如果已经被设置为其他状态了，那么就不用管了，别的协程里已经解决了
					if state != CANDIDATE {
						//fmt.Println("#",idx," vote",rf.me,"but useless")
						return
					}
					//rf.mu.Unlock()

					//如果对方告诉你，他比你还大，那说明你out了，马上去当follower
					//todo 当然我还有点没get这是哪种并发情况导致的这个

					//rf.mu.Lock()
					if reply.Term > currentTerm {
						rf.beFollower(reply.Term)
					}
					//rf.mu.Unlock()

					//现在才去看投票结果
					if reply.VoteGranted {
						//这里考虑并发问题，票数增加
						atomic.AddInt32(&votes, 1)
					}

					//If votes received from majority of servers: become leader
					//把检查票的部分直接加入到协程里每个地方去考虑了
					if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
						//rf.mu.Lock()
						rf.beLeader()
						//rf.mu.Unlock()
						//确保自己不要卡在candidate 的 select那里 马上把心跳包发出去
						send(rf.voteCh)
					}
				}
			}(i)
		}

	}

}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIndex()
	if idx < 0 {
		return -1
	}
	return rf.log[idx].Term
}

func (rf *Raft) getLastLogIndex() int {

	return len(rf.log) - 1

}

func send(ch chan bool) {
	select {
	case <-ch:
		//大概是，有状态的话清理掉积累的
	default:

	}
	ch <- true
}

//变成Follower
func (rf *Raft) beFollower(term int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = FOLLOWER
	rf.voteFor = NULL
	rf.currentTerm = term
	rf.persist()

}

//变成leader
func (rf *Raft) beLeader() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//emm非要合适下是不是CANDIDATE，因为可能竞争先变成Follower的情况
	if rf.state != CANDIDATE {
		return
	}

	//fmt.Println("#",rf.me," is now leader")

	//fmt.Println("==========Now Leader Is ",rf.me,"==========")

	//切换状态了
	rf.state = LEADER
	//重置
	//(Reinitialized after election)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
}
