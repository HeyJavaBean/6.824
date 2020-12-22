package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State string

const (
	//RAFT容器的三个状态
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//一个Candidate调用别人的这个RPC接口，请求给自己投票
//在这个场景里是labrpc的模拟网络场景给调用的
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	success := false
	//如果别人比他低一届，不行
	if args.Term < rf.currentTerm {

		//如果他已经投过票了，也叭行
	} else if rf.voteFor != NULL && rf.voteFor != args.CandidateId{

		//如果别人记录的东西还没他的早，滚
	} else if args.LastLogTerm < rf.getLastLogTerm(){

		//如果别人的记录还没他多，那他leader也不用当了...
	} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex() {

	} else{
		//变成那人的follower
		//fmt.Println(rf.me,":I vote for ",args.CandidateId)
		rf.voteFor = args.CandidateId
		success = true
		rf.state = FOLLOWER
		send(rf.voteCh)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = success

}





func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//心跳接受方，需要同步日志
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	defer send(rf.appendLogCh)

	if args.Term >= rf.currentTerm {
		rf.beFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	success := false
	if args.Term < rf.currentTerm {
		return
	}
	prevLogIndexTerm := -1
	//可以正常计算的情况
	if args.PrevLogIndex>=0 && args.PrevLogIndex < len(rf.log){
		prevLogIndexTerm = rf.log[args.PrevLogIndex].Term
	}

	//失败的情况，需要返回
	if prevLogIndexTerm!=args.PrevLogTerm{
		return
	}

	success = true
	reply.Success = success


	newLog := []Log{}
	newLog = append(newLog,rf.log[:args.PrevLogIndex+1]...)
	newLog = append(newLog,args.Entries...)
	rf.log = newLog
	//修改提交
	if args.LeaderCommit >= rf.commitIndex{
		rf.commitIndex = Min(args.LeaderCommit,rf.getLastLogIndex())
		rf.updateLastApplied()
	}
	fmt.Println("[Log]:",rf.log)
}

func (rf *Raft) updateLastApplied(){
	for rf.lastApplied<rf.commitIndex{
		rf.lastApplied++
		curLog:= rf.log[rf.lastApplied]
		msg := ApplyMsg{
			true,
			curLog,
			rf.lastApplied,
		}
		fmt.Println("hey yo send:",msg)
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
					if rf.state!=LEADER{
						return
					}

					args := AppendEntriesArgs{
						rf.currentTerm,
						rf.me,
						//todo 不是很懂得
						append([]Log{},rf.log[rf.nextIndex[idx]:]...),
						rf.commitIndex,

						rf.getLastLogIndex(),
						rf.getLastLogTerm(),
					}

					reply := &AppendEntriesReply{}

					fmt.Println("Send:",
						rf.me,"=>",idx,":",
						"[Term:",rf.currentTerm,"] [LC:",rf.commitIndex,"] [PreIn:",rf.getLastLogIndex(),"] [PreT:",
						rf.getLastLogTerm(),"]\nEntry Is:",append([]Log{},rf.log[rf.nextIndex[idx]:]...))
					ret := rf.sendAppendEntries(idx,&args,reply)

					fmt.Println("Get:",idx,"=>",rf.me,":[Term:",strconv.Itoa(reply.Term),"] [Success:",reply.Success,"]")
					if !ret{
						return
					}

					if reply.Term>rf.currentTerm{
						rf.beFollower(reply.Term)
						return
					}

					if reply.Success{
						rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[idx] = rf.matchIndex[idx] + 1
						rf.updateCommitIndex()
						fmt.Println("[Server for ",idx,"]: MatchIndex:",rf.matchIndex[idx]," NextIndex:",rf.nextIndex[idx])
						return
					}else{
						//fixme
						if rf.nextIndex[idx]>0{
							rf.nextIndex[idx]--
						}

						//锁？
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
	sort.Ints(copyMatchIndex)
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
	index := -1
	term := rf.currentTerm
	isLeader :=  rf.state==LEADER

	if isLeader{


		index = rf.getLastLogIndex()+1

		//todo
		rf.log = append(rf.log, Log{
			rf.currentTerm,command,
		})

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
	rf.log = make([]Log, 1)
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

	//fixme 很多地方都是需要加锁的！！！

	//heartbeatTime := time.Duration(rand.Intn(50)+15) * time.Millisecond
	heartbeatTime := time.Duration(rand.Intn(500)+1500) * time.Millisecond
	for {
		//electionTimer := time.Duration(rand.Intn(100)+100) * time.Millisecond
		//fmt.Println(rf.me,":I'm ",rf.state)
		//根据状态判断该干什么
		switch rf.state {

		case FOLLOWER, CANDIDATE:
			select {
			//如果自己给别人投票了
			case <-rf.voteCh:


			//如果收到master的心跳
			case <-rf.appendLogCh:

			//如果触发了超时  emmm这个是每次for过来又等一次吗
			//case <-time.After(time.Duration(rand.Intn(150)+150) * time.Millisecond):
			case <-time.After(time.Duration(rand.Intn(1500)+1500) * time.Millisecond):
				//变成candidiate
				//todo 怎么优雅的写日志
				fmt.Println(rf.me,":vote me please!")
				rf.beCandidate()
			}
		//如果是Leader
		case LEADER:
			//开始做日志心跳，然后睡觉
			//todo
			//fmt.Println("Leader send troopers!")
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
	//自己的状态
	rf.state = CANDIDATE
	//• Increment currentTerm
	rf.currentTerm++
	//• Vote for self
	rf.voteFor = rf.me
	//• Reset election timer

	//• Send RequestVote RPCs to all other servers
	go rf.startElection()
}

//开始找别人要票
//开始选举，也就是找别人要票
func (rf *Raft) startElection() {

	//构造投票请求，也就是：我这届投票给我！
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm(),
	}

	//代表自己收到的票数
	var votes int32 = 1

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
					//如果已经被设置为其他状态了，那么就不用管了，别的协程里已经解决了
					if rf.state != CANDIDATE {
						return
					}
					//如果对方告诉你，他比你还大，那说明你out了，马上去当follower
					//todo 当然我还有点没get这是哪种并发情况导致的这个
					if reply.Term > rf.currentTerm {
						rf.beFollower(reply.Term)
					}
					//现在才去看投票结果
					if reply.VoteGranted {
						//这里考虑并发问题，票数增加
						atomic.AddInt32(&votes, 1)
					}

					//If votes received from majority of servers: become leader
					//把检查票的部分直接加入到协程里每个地方去考虑了
					if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
						//fmt.Println(rf.me,":total people:",int32(len(rf.peers))," and I got ",votes," from ",idx)
						rf.beLeader()
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
	if len(rf.log)==1{
		return 0
	}
	return len(rf.log) - 2
}

//todo ？？？？？
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
	rf.state = FOLLOWER
	rf.voteFor = NULL
	rf.currentTerm = term
}

//变成leader
func (rf *Raft) beLeader() {

	fmt.Println(rf.me,":Hey I'm Leader!")
	//emm非要合适下是不是CANDIDATE，因为可能竞争先变成Follower的情况
	if rf.state != CANDIDATE {
		return
	}


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
