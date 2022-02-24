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
//	"bytes"
	"sync"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"fmt"
)

const STATEFOLLOWER int = 0
const STATECANDIDATE int = 1
const STATELEADER int = 2
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type Entry struct{
	Index int
	Term int
	Command interface{}
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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
	term  int
	state int // 2leader 1 candidate 0 follwer
	voteFor int//-1 stands for non
	leader int // who?
	live int
	live_lock sync.Mutex
	grantNum int //0
	heartbeat chan int
	lock sync.Mutex
	term_lock sync.Mutex
	vote_lock sync.Mutex
	leader_lock sync.Mutex
	state_lock sync.Mutex
	grant_lock sync.Mutex
	me_lock sync.Mutex


	commitIndex int //只有leader才能确认 然后告诉各个peer
	lastApplied int //每个都要apply 另一个线程去做
	commit_lock sync.Mutex
	apply_lock sync.Mutex
//Volatile state on leaders:

	nextIndex []int //leader通过match修改 然后根据回应修改match 固定一个线程去做
	matchIndex []int 

	entries []Entry
	entry_lock sync.Mutex
	leaderLastApplied int

	applyCh chan ApplyMsg //怎么写不进去东西呢？
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.atomicGetTerm(),rf.atomicGetLeader() == rf.me
}

func (rf *Raft) atomicGetMe()int{
	me := -1
	rf.me_lock.Lock()
	me = rf.me
	rf.me_lock.Unlock()
	return me
}
func (rf *Raft)atomicGetGrantNum ()int{
	num := -1
	rf.grant_lock.Lock()
	num = rf.grantNum
	rf.grant_lock.Unlock()
	return num
}
func (rf*Raft)atomicIncGrantNum(){
	rf.grant_lock.Lock()
	rf.grantNum++
	rf.grant_lock.Unlock()
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
func (rf *Raft) Snapshot(index int,snapshot []byte) {
	// Your code here (2D).
}
func (rf *Raft) CondInstallSnapshot(SnapshotTerm int,
	SnapshotIndex int, Snapshot []byte) bool{
return true
	}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int //candidate’s term
	CandidateId int//candidate requesting vote
	CommitIndex int
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm int//term of candidate’s last log entry (§5.4)
};

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
};

type AppendEntryArgs struct{
	Term int //leader’s term
	LeaderId int //so follower can redirect clients
	PrevLogIndex int //index of log entry immediately preceding
						//new ones
	PrevLogTerm int //term of prevLogIndex entry
	Entries []Entry 	//log entries to store (empty for heartbeat;
				//may send more than one for efficiency)
	LeaderCommit int //leader’s commitIndex

};

type AppendEntryReply struct{
	Term int
	Want int
	Success bool
	Stale bool
};
type DetectLiveArgs struct{
	
}
type DetectLiveReply struct{
	Live bool
}
func (rf *Raft)DetectLive(args *DetectLiveArgs,reply *DetectLiveReply){
	reply.Live = true
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) atomicVoteFor(candidateId int) bool{
	grant := false
	rf.vote_lock.Lock()
	if(rf.voteFor < 0 || rf.voteFor == candidateId){
		rf.voteFor = candidateId
		grant = true
	}
	rf.vote_lock.Unlock()
	return grant
}
func (rf *Raft) atomicSetLeader(leader int)bool{
	ok := false
	rf.leader_lock.Lock()
	rf.leader = leader
	ok = true
	rf.leader_lock.Unlock()
	return ok
}
func (rf *Raft) atomicGetLeader() int{
	leader := -1
	rf.leader_lock.Lock()
	leader = rf.leader
	rf.leader_lock.Unlock()
	return leader
}
func (rf *Raft)atomicClearVote(){
	rf.vote_lock.Lock()
	rf.grant_lock.Lock()
	rf.voteFor = -1
	rf.grantNum = 0
	rf.vote_lock.Unlock()
	rf.grant_lock.Unlock()
}

func (rf *Raft) atomicSetLeaderSetState(leader int,state int)bool{
	ok := false
	clear := false
	if(rf.state == state){
		//follower->follower
		//leader - 1 ->确定
		if(state == STATEFOLLOWER && rf.leader < 0 && leader >=0 ){
			clear = true
		}
		//leader - 1 -> -1 不会发生 因为那样状态会变Candidate
	}else if(rf.state == STATELEADER && state == STATEFOLLOWER){
		//fmt.Println("L -> F  follow ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
		clear = true
	}else if(rf.state == STATEFOLLOWER && state == STATECANDIDATE){
		//只有投给自己成功了才自增term 
		//不然有没有可能在投给他人成功（candidate状态）后再次自增term 导致无法接受HB
		//Commit突然变？
	//	fmt.Println("F->C commit",rf.commitIndex)
	//fmt.Println("F -> C  candi   ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
		clear = true
	}else if(rf.state == STATECANDIDATE && state == STATEFOLLOWER){
		//fmt.Println("C -> F  follow ",rf.me,"term",rf.term,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
		//如果leader是-1则保留投票状态
		//leader确定后则应该清空
		//如果是投给自己没有得到回应应该清空状态
		/*
		已经vote

		未vote （不必清空）
		*/
		//自转
		if(leader == -1){
			//没有得到足够的票数
			//已经投票 dengdaileader
			if(rf.voteFor == rf.me){
				clear = true
	//			fmt.Println("C -> F  remake ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
			}
			//投给别人的 等待去了
		}else if(leader >= 0){
			//别人设置
			clear = true
	//		fmt.Println("C -> F  waitHB ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
	
		}	
	}else if(rf.state == STATECANDIDATE && state == STATELEADER){
		clear = true
		//rf.clearStaleEntry()
		//从保存点恢复 暂时为0
		//不要一口气发送太多丫 先试探commit 需要一个定制的？？
	//	fmt.Println("C -> L  leader ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
		rf.matchIndex = make([]int,len(rf.peers))
		rf.nextIndex = make([]int,len(rf.peers))
		for index,_ := range rf.nextIndex{
			rf.nextIndex[index] = 1
		}
		rf.matchIndex[rf.me] = rf.atomicGetLastEntry().Index
		rf.nextIndex[rf.me] = rf.atomicGenerateIndex()
	}else{
		//不合法的状态转换
		return false
	}
	rf.leader_lock.Lock()
	rf.state_lock.Lock()
	rf.leader = leader
	rf.state = state
	if(clear){
		rf.atomicClearVote()
	}
	ok = true
	rf.leader_lock.Unlock()
	rf.state_lock.Unlock()
	return ok
}

func (rf *Raft)atomicSetTermIfNeed(term int) bool{
	set := false
	rf.term_lock.Lock()
	currentTerm := rf.term
	if(term > currentTerm){
		rf.term = term
		set = true
	}
	rf.term_lock.Unlock()
	return set
}
func(rf *Raft)atomicGetTerm()int{
	t := -1
	rf.term_lock.Lock()
	t = rf.term
	rf.term_lock.Unlock()
	return t
}
//too many disconnect 保证commit再次同步
//LastLogIndex:可能在commit之前的last 要包含commit的那个而且是正确版本的
//Leader投票变Follower
//LEADER要学习更大的follower吗
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.term
	reply.VoteGranted = false
	
	//读取vote和改变vote要原子过程
	//接到不合格的candidate发起的request 不会改变原来的状态
	//commit并不是被所有知晓 所有里持有commit最大的那个
	//反证法！多数已收到只是没有更新commit 获胜follower其log大于剩余至少1/2
	//故该最大log大于等于commit
	//优先级：Term必须符合 LastLogTerm必须>= LastLogIndex是辅助项（快速选出）更新的term无需满足Index
	lastentry := rf.atomicGetLastEntry()
	lastLogIndex := lastentry.Index
	lastLogTerm := lastentry.Term
	if(args.Term >= rf.term && (args.LastLogTerm > lastLogTerm || (lastLogTerm == args.LastLogTerm && args.LastLogIndex >= lastLogIndex))){
	//args.LastLogTerm > rf.atomicGetLastEntry().Term)){
		if(rf.atomicVoteFor(args.CandidateId)){
			reply.VoteGranted = true
			if(rf.voteFor == rf.me){
			}else{
				rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
			}
		}
}	
	rf.atomicSetTermIfNeed(args.Term)
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs,reply *AppendEntryReply){
	reply.Term = rf.term
	if(args.Term < rf.term){
		//对方 turn follower
		reply.Success = false
		return
	}
	if(rf.atomicGetMe()==args.LeaderId){
		reply.Success = true
		//reply.Want = rf.atomicGenerateIndex()
		return
	}

	rf.atomicSetTermIfNeed(args.Term)
	//旧的leader会在这里转化为follower吗？
	if rf.leader < 0 {
		rf.atomicSetLeaderSetState(args.LeaderId,STATEFOLLOWER)
	}
	//HB COME		
	select{
	case rf.heartbeat <- args.LeaderId:
		break
	default:
		break //do not access 过时的心跳
	}
	//为什么map里有两个占位符？
	if(rf.entries[len(rf.entries) - 1].Index < args.PrevLogIndex || rf.entries[args.PrevLogIndex].Term != args.PrevLogTerm){
		//版本覆盖
				if(len(rf.entries) <= args.PrevLogIndex){
					//无法连续接收
					reply.Success = false
					return
				}else{
					rf.entries = rf.entries[0:args.PrevLogIndex]
				}
	}
	
	//CHECK VERSION
	copy_index := 0
	
	if(len(args.Entries) == 0){
		//更新commit必须有正确版本？
		reply.Success = true		
		if(args.LeaderCommit <= args.PrevLogIndex){
			rf.commitIndex = args.LeaderCommit}
		return
	}
	//本地 0[0]
	//发来 P0[0] entries{0[0]}
	//识别 entries{0[0]} 为多余的！
	for index,correct_entry	:= range args.Entries{
		if(index + args.PrevLogIndex + 1 < len(rf.entries)){
			//exist entry
			if(rf.entries[index + args.PrevLogIndex + 1].Index != correct_entry.Index || rf.entries[index + args.PrevLogIndex + 1].Term != correct_entry.Term){
					//wrong version
					rf.entries = rf.entries[0:index + args.PrevLogIndex + 1]
					copy_index = index
					break
				}else{
					//correct
				}
		}else{
			//non exist
			//but may repeat!!!干脆在保存点那里恢复好！！
			if(rf.atomicGetLastEntry().Index + 1 != correct_entry.Index){
				reply.Success = false
				return
			}
			copy_index = index
			break
		}
	}
	 for copy_index < len(args.Entries) {
		 rf.entries = append(rf.entries,args.Entries[copy_index])
		 copy_index++
	 }
	//这次发来的commit可能包含新版本 所以记录下新版本再更新commit就是安全的
	rf.commitIndex = args.LeaderCommit
	//fmt.Println(rf.entries)
	reply.Success = true
}

func (rf *Raft)atomicGetLastEntry()Entry{
	ret := Entry{}
	rf.entry_lock.Lock()
	ret = rf.entries[len(rf.entries)-1]
	rf.entry_lock.Unlock()
	return ret
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
//DETECT LIVE适合用AppendEntry吗（暗含检验leader）？用RequestVote可以吗
func (rf *Raft) sendDetectLives(t int)int{
		args := RequestVoteArgs{}
		reply := RequestVoteReply{}
		args.Term = -1
		live := []int{}
		consume := len(rf.peers)
		for index,_ := range rf.peers{
			//lives只受是否完成调用影响
			go rf.sendRequestVoteInTimeout(t,&live,&consume,index,&args,&reply)
		}
	time.Sleep(time.Duration(t))
	for consume > 0{
		//	等待判活时间结束
	}
	return len(live)
}
func (rf *Raft) sendRequestVote(live *[]int,server int, args *RequestVoteArgs, reply *RequestVoteReply)  {
	args.Term = rf.atomicGetTerm()
	args.CandidateId = rf.atomicGetMe()
	args.CommitIndex = rf.commitIndex
	reply.VoteGranted = false
	// args.LastLogIndex = rf.entries[len(rf.entries)-1].Index
	// args.LastLogTerm = rf.entries[len(rf.entries)-1].Term
	 args.LastLogIndex = rf.atomicGetLastEntry().Index
	 args.LastLogTerm = rf.atomicGetLastEntry().Term
	
	rf.peers[server].Call("Raft.RequestVote", args, reply)
	if(reply.Term > rf.term) {
		rf.term = reply.Term
		rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
	}
	*live = append(*live,server)
}
//针对一个发送 并在timeout时间后修改得到的投票数
func (rf *Raft) sendRequestVoteInTimeout(timeout int,live *[]int,consume *int,server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = false
	go rf.sendRequestVote(live,server,args,reply)
	time.Sleep(time.Duration(timeout))
	if(reply.VoteGranted){
		rf.atomicIncGrantNum()
		//fmt.Println("C",rf.me,"got vote num",rf.grantNum)
	}
	*consume--
}
//现在的逻辑是心跳携带entry 都要设置leader 那么args被填上leader的位置: tick循环 infi循环
func (rf *Raft) sendAppendEntry(done chan int,server int,args *AppendEntryArgs,reply *AppendEntryReply){
	args.PrevLogTerm = rf.entries[rf.matchIndex[server]].Term
	args.PrevLogIndex = rf.entries[rf.matchIndex[server]].Index
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	args.Term = rf.term
	reply.Success = false
//	fmt.Println(rf.me,"send HB TO",server)
	rf.peers[server].Call("Raft.AppendEntry",args,reply)
	//rf.atomicSetTermIfNeed(reply.Term)
	done <- server
}

//command不包含index term entry包含 index term都是leader分配的
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	var index int
	var term int 
	var isLeader bool
	isLeader = false
	index = -1
	term = -1
	if(rf.atomicGetLeader() == rf.atomicGetMe()){
		//fmt.Println(rf.me,"im leader i 转发")
		isLeader = true
		index = rf.atomicGenerateIndex()
		//fmt.Println("leader give index",index)
		term = rf.atomicGetTerm()
		if(rf.saveEntryToLeader(command,index)){
		//go rf.infinitelySendToPeersEveryTimeout(50000000,command,index,term)
		//rf.sendToPeersOnce(50000000)
		} //成功就结束 是不是要延迟开始？
	}
	return index, term, isLeader
}
func (rf *Raft)saveEntryToLeader(command interface{},index int) bool{
	save := false
	if(rf.atomicGenerateIndex() == index){
		entry := Entry{}
		entry.Index = index
		entry.Command = command //封装了Command
		entry.Term = rf.atomicGetTerm() //发来时leader的term
		rf.entries = append(rf.entries,entry)
		rf.nextIndex[rf.me] = index+1
		rf.matchIndex[rf.me] = index
		save = true
	//	fmt.Println("new entry[",index,"] save to leader")
	}
	return save
}
func (rf *Raft)atomicGenerateIndex()int{
	ne := 0
	rf.entry_lock.Lock()
	ne = rf.entries[len(rf.entries) - 1].Index + 1
	rf.entry_lock.Unlock()
	return ne
}
//too many RPC bytes的平衡 数量和字节的平衡
//如果心跳太慢 集中发送报文的频率就慢？
func (rf *Raft) sendToPeersOnce(timeout int){
	if(rf.state!=STATELEADER){return}
	last := rf.atomicGetLastEntry().Index //集中发送
	live := []int{} //server - live

	consume := len(rf.peers)
	args := make([]AppendEntryArgs,len(rf.peers))
	reply := make([]AppendEntryReply,len(rf.peers))
	
	for server,_ := range rf.peers{
		i := rf.nextIndex[server]
		rf.entry_lock.Lock()

		args[server].PrevLogIndex = rf.entries[rf.matchIndex[server]].Index
		args[server].PrevLogTerm = rf.entries[rf.matchIndex[server]].Term

		if(rf.nextIndex[server] - rf.matchIndex[server] > 1){
			//fmt.Println(server,"on way")
			goto emptysend
		}
		//fmt.Println("send to",server)
		for i <= last && i < len(rf.entries){
			//刚准备commit 但是有没有被新leader覆盖捏。。
			args[server].Entries = append(args[server].Entries,rf.entries[i])
			i++
		}
		emptysend:

		rf.entry_lock.Unlock()
		reply[server].Term = -1
		reply[server].Success = false
		go rf.AppendEntryInTimeoutRecordFail(&live,timeout,&consume,server,&args[server],&reply[server])
	}
	time.Sleep(time.Duration(timeout))
	for consume > 0{
	}
	//exit check
// 	//biger term 检测
	for _,r := range reply{
		if(r.Term > rf.term) {
			rf.term = r.Term
			rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
		//	fmt.Println("leader find a bigger term")
			return
		}
	}
	succnum:=0
	for _,ack := range live{
		if (ack == 1){
			succnum++
		}
	}
	livenum := len(live)

	if(succnum > (len(rf.peers)/2)){
		//保证现在commit的不会被覆盖掉
		if(last < len(rf.entries) && rf.entries[last].Term >= rf.term){
			//figure 8
			//fmt.Println("f8")
				rf.commitIndex = last
		}
				//fmt.Println("success")
	}else{
		//fmt.Println("ack太少")
	}
	
	 if(livenum <=  len(rf.peers)/2) {
		// fmt.Println("live 太少")
		 rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
	 }

// 	//old leader
// 	stale := 0
// 	for _,ack := range live{
// 		if ack == 0 {
// 			stale++
// 		}
// 	}
// 	if(stale + len(rf.peers) - livenum >len(rf.peers)/2) {
// 		fmt.Println("leader 失联 + notcommit太多")
// 		rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
// 	}
}
//和HB是两路
func(rf *Raft) AppendEntryInTimeoutRecordFail(live *[]int,timeout int,consume *int,server int,args *AppendEntryArgs,reply *AppendEntryReply){
	done := make(chan int,1)
	 //本次发送的最高的
	go rf.sendAppendEntry(done,server,args,reply)
	//如果发送空log也要考虑next
	if(len(args.Entries) > 0){
	rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
	}
	//限时等待
	time.Sleep(time.Duration(timeout))
	select{
	case <-done:
		if(reply.Success){
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		*live = append(*live,1)
		}else{
			//回退next情况①
			//不能确定从哪个开始不一样了
			if(rf.matchIndex[server] > 0){
			rf.matchIndex[server]--}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		//	fmt.Println("not match")
		    *live = append(*live,0)
		}
			break
	default:
		//回退next情况② ?没理我
		//rf.nextIndex[server] = reply.Want
		break		
	}
	*consume--
}

func (rf *Raft) checkApply(initstate int){
	for true {
		if(rf.state!=initstate){break}
		msg := <-rf.applyCh
		fmt.Println(rf.me,"aplychan recieve",msg.CommandIndex,msg.Command)
	}
}
func (rf *Raft) apply(initstate int){
	applyindex := rf.lastApplied + 1
	for (applyindex < len(rf.entries) && applyindex <= rf.commitIndex ){
		//Figure8说了什么？
		//term2:leader1 :2[2] 未同步到多数 未commit
		//term3:leader5 :2[3]
		//term4:leader1 将其2[2]同步到多数 自己commit但未告知多数
		//term5:为了保证被提交的2[2] 不可以让为拥有2[2]的当选
		//但worker5再次当选leader[因为最新log 较大term优先原则！]
		//但实际上2[2]是在term4被commit的！那么怪异的2[2]就这样被覆盖
		//不要commit旧term的！因为如果这个时候挂了 旧term的log可能会被覆盖 除非又有本term的log来到
		//证明本轮的多数已经拥有“最大的term” 以保全commit以往是安全的
		apl := ApplyMsg{}
		apl.CommandValid = true
		apl.CommandIndex = rf.entries[applyindex].Index
		apl.Command = rf.entries[applyindex].Command

		rf.applyCh <- apl
		//fmt.Println("leader",rf.leader,rf.me,"apply",apl.CommandIndex,"[",rf.entries[applyindex].Term,"]","and nonw comm",rf.commitIndex,"term",rf.term,rf.entries)
		applyindex++ //让apply不合法
		rf.lastApplied++ //没错啊 之前是比applyindex小1
		//不同的提交轮次之间关系是顺序的！所以不担心重复提交
	}
	//锁住了entry 锁不住commit 开始提交的commit限制一定要和结束的lastapply相同
}
func (rf *Raft)applyonce(state int){
	if(rf.state!=state){
		fmt.Println("leader out")
		return
	}
	
	//applyindexapplyindex := rf.commitIndex
	var applyindex int

	//fmt.Println(rf.me,"lastApply is",rf.lastApplied,"and last commited is",rf.commitIndex)

	//if(rf.commitIndex > rf.lastApplied){
	//	applyindex = rf.lastApplied + 1
	applyindex=rf.lastApplied
	if(rf.leaderLastApplied > applyindex){
		applyindex = rf.leaderLastApplied
	}
	
	for applyindex > 0 && applyindex < len(rf.entries) && 
	rf.entries[applyindex].Index <= rf.leaderLastApplied{
		apl := ApplyMsg{}
		apl.CommandValid = true
		apl.CommandIndex = rf.entries[applyindex].Index
		apl.Command = rf.entries[applyindex].Command
		rf.applyCh <- apl
	//	A := <-rf.applyCh
		
		rf.lastApplied ++
		applyindex++
	}
	
	//else 不需要补齐
	//match和commit初始化为什么？
	rf.nextIndex[rf.atomicGetMe()] = rf.entries[len(rf.entries)-1].Index+1
	rf.matchIndex[rf.atomicGetMe()] = rf.entries[len(rf.entries)-1].Index
	
	
	//删除掉commit以前的？感觉也可以不删
}
func (rf *Raft)atomicGetCommit()int{
	ret := -1
	rf.commit_lock.Lock()
	ret = rf.commitIndex
	rf.commit_lock.Unlock()
	return ret
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// heartsbeats recently.

func (rf *Raft) ticker() {
	rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
	for rf.killed() == false {
		//FOLLOWER状态 voteFor应该清除吗？
		//FOLLOWER来自两个转化：①发现自己不是leader(可能再次投给别人 要在setState里clearVote & grantNum)
		//②已经为其他候选投出 在Follower状态不应该投出
		//③已经为自己投出但是没有得到足够回应 若因为live数量不足应该递减term 若只是没有得到足够投票不应该递减term
			//此种都应该清除投票状态
		if(rf.state == STATEFOLLOWER){
			rf.apply(STATEFOLLOWER)
			rand.Seed(time.Now().UnixNano())
			randTime := rand.Intn(150)
			time.Sleep(time.Duration(150000000+randTime*1000000)) 
			//candidatetimeout + heartbeat时间如果 > 两个followersleep只差
			//则明明已经有了leader却没有发现
				if rf.atomicGetLeader() >= 0 {
					//fmt.Println("term",rf.term,"F",rf.me,"know leader is",rf.leader)
					select{
					case <-rf.heartbeat:
//fmt.Println("follow ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
					break
					default:
					//	fmt.Println(rf.me,"find NO HB FROM",rf.leader)
						rf.atomicSetLeaderSetState(-1,STATECANDIDATE)
					}
					}else{
					//leader < 0
				//	fmt.Println("term",rf.term,"F",rf.me,"find leader dead leader:",rf.leader)
				//	fmt.Println("CANDI TIMEOUT",randTime)
					rf.atomicSetLeaderSetState(-1,STATECANDIDATE)
					continue //重新进入循环
				}
		}else if rf.state == STATECANDIDATE{
			//和FOLLOWER互相转化 voteFor清空没？
			//检查<0和改变应该原子地完成 否则可能会投出多个leader
			if(rf.atomicVoteFor(rf.atomicGetMe())){
			//	add := rand.Intn(len(rf.peers))
				rf.term_lock.Lock()
				rf.term ++
				rf.term_lock.Unlock()
			//	fmt.Println("term",rf.term,"C",rf.me,"commit",rf.commitIndex)
				//同时计算活着的&得到的投票数
				live := []int{}
				consume := len(rf.peers)
				for candi,_ := range rf.peers{
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				//只在sendRequestVote里修改grantNum
				go rf.sendRequestVoteInTimeout(1000000,&live,&consume,candi,&args,&reply)
			}
				time.Sleep(time.Duration(1000000))
				for consume > 0 {

				}
				livenum := len(live)
				//if(livenum > 1 && (livenum) > len(rf.peers)/2 && rf.atomicGetGrantNum() > (livenum)/2){
				if((livenum) > len(rf.peers)/2 && rf.atomicGetGrantNum() > len(rf.peers)/2){
				//	fmt.Println(rf.me,"get grantNUM",rf.grantNum,"so it become L")
					rf.atomicSetLeaderSetState(rf.atomicGetMe(),STATELEADER)
					continue
				}else{
					//这其中又有什么omi 孤立 or 少数
					if((livenum) > len(rf.peers)/2){	
					}else{
						rf.term--
					}
					//fmt.Println(rf.me,"lose vote")
					rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
					continue
				}
			}else{
					//fmt.Println("have Vote For",rf.voteFor)
					rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
					continue
				}
		}else if rf.state == STATELEADER{
			rf.sendToPeersOnce(150000000)
			rf.apply(STATELEADER)
		//	fmt.Println("leader ",rf.leader,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
			
			continue
}

	}
	//	fmt.Println(rf.me,"killed")
	
}//func

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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.leader = -1 // dont know who is the leader
	rf.voteFor = -1
	rf.dead = 0
	rf.heartbeat = make(chan int,1)
	rf.applyCh = applyCh //往里面写了 怎么读不出来呢？
	rf.entries = []Entry{}
	dumb := Entry{}
	dumb.Term = 0
	dumb.Index = 0 //占位
	rf.entries = append(rf.entries,dumb)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderLastApplied = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.matchIndex = make([]int,len(rf.peers)) //0
	rf.nextIndex = make([]int,len(rf.peers)) // 1
	//see :setStateSetLeader
	for index,_ := range rf.nextIndex{
	rf.nextIndex[index] = 1	
	}
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()


	return rf
}


//2n+1
//挂掉N正常？
//一定要选择含有commit的那个