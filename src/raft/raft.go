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
	if(rf.state == state){
		//follower->follower
		//leader - 1 ->确定
		if(state == STATEFOLLOWER && rf.leader < 0 && leader >=0 ){
			rf.atomicClearVote()
		}
		//leader - 1 -> -1 不会发生 因为那样状态会变Candidate
	}else if(rf.state == STATELEADER && state == STATEFOLLOWER){
		rf.atomicClearVote()
	}else if(rf.state == STATEFOLLOWER && state == STATECANDIDATE){
		//只有投给自己成功了才自增term 
		//不然有没有可能在投给他人成功（candidate状态）后再次自增term 导致无法接受HB
		rf.atomicClearVote()
	}else if(rf.state == STATECANDIDATE && state == STATEFOLLOWER){
		//如果leader是-1则保留投票状态
		//leader确定后则应该清空
		//如果是投给自己没有得到回应应该清空状态
		if(rf.voteFor == rf.me){
			rf.atomicClearVote()
		}
	}else if(rf.state == STATECANDIDATE && state == STATELEADER){
		rf.atomicClearVote()
		//rf.clearStaleEntry()
		//从保存点恢复 暂时为0
		rf.matchIndex = make([]int,len(rf.peers))
		rf.nextIndex = make([]int,len(rf.peers))
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
	ok = true
	rf.leader_lock.Unlock()
	rf.state_lock.Unlock()
	return ok
}

func (rf *Raft)atomicSetTermIfNeed(term int) bool{
	// set := false
	// rf.term_lock.Lock()
	// currentTerm := rf.term
	// if(term > currentTerm){
	// 	rf.term = term
	// 	set = true
	// }
	// rf.term_lock.Unlock()
	// return set
	rf.term = term
	return true
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
	reply.VoteGranted = false
	//读取vote和改变vote要原子过程
	//接到不合格的candidate发起的request 不会改变原来的状态
	//commit并不是被所有知晓 所有里持有commit最大的那个
	if(args.LastLogIndex >= rf.commitIndex  &&args.Term >= rf.term){
	//if(args.LastLogIndex >= rf.commitIndex  && args.LastLogTerm >= rf.atomicGetLastEntry().Term &&args.Term >= rf.term){
	//if(args.LastLogIndex >= rf.commitIndex  && args.Term >= rf.term){
		if(rf.atomicVoteFor(args.CandidateId)){
			reply.VoteGranted = true
			//LEADER和FOLLOWER和投给自己的CANDIDATE、未投票的CANDIATE收到
			rf.atomicSetTermIfNeed(args.Term)
			if(rf.voteFor == rf.me){
				//继续在CANDIDATE状态等待grantNum的判定
			}else{
				rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
			}
		}
			rf.term = args.Term
			reply.Term = rf.atomicGetTerm()
			return
}	
	reply.Term = rf.atomicGetTerm()
	return
}
func (rf *Raft) clearStaleEntry(){
	dele := len(rf.entries) - 1 //以args.Eentries的版本为基础 
	for dele > 0  {
		if(rf.entries[dele].Index > rf.commitIndex){
				rf.entries = rf.entries[0:len(rf.entries)-1]
			}else{
			 break
			}
				dele--
		}
}
func (rf *Raft) AppendEntry(args *AppendEntryArgs,reply *AppendEntryReply){
	if(args.Term >= rf.term && args.LeaderCommit >= rf.commitIndex){
			//我认为你是leader
			rf.atomicSetTermIfNeed(args.Term)
			reply.Term = rf.atomicGetTerm() //如果对方term不一致的话说明不承认leader辣
			//在HB收到最新的commit 不携带entry 也可以校验本地log是否过时
			//commit跟上了 entry却没跟上 有意义吗？
			//rf.commitIndex = args.LeaderCommit
			//newcommit := args.commitIndex


			//FOLLOWER记录来自LEADER的心跳
			if rf.leader < 0 {
				//在FOLLOWER状态收到HB 在StateChange里清空投票状态
				rf.atomicSetLeaderSetState(args.LeaderId,STATEFOLLOWER)
				//fmt.Println("in append entry ",rf.me,"set L TF",args.LeaderId)
			}
			if rf.leader != rf.me{
			//因为
			select{
			case rf.heartbeat <- args.LeaderId:
				break
			default:
				break //do not access 过时的心跳
			}
			//FOLLOWER更新commit
			
			//FOLLOWER查看携带的ENTRY 
			if(len(args.Entries) == 0){
				//再sendEntry里不要设置want哇因为不检查版本 检查完版本后的want才是正确的
				reply.Success = true
				
				if(args.LeaderCommit < rf.atomicGenerateIndex()){
					rf.commitIndex = args.LeaderCommit
				}
				//reply.Want = rf.atomicGenerateIndex();
				//HB只回复进度 不更新commit
				return
			}else{
				//删除过时的本地entry 并判断是否能存下刚刚发来的 返回Want哪个 将成为next
			}
			}else{
				//LEADER收到自己的HB 同样返回接受和更新过的term
				reply.Success = true
				//reply.Term = rf.term 统一修改
				reply.Want = rf.atomicGenerateIndex()
				return
			}
		
		
		
		//如果和发来的版本矛盾（已经“新发来”的循环里）
		//弄一个新发来的index - term map把
		leaderEdition := make(map[int]int,len(args.Entries))
		for _,entry := range args.Entries {
			leaderEdition[entry.Index] = entry.Term
		}
		//FOLLOWER删除旧版本entry 如果在发来的entry范围内而拥有低版本的entry则删掉
		dele := 1 //以args.Eentries的版本为基础 
		for dele < len(rf.entries) {
			if( rf.entries[dele].Index > args.LeaderCommit || 
				rf.entries[dele].Index >= args.Entries[0].Index && 
				rf.entries[dele].Term != leaderEdition[rf.entries[dele].Index] ){
					rf.entries = rf.entries[0:dele]
					break
				}else{
				 dele++
				}		
			}
		//当FOLLOWER的本地最高Index无法接续args.Entries[0] 则un success
		//begin index 

		nextIndex := rf.atomicGenerateIndex()
		if(args.Entries[len(args.Entries) - 1].Index < nextIndex){
			reply.Success = true
			reply.Want = rf.atomicGetLastEntry().Index + 1
			if(args.LeaderCommit > rf.commitIndex && args.LeaderCommit <= rf.atomicGetLastEntry().Index){
			rf.commitIndex = args.LeaderCommit}
			return
		}else if (nextIndex < args.Entries[0].Index){
				//wait old come
				reply.Success = false
				reply.Want = rf.atomicGetLastEntry().Index + 1
				if(args.LeaderCommit > rf.commitIndex && args.LeaderCommit <= rf.atomicGetLastEntry().Index){
					rf.commitIndex = args.LeaderCommit}
				return
		}else{
		//找到highest + 1开始写入 一直到最后一个
				begin := 0
				for begin < len(args.Entries)  {
				//	fmt.Println("begin:",begin)
					if(args.Entries[begin].Index == nextIndex){
						break
					}else{
						begin ++
					}
				}
				for begin <= (len(args.Entries) -1 ){
					rf.entries = append(rf.entries,args.Entries[begin])
					begin++
				}
				//fmt.Println(rf.me,"writeok",args.Entries[len(args.Entries) - 1].Index)
				reply.Want = rf.atomicGetLastEntry().Index + 1
				reply.Success = true
				if(args.LeaderCommit > rf.commitIndex && args.LeaderCommit <= rf.atomicGetLastEntry().Index){
					rf.commitIndex = args.LeaderCommit}
				return
			}
}else{
	reply.Term = rf.term
	reply.Success = false
}
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
	reply.VoteGranted = false
	// args.LastLogIndex = rf.entries[len(rf.entries)-1].Index
	// args.LastLogTerm = rf.entries[len(rf.entries)-1].Term
	 args.LastLogIndex = rf.entries[rf.commitIndex].Index
	 args.LastLogTerm = rf.entries[rf.commitIndex].Term
	
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
	args.PrevLogTerm = rf.atomicGetLastEntry().Term
	args.PrevLogIndex = rf.atomicGetLastEntry().Index
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	args.Term = rf.term
	reply.Success = false
	rf.peers[server].Call("Raft.AppendEntry",args,reply)
	//rf.atomicSetTermIfNeed(reply.Term)
	if(reply.Term > rf.term) {
		rf.term = reply.Term
		rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
	}
	done <- server
}
//只取timeout那个时间点的结果
//暂时不想赋予lives的内容太多的意义 只取其长度？
func(rf *Raft) sendAppendEntryInTime(lives *[]int,timeout int,consume *int,server int,args *AppendEntryArgs,reply *AppendEntryReply){
	//fmt.Println("I GOT LD COMMIT",rf.commitIndex)
	//都没有机会打印哇？
	done := make(chan int,1)
	liveserver := -1
	go rf.sendAppendEntry(done,server,args,reply)
	time.Sleep(time.Duration(timeout))

	select{
	case liveserver = <-done://成功失败都是探测到live
		*lives = append(*lives,liveserver)
			break
	default:break
	}
	*consume--;
}
//关心谁不接受心跳吗？（不承认leader）
//关心无法通信吗？（可能区域联通）
func(rf *Raft) sendHeartBeatAndSleepTimeout(timeout int)bool{
	live := []int{}
	consume := len(rf.peers)
	args:= make([]AppendEntryArgs,len(rf.peers))
	reply:=make([]AppendEntryReply,len(rf.peers))
	for index,_ := range rf.peers{
		go rf.sendAppendEntryInTime(&live,timeout,&consume,index,&args[index],&reply[index])
	}
	time.Sleep(time.Duration(timeout))
	for consume > 0 {

	}
	agree := 0
	for i,r := range reply{
		if(r.Success && r.Term <= args[i].Term) {
			//fmt.Println("follower term biger")
			agree++
		}
	}
	if(agree <= len(rf.peers)/2){
		rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
		fmt.Println(rf.me,"old leader")
		//大部分的term是什么？
		//可能自己增加的term太多 但是并未拥有最新的entry 没有做回FOLLOWER的离群者没有删掉旧的Entry
		//term怎么说呢？
	}

	livenum := len(live)
	// fmt.Println(rf.me,"find live",live)
	 //在candidate探活状态转换里修改Term
	 if(livenum - 1 <  (len(rf.peers)-1)/2) {
		 rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
	 }
	return false
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
		//command先存在本地 然后启动无限过程:
		//发送给peer
		//set commit状态在
		//当然也包括自己哈
		//初始化match和next没
		//怎么存到【】entry里呢？发给各个peer要读leader的[]entry啊
		if(rf.saveEntryToLeader(command,index)){
	//	fmt.Println("leader save",index,"now COMMIt is ",rf.commitIndex)
		go rf.infinitelySendToPeersEveryTimeout(50000000,command,index,term)} //成功就结束 是不是要延迟开始？
	}
	//	什么时候返回给客户？还是说只有Appl之后才返回？
	// return immediately
	//fmt.Println(rf.me,"says isLeader is",isLeader,"==",rf.leader,"and index is",index,"==",rf.atomicGenerateIndex())
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
func (rf *Raft) infinitelySendToPeersEveryTimeout(timeout int,command interface{},index int,term int){
	//初始化
	try := 1
	resend := []int{}

	for i,_ := range rf.peers {
		resend = append(resend,i)
	}
	last := rf.atomicGetLastEntry().Index
	again:
	fail := make(map[int]int)
	for _,s:=range resend{
		fail[s] = s
	}
	//成功过半 如何定义？
		if(len(rf.peers) - len(resend) > (len(rf.peers)/2)){
	//	fmt.Println("majority record entry[",index,"]")
		if(last > rf.commitIndex){
			rf.commitIndex = last
			//fmt.Println("leader commit",rf.commitIndex)
		}
	}
//	fmt.Println("send entry[",index,"] round ",try)
	try++
	consume := len(resend)
	args := make([]AppendEntryArgs,len(rf.peers))
	reply := make([]AppendEntryReply,len(rf.peers))
	
	for _,server := range resend{
		if(rf.state!=STATELEADER) {return}
		i := rf.nextIndex[server]
		//发给自己空包
		for i <= last{
			args[server].Entries = append(args[server].Entries,rf.entries[i])
			i++
		}
		
		reply[server].Term = -1
		reply[server].Success = false
		//record fail and success !match
		//实际上是20ms
		go rf.AppendEntryInTimeoutRecordFail(&fail,timeout,&consume,server,&args[server],&reply[server])
	}
	time.Sleep(time.Duration(timeout))
	for consume > 0{

	}
	if(len(fail) > 0) {
		//fmt.Println("resend",index,"to",fail)
		resend = []int{}
		for _,f :=range fail{
			resend =append(resend,f)
		}
		goto again
	} else{
	//	fmt.Println(index,"之前 all send")
		/*之前忘了这里*/
		if(last > rf.commitIndex){
			rf.commitIndex = last
			//fmt.Println("FOUND NO FAIL SO UPDATE COMMIT")
		}
	}

}
//使用map是因为将成功的从map中删除
func(rf *Raft) AppendEntryInTimeoutRecordFail(fail *map[int]int,timeout int,consume *int,server int,args *AppendEntryArgs,reply *AppendEntryReply){

	//限时等待
	done := make(chan int,1)

	liveserver:= -1
		//will send change next
	//TOatomic
	rf.nextIndex[server] = args.PrevLogIndex + 1
	
	go rf.sendAppendEntry(done,server,args,reply)
	time.Sleep(time.Duration(timeout))
	*consume--
	select{
	case liveserver = <-done:
			break
	default:
		break		
	}
	//下一层函数里处理了reply
	//success:已经拥有这些咯
	if(liveserver >= 0 && reply.Success){
		delete(*fail,server)
	}else{
		
	}
	rf.nextIndex[server] = reply.Want
	rf.matchIndex[server] = reply.Want - 1
}

func (rf *Raft) checkApply(initstate int){
	for true {
		if(rf.state!=initstate){break}
		msg := <-rf.applyCh
		fmt.Println(rf.me,"aplychan recieve",msg.CommandIndex,msg.Command)
	}
}
func (rf *Raft) apply(initstate int){
	//checkstate == initstate
	if(rf.state!=initstate){
		return
	}
	applyindex := rf.lastApplied + 1

	for ( applyindex > 0 && applyindex < len(rf.entries)&& 
	rf.entries[applyindex].Index <= rf.commitIndex ){
		apl := ApplyMsg{}
		apl.CommandValid = true
		apl.CommandIndex = rf.entries[applyindex].Index
		apl.Command = rf.entries[applyindex].Command

		rf.applyCh <- apl
	//	A := <-rf.applyCh
		rf.lastApplied ++
		applyindex++
		//fmt.Println("leader",rf.leader,rf.me,"apply","index:",apl.CommandIndex,"and nonw comm",rf.commitIndex,"term",rf.term)
	}
		//在一个状态里有timeout的sleep辣
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
			time.Sleep(time.Duration(150000000+randTime*1000000)) //150~300ms
			//candidatetimeout + heartbeat时间如果 > 两个followersleep只差
			//则明明已经有了leader却没有发现
				if rf.atomicGetLeader() >= 0 {
					//fmt.Println("term",rf.term,"F",rf.me,"know leader is",rf.leader)
					select{
					case <-rf.heartbeat:
							//fmt.Println("latest entry is ",rf.entries[len(rf.entries)-1].Index,"and recieve hb from leader ",rf.leader,"now apply",rf.lastApplied,"commit",rf.commitIndex)	
					break
					default:
						//fmt.Println("but no hb come so F",rf.me,"turn to C")
						rf.atomicSetLeaderSetState(-1,STATECANDIDATE)
					}
					}else{
					//leader < 0
					//fmt.Println("term",rf.term,"F",rf.me,"find leader dead leader:",rf.leader)
					rf.atomicSetLeaderSetState(-1,STATECANDIDATE)
					continue //重新进入循环
				}
		}else if rf.state == STATECANDIDATE{
			//和FOLLOWER互相转化 voteFor清空没？
			//检查<0和改变应该原子地完成 否则可能会投出多个leader
			if(rf.atomicVoteFor(rf.atomicGetMe())){
				rf.term_lock.Lock()
				rf.term++
				rf.term_lock.Unlock()
				//fmt.Println("term",rf.term,"C",rf.me,"vote for it self get grant",rf.grantNum,rf.voteFor)
				//同时计算活着的&得到的投票数
				live := []int{}
				consume := len(rf.peers)
				randTime := rand.Intn(150000000)
				for candi,_ := range rf.peers{
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				//只在sendRequestVote里修改grantNum
				go rf.sendRequestVoteInTimeout(randTime,&live,&consume,candi,&args,&reply)
			}
				time.Sleep(time.Duration(randTime))
				for consume > 0 {

				}
				livenum := len(live)
				//if(livenum > 1 && (livenum) > len(rf.peers)/2 && rf.atomicGetGrantNum() > (livenum)/2){
				if((livenum) > len(rf.peers)/2 && rf.atomicGetGrantNum() > len(rf.peers)/2){
					fmt.Println(rf.me,"get grantNUM",rf.grantNum,"so it become L")
					rf.atomicSetLeaderSetState(rf.atomicGetMe(),STATELEADER)
					continue
				}else{
					//这其中又有什么omi 孤立 or 少数
					if((livenum) > len(rf.peers)/2){	
					}else{
						//rf.term--
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
			//rf.leader = rf.me
			rf.apply(STATELEADER)
			//并返回term
			//并没有真的直接修改状态 只发送一次心跳
			rf.sendHeartBeatAndSleepTimeout(150000000)
			continue
}

	}// no died
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
	rf.matchIndex = make([]int,len(rf.peers))
	rf.nextIndex = make([]int,len(rf.peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}


//2n+1
//挂掉N正常？
//一定要选择含有commit的那个