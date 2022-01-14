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
	if(rf.voteFor < 0){
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
	rf.voteFor = -1
	rf.vote_lock.Unlock()
}
func (rf *Raft) atomicSetLeaderSetState(leader int,state int)bool{
	ok := false
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	var currentTerm int
	currentTerm = rf.atomicGetTerm()
	//读取vote和改变vote要原子过程
	//接到不合格的candidate发起的request 不会改变原来的状态
	if(args.Term >= currentTerm ){
		if(args.LastLogIndex>=rf.commitIndex && 
			rf.atomicVoteFor(args.CandidateId)){
			reply.VoteGranted = true
			rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
			//新的term什么时候被接受
			rf.atomicSetTermIfNeed(args.Term)
			fmt.Println("C",rf.me,"vote for",args.CandidateId,"and turn to a follower")
		}
	}
	
	reply.Term = rf.atomicGetTerm()
}
//怎么错全在这里啊？非阻塞写channel 也要设置leader 也要修改对方状态
//也可以不？
//晕了 怎么把自己也改成follower了
func (rf *Raft) AppendEntry(args *AppendEntryArgs,reply *AppendEntryReply){
		//保活、探测
		//更新过term
		reply.Term = rf.atomicGetTerm()
		reply.Success = false
		//以对方为准的前提？
		//如果有一个离群的自动增加term : 满足其最大拥有entry还不如COMMIT
	//	if(args.PrevLogIndex>=rf.entries[len(rf.entries)-1].Index&&
		//args.PrevLogTerm>=rf.entries[len(rf.entries)-1].Term){
	//	if(args.Term >= rf.term){
			//ACCEPT A APPEND
		fmt.Println(rf.me,"ACCEPT A APPEND FROM",args.LeaderId)

		rf.atomicSetTermIfNeed(args.Term)
		rf.atomicSetLeader(args.LeaderId)
		fmt.Println("in APPEND ENTRY SET LD",args.LeaderId,rf.leader)
		select{
		case rf.heartbeat <- args.LeaderId:
			break
		default:
			break //do not access 过时的心跳
		}

		rf.commitIndex = args.LeaderCommit
	//	rf.leaderLastApplied = args.LeaderApply
	
	
		if(rf.leader==rf.me){
			//不需要这样检查 因为leader一开始成为leader也需要设置
			reply.Success = true
			return
			}
		//=========== setleader term commit工作=======

		if(len(args.Entries) == 0){
			reply.Success = true
			return
		}
	
		
		//旧的leader设置了commit 但是新的leader没有收到 
		//commit应该按照新的leader(更小的来)
		//或者follower还没有收到commit标号的entry 但毕竟这些被多数server收到了
		//还是说直接听信呢
	


		//收到的args.Entries[0].Index开始 如果有 可能是旧的 全部删掉！
		//得到的dele最小值是0
		//follower的本地log应该到match[server]都是一致的
		//一致包含index和term 都一致 那么leader既然发来了 就是它觉得
		//server有一些不match的 无论如何都要删掉 然后重写leader给的版本
		
		dele := len(rf.entries) - 1 //以args.Eentries的版本为基础 
		//如果和发来的版本矛盾（已经“新发来”的循环里）
		//弄一个新发来的index - term map把
		leaderEdition := make(map[int]int,len(args.Entries))
		for _,entry := range args.Entries {
			leaderEdition[entry.Index] = entry.Term
		}

		for dele > 0  {
			if( rf.entries[dele].Index > args.LeaderCommit && 
				rf.entries[dele].Term!=args.Term){
					rf.entries = rf.entries[0:len(rf.entries)-1]
				}else{
				 break
				}
					dele--
			}
		 //break将来到这里 dele作为新的边界
	
		/*
			Term int //leader’s term
			LeaderId int //so follower can redirect clients
			PrevLogIndex int //index of log entry immediately preceding
						//new ones
			PrevLogTerm int //term of prevLogIndex entry
			Entries []int 	//log entries to store (empty for heartbeat;
				//may send more than one for efficiency)
			LeaderCommit int //leader’s commitIndex
		*/
		//就是一些常规的工作在上面

		//begin index 
		begin := 0
		recieve := args.PrevLogIndex - len(args.Entries)
		highestEntry := rf.atomicGetLastEntry()

		if(highestEntry.Index == recieve){
			//last+1 prev
			goto write
		}else{
			if(highestEntry.Index < recieve){
				//wait old come
				reply.Success = false
				return
			}else{
				//begin 下标 -> begin = highest + 1
				
				for begin <= (len(args.Entries) - 1) {
				//	fmt.Println("begin:",begin)
					if(args.Entries[begin].Index == highestEntry.Index + 1){
						break
					}else{
						begin ++
					}
				}
				if(begin == len(args.Entries)) {return} //发来的全是旧的
				goto write
			}
		}

		//last+1 pre
		write:
		//TOatomic	
		for begin <= (len(args.Entries) -1 ){
			rf.entries = append(rf.entries,args.Entries[begin])
			begin++
		}
		fmt.Println(rf.me,"writeok",args.PrevLogIndex)
		reply.Success = true
	//}else{
	//	fmt.Println(rf.me,"state is",rf.state,"reject a AppendEntry from",args.LeaderId)
//	}
		//旧的leader怎么知道自己不是leader？
		//那么这个旧的leader也会受到新的leader的LeaderID 
		//在ticker循环中会发现自己并非leader 并转换到follower状态

		
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
//
func (rf *Raft) sendDetectLives(t int)int{
	//invoke intime func
		args := AppendEntryArgs{}
		reply := AppendEntryReply{}
		reply.Term = -1

		lives := []bool{}
		for index,_ := range rf.peers{
			//in 50ms if return append
			go rf.sendAppendEntryInTime(&lives,index,&args,&reply)
		}
	time.Sleep(time.Duration(t))
	return len(lives)
}
func (rf *Raft) sendRequestVote(done chan bool,server int, args *RequestVoteArgs, reply *RequestVoteReply)  {
	args.Term = rf.atomicGetTerm()
	args.CandidateId = rf.atomicGetMe()
	reply.VoteGranted = false
	//我想的是 如果不够新 就不能当选新的leader（去回应client）
	//如果commit已经更新了 但并没有存下已经被commit的entries
	//不能当选leader
	//如果commit还没有更新 但是已经存下 会重新apply已经apply过的
	//appply也是如此
	//必然是存了才有commit
	args.LastLogIndex = rf.commitIndex
	if(rf.entries[len(rf.entries)-1].Index < args.LastLogIndex){
		args.LastLogIndex = rf.entries[len(rf.entries)-1].Index
	}
	args.LastLogTerm = rf.entries[len(rf.entries)-1].Term
	rf.peers[server].Call("Raft.RequestVote", args, reply)
	if(reply.VoteGranted){
		done <- true
	}else{
		done <- false
	}
	
}
//detect live
func (rf *Raft) sendRequestVoteInTime(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	reply.Term = -1
	done := make(chan bool,1)
	timeout := time.NewTimer(time.Duration(50000000))
	ok := false
	go rf.sendRequestVote(done,server,args,reply)
	
		select{
			case <-timeout.C:
				break
			case ok = <-done:
				break
		}
	
	//fmt.Println(server,time.Now().UnixNano())
	//这个超时会返回吗？
	//对方的RequestVote服务 自增自己的grantNUm
	if(ok){
		rf.atomicIncGrantNum()
		//fmt.Println("C",rf.me,"got vote num",rf.grantNum)
	}
	if(reply.Term < 0){
		//自己断网啦？
	//	fmt.Println(rf.me,"disconnect from" ,server)
	}else{
	//	fmt.Println(rf.me,"connect from" ,server)
	}
	if(rf.atomicSetTermIfNeed(reply.Term)){
		rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
	}
	return ok
}
//现在的逻辑是心跳携带entry 都要设置leader 那么args被填上leader的位置: tick循环 infi循环
func (rf *Raft) sendAppendEntry(done chan bool,server int,args *AppendEntryArgs,reply *AppendEntryReply) bool {
	//fmt.Println("leader send",args.PrevLogIndex," COMMIT",args.LeaderCommit)
	//leader 为什么出错
	args.LeaderId = rf.me
	ok := rf.peers[server].Call("Raft.AppendEntry",args,reply)
	rf.atomicSetTermIfNeed(reply.Term)
	
	done <- reply.Success
	return ok
}
func(rf *Raft) sendAppendEntryInTime(lives *[]bool,server int,args *AppendEntryArgs,reply *AppendEntryReply)bool{

	//限时等待
	
	done := make(chan bool,1)

	ok := false
	go rf.sendAppendEntry(done,server,args,reply)
	time.Sleep(time.Duration(50000000))
	select{
	case ok = <-done://成功失败都是探测到live
		*lives = append(*lives,ok)
			break
	default:break
	}
	return ok //是否连通
}
func(rf *Raft) sendHeartBeatAndSleep50ms(args *AppendEntryArgs,reply *AppendEntryReply)bool{
	//限时等待
	//并行发送心跳
	lives := []bool{}
	for index,_ := range rf.peers{
		go rf.sendAppendEntryInTime(&lives,index,args,reply)
	}
	time.Sleep(time.Duration(50000000))
	flt := 0
	for _,ans:= range lives{
		if(ans){
			flt++
		}
	}
	fmt.Println("WTFFFFFFgrantLD",flt)
	return flt > 1
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
		go rf.infinitelySendToPeers(command,index,term)} //成功就结束 是不是要延迟开始？
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
	return rf.entries[len(rf.entries) - 1].Index + 1
}
func (rf *Raft) infinitelySendToPeers(command interface{},index int,term int){
	//初始化
	try := 1
	resend := []int{}

	for index,_ := range rf.peers {
		resend = append(resend,index)
	}
	again:
	fail := make(map[int]int)
	for _,s:=range resend{
		fail[s] = s
	}
	//如果联系不上部分server majority怎么算？还是算活着的部分
	//在heartbeat里更新Live数
	//if live < 1 be an old leader
	//在heart beat 里
	if(rf.atomicGetLeader()!=rf.atomicGetMe()){
		fmt.Println("me",rf.me,"leader",rf.leader)
		fmt.Println("TOO many disconnect")
		return
	}
	
	//成功过半 如何定义？
		if(len(rf.peers) - len(resend) > (len(rf.peers)/2)){
		//fmt.Println("majority record entry[",index,"]")
		if(index > rf.commitIndex){
			rf.commitIndex = index
			fmt.Println("leader commit",rf.commitIndex)

		}
	}
	//fmt.Println("send entry[",index,"] round ",try)
	try++
	for _,server := range resend{
		//fmt.Println("send",index," to ",server,"time:",try)
		//go 等待一段时间的AppendEntry调用
		//同时传入一个容器 如果没有处理完 或者失败了 把失败的peer放到容器
		//换言之 传入resend 如果在规定时间内 返回且成功 就不做操作 否则添加server（原peers中的下标）到sclice中
		//resend以地址的形势调用
		//算了 返回一个新的吧
	/*
			Term int //leader’s term
			LeaderId int //so follower can redirect clients
			PrevLogIndex int //index of log entry immediately preceding
						//new ones
			PrevLogTerm int //term of prevLogIndex entry
			Entries []int 	//log entries to store (empty for heartbeat;
				//may send more than one for efficiency)
			LeaderCommit int //leader’s commitIndex
		*/
		//要考虑到在AppendEntry里做哪些事情  Args要写全
		args := AppendEntryArgs{}
		args.LeaderCommit = rf.atomicGetCommit()
		args.Term = rf.atomicGetTerm()
		//leader自己设置自己是leader
		args.LeaderId = rf.leader //leader莫名其妙会变成0我真的很害怕
		//prevLogTerm是最新收到的
		args.PrevLogIndex = index
		args.PrevLogTerm = term
		//TOatomic
		i := rf.nextIndex[server]
		if(i > index){
			continue
			//leader自己
		}
	//	fmt.Println("send entry",i,"~",index,"to server",server)
		for i <= index{
			//TOatomic
			//记录发来的entry
			//是client发来的
			args.Entries = append(args.Entries,rf.entries[i])
			i++
		}
		
		reply := AppendEntryReply{}
		reply.Term = -1
		reply.Success = false
		//record fail and success !match
		//实际上是20ms
		go rf.AppendEntryIn50msRecordFail(&fail,server,&args,&reply)
	}
	//wait 50ms 
	//把这个去掉以后 不会有那么多失败 就是说client提交是很快的
	//但为什么wait的话就会失败很多
	//sleep的话 查看失败数量的时候 已经全部完成 有没有在end那里commit

	//但是去掉sleep 就恰好来到majority那里//有一个陷阱 一定等时间够了才
	//积累了足够的fail 因为 不到50ms 视作没有fail
	//所以这个设计有点问题哈 应该是成功就从重做列表里划掉……
	time.Sleep(time.Duration(50000000))
	if(len(fail) > 0) {
	//	fmt.Println("resend",index,"to",fail)
		resend = []int{}
		for _,f :=range fail{
			resend =append(resend,f)
		}
//	time.Sleep(time.Second)
		goto again
	} else{
	//	fmt.Println(index,"之前 all send")
		/*之前忘了这里*/
		if(index > rf.commitIndex){
			rf.commitIndex = index
			fmt.Println("FOUND NO FAIL SO UPDATE COMMIT")
		//	fmt.Println("leader更新commit",rf.commitIndex)
		}
		
	}

}
func(rf *Raft) AppendEntryIn50msRecordFail(fail *map[int]int,server int,args *AppendEntryArgs,reply *AppendEntryReply)bool{

	//限时等待
	done := make(chan bool,1)

	ok := false
		//will send change next
	//TOatomic
	oldnext := rf.nextIndex[server]
	rf.nextIndex[server] = args.PrevLogIndex + 1
	
	go rf.sendAppendEntry(done,server,args,reply)
	time.Sleep(time.Duration(50000000))
	select{
	case ok = <-done:
			break
	default:
		break		
	}
	//下一层函数里处理了reply
	if(ok){
		delete(*fail,server)
		//一次失败 会导致一次重发 那么在nextIndex被修改前不要重新组装发送内容
		//如果发送多次都失败了 那么nextIndex应该只递减一次
		rf.nextIndex[server] = oldnext
		//可能优化：按照follower回报的立刻重发
		//fmt.Println("AppendEntry ",args.PrevLogIndex,"failed on",server)
	}else{
		//TOatomic
		if(rf.matchIndex[server] < args.PrevLogIndex){
			rf.matchIndex[server] = args.PrevLogIndex
		//	fmt.Println("match[",server,"]",rf.matchIndex[server])
		}
	}
	return ok //是否连通
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
	for true {
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
		fmt.Println("leader",rf.leader,rf.me,"apply","index:",apl.CommandIndex,"and nonw comm",rf.commitIndex)
	}


	//wait
//	fmt.Println(rf.me,"wait_apply")
	time.Sleep(time.Duration(50000000))
}
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
		if(rf.state == STATEFOLLOWER){
			go rf.apply(STATEFOLLOWER) //为什么follower不apply呢？
		//	go rf.checkApply(STATEFOLLOWER)
			rf.atomicClearVote()
			rand.Seed(time.Now().UnixNano())
			randTime := rand.Intn(150)
			time.Sleep(time.Duration(150000000+randTime*1000000)) //150~300ms
			//candidatetimeout + heartbeat时间如果 > 两个followersleep只差
			//则明明已经有了leader却没有发现
			
				if rf.atomicGetLeader() >= 0 {
				
					//fmt.Println("term",rf.term,"F",rf.me,"know leader is",rf.leader)
					select{
					case <-rf.heartbeat:
					break
					//	fmt.Println("latest entry is ",rf.entries[len(rf.entries)-1].Index,"and recieve hb from leader ",rf.leader,"now apply",rf.lastApplied,"commit",rf.commitIndex)
						
					default:
					//	fmt.Println("but no hb come so F",rf.me,"turn to C")
						rf.atomicSetLeaderSetState(-1,STATECANDIDATE)
					}
					}else{
					//leader < 0
					//fmt.Println("term",rf.term,"F",rf.me,"find leader dead")
					rf.atomicSetLeaderSetState(-1,STATECANDIDATE)
					continue //重新进入循环
				}
		}else if rf.state == STATECANDIDATE{
			
			//检查<0和改变应该原子地完成 否则可能会投出多个leader
			if(rf.atomicVoteFor(rf.atomicGetMe())){
				rf.term_lock.Lock()
				rf.term++
				rf.term_lock.Unlock()
				//在发起投票之前应该把自己的票数清零 如果此时已经投给别人 是否应该在
				//follower状态的循环里  但没有检查哇 就还是candidate 停留一段时间
				rf.grant_lock.Lock()
				rf.grantNum = 0
				rf.grant_lock.Unlock()
				fmt.Println("term",rf.term,"C",rf.me,"vote for it self")
				for candi,_ := range rf.peers{
				args := RequestVoteArgs{}

				reply := RequestVoteReply{}
		
				//在send内部计算grantNum
				go rf.sendRequestVoteInTime(candi,&args,&reply)
				
				//在send函数中可能会改变term从而进入follower状态 状态一旦变为follower 再次进入状态循环
				//不会来到Candidate 分支了
			}}else{
				//已经投过票
					rf.vote_lock.Lock()
					voteFor := rf.voteFor
					rf.vote_lock.Unlock()
					if(voteFor == rf.atomicGetMe()){
						//寻求vote的时候不应该等待太久 如果一个send没有得到回应 应该视作 对方的拒绝
						fmt.Println("C",rf.me," wil detect live...")
						//最多50ms确定 在函数中sleep随机
						live := rf.sendDetectLives(50000000)
						//TOatomic
						rf.live = live
						//live 包括自己嘛？包括的
						//live 满足什么要求可以组成一个群组？
						
					//	time.Sleep(time.Duration(50000000))

						//if(live > 1 && (live) > len(rf.peers)/2 && rf.atomicGetGrantNum()+1 > (live)/2){
						if(live > 1 && (live) > len(rf.peers)/2 && rf.atomicGetGrantNum()+1 > (live)/2){
							//fmt.Println("so it become L")
						//也可以只设置leader状态 不设置编号
							rf.atomicSetLeaderSetState(rf.atomicGetMe(),STATELEADER)
							continue
						}else{
							fmt.Println("so",rf.me,"it turn to F")
							rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
							continue
						}
					}else{
						rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
							continue
					}
				}
		}else if rf.state == STATELEADER{
			rf.atomicClearVote()
			//先补上可以吗 、摁
			//rf.applyonce(STATELEADER)
			go rf.apply(STATELEADER)
			//go rf.checkApply(STATELEADER)
			//TOatomic
			//存档啊
			fmt.Println(rf.me,"bealeader","comm",rf.commitIndex)
			
				heartbeat := AppendEntryArgs{}
				heartbeat.Term = rf.atomicGetTerm()
				heartbeat.LeaderId = rf.me
				heartbeat.LeaderCommit = rf.commitIndex
				e := rf.atomicGetLastEntry()
				heartbeat.PrevLogTerm = e.Term
				heartbeat.PrevLogIndex = e.Index
				reply := AppendEntryReply{}
				//并返回term
				//并没有真的直接修改状态
				rf.sendHeartBeatAndSleep50ms(&heartbeat,&reply)
			//	if(!rf.sendHeartBeatAndSleep50msIfNoReplyTurnToFollower(&heartbeat,&reply)){
				//	fmt.Println(rf.me,"HB FAIL")
				//	rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
				//}

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
	dumb.Term = -1
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
