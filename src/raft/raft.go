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


	commitIndex int //??????leader???????????? ??????????????????peer
	lastApplied int //????????????apply ?????????????????????
	commit_lock sync.Mutex
	apply_lock sync.Mutex
//Volatile state on leaders:

	nextIndex []int //leader??????match?????? ????????????????????????match ????????????????????????
	matchIndex []int 

	entries []Entry
	entry_lock sync.Mutex
	leaderLastApplied int

	applyCh chan ApplyMsg //??????????????????????????????
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
	Term int //candidate???s term
	CandidateId int//candidate requesting vote
	CommitIndex int
	LastLogIndex int //index of candidate???s last log entry (??5.4)
	LastLogTerm int//term of candidate???s last log entry (??5.4)
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
	Term int //leader???s term
	LeaderId int //so follower can redirect clients
	PrevLogIndex int //index of log entry immediately preceding
						//new ones
	PrevLogTerm int //term of prevLogIndex entry
	Entries []Entry 	//log entries to store (empty for heartbeat;
				//may send more than one for efficiency)
	LeaderCommit int //leader???s commitIndex

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
		//leader - 1 ->??????
		if(state == STATEFOLLOWER && rf.leader < 0 && leader >=0 ){
			clear = true
		}
		//leader - 1 -> -1 ???????????? ????????????????????????Candidate
	}else if(rf.state == STATELEADER && state == STATEFOLLOWER){
		//fmt.Println("L -> F  follow ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
		clear = true
	}else if(rf.state == STATEFOLLOWER && state == STATECANDIDATE){
		//????????????????????????????????????term 
		//?????????????????????????????????????????????candidate????????????????????????term ??????????????????HB
		//Commit????????????
	//	fmt.Println("F->C commit",rf.commitIndex)
	//fmt.Println("F -> C  candi   ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
		clear = true
	}else if(rf.state == STATECANDIDATE && state == STATEFOLLOWER){
		//fmt.Println("C -> F  follow ",rf.me,"term",rf.term,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
		//??????leader???-1?????????????????????
		//leader????????????????????????
		//?????????????????????????????????????????????????????????
		/*
		??????vote

		???vote ??????????????????
		*/
		//??????
		if(leader == -1){
			//???????????????????????????
			//???????????? dengdaileader
			if(rf.voteFor == rf.me){
				clear = true
	//			fmt.Println("C -> F  remake ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
			}
			//??????????????? ????????????
		}else if(leader >= 0){
			//????????????
			clear = true
	//		fmt.Println("C -> F  waitHB ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
	
		}	
	}else if(rf.state == STATECANDIDATE && state == STATELEADER){
		clear = true
		//rf.clearStaleEntry()
		//?????????????????? ?????????0
		//?????????????????????????????? ?????????commit ???????????????????????????
	//	fmt.Println("C -> L  leader ",rf.me,"latest",rf.entries[len(rf.entries)-1].Index,"[",rf.entries[len(rf.entries)-1].Term,"]","commit",rf.commitIndex)	
		rf.matchIndex = make([]int,len(rf.peers))
		rf.nextIndex = make([]int,len(rf.peers))
		for index,_ := range rf.nextIndex{
			rf.nextIndex[index] = 1
		}
		rf.matchIndex[rf.me] = rf.atomicGetLastEntry().Index
		rf.nextIndex[rf.me] = rf.atomicGenerateIndex()
	}else{
		//????????????????????????
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
//too many disconnect ??????commit????????????
//LastLogIndex:?????????commit?????????last ?????????commit?????????????????????????????????
//Leader?????????Follower
//LEADER??????????????????follower???
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.term
	reply.VoteGranted = false
	
	//??????vote?????????vote???????????????
	//??????????????????candidate?????????request ???????????????????????????
	//commit???????????????????????? ???????????????commit???????????????
	//?????????????????????????????????????????????commit ??????follower???log??????????????????1/2
	//????????????log????????????commit
	//????????????Term???????????? LastLogTerm??????>= LastLogIndex???????????????????????????????????????term????????????Index
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
		//?????? turn follower
		reply.Success = false
		return
	}
	if(rf.atomicGetMe()==args.LeaderId){
		reply.Success = true
		//reply.Want = rf.atomicGenerateIndex()
		return
	}

	rf.atomicSetTermIfNeed(args.Term)
	//??????leader?????????????????????follower??????
	if rf.leader < 0 {
		rf.atomicSetLeaderSetState(args.LeaderId,STATEFOLLOWER)
	}
	//HB COME		
	select{
	case rf.heartbeat <- args.LeaderId:
		break
	default:
		break //do not access ???????????????
	}
	//?????????map????????????????????????
	if(rf.entries[len(rf.entries) - 1].Index < args.PrevLogIndex || rf.entries[args.PrevLogIndex].Term != args.PrevLogTerm){
		//????????????
				if(len(rf.entries) <= args.PrevLogIndex){
					//??????????????????
					reply.Success = false
					return
				}else{
					rf.entries = rf.entries[0:args.PrevLogIndex]
				}
	}
	
	//CHECK VERSION
	copy_index := 0
	
	if(len(args.Entries) == 0){
		//??????commit????????????????????????
		reply.Success = true		
		if(args.LeaderCommit <= args.PrevLogIndex){
			rf.commitIndex = args.LeaderCommit}
		return
	}
	//?????? 0[0]
	//?????? P0[0] entries{0[0]}
	//?????? entries{0[0]} ???????????????
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
			//but may repeat!!!???????????????????????????????????????
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
	//???????????????commit????????????????????? ?????????????????????????????????commit???????????????
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
//DETECT LIVE?????????AppendEntry??????????????????leader?????????RequestVote?????????
func (rf *Raft) sendDetectLives(t int)int{
		args := RequestVoteArgs{}
		reply := RequestVoteReply{}
		args.Term = -1
		live := []int{}
		consume := len(rf.peers)
		for index,_ := range rf.peers{
			//lives??????????????????????????????
			go rf.sendRequestVoteInTimeout(t,&live,&consume,index,&args,&reply)
		}
	time.Sleep(time.Duration(t))
	for consume > 0{
		//	????????????????????????
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
//?????????????????? ??????timeout?????????????????????????????????
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
//??????????????????????????????entry ????????????leader ??????args?????????leader?????????: tick?????? infi??????
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

//command?????????index term entry?????? index term??????leader?????????
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	var index int
	var term int 
	var isLeader bool
	isLeader = false
	index = -1
	term = -1
	if(rf.atomicGetLeader() == rf.atomicGetMe()){
		//fmt.Println(rf.me,"im leader i ??????")
		isLeader = true
		index = rf.atomicGenerateIndex()
		//fmt.Println("leader give index",index)
		term = rf.atomicGetTerm()
		if(rf.saveEntryToLeader(command,index)){
		//go rf.infinitelySendToPeersEveryTimeout(50000000,command,index,term)
		//rf.sendToPeersOnce(50000000)
		} //??????????????? ???????????????????????????
	}
	return index, term, isLeader
}
func (rf *Raft)saveEntryToLeader(command interface{},index int) bool{
	save := false
	if(rf.atomicGenerateIndex() == index){
		entry := Entry{}
		entry.Index = index
		entry.Command = command //?????????Command
		entry.Term = rf.atomicGetTerm() //?????????leader???term
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
//too many RPC bytes????????? ????????????????????????
//?????????????????? ????????????????????????????????????
func (rf *Raft) sendToPeersOnce(timeout int){
	if(rf.state!=STATELEADER){return}
	last := rf.atomicGetLastEntry().Index //????????????
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
			//?????????commit ?????????????????????leader???????????????
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
// 	//biger term ??????
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
		//????????????commit?????????????????????
		if(last < len(rf.entries) && rf.entries[last].Term >= rf.term){
			//figure 8
			//fmt.Println("f8")
				rf.commitIndex = last
		}
				//fmt.Println("success")
	}else{
		//fmt.Println("ack??????")
	}
	
	 if(livenum <=  len(rf.peers)/2) {
		// fmt.Println("live ??????")
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
// 		fmt.Println("leader ?????? + notcommit??????")
// 		rf.atomicSetLeaderSetState(-1,STATEFOLLOWER)
// 	}
}
//???HB?????????
func(rf *Raft) AppendEntryInTimeoutRecordFail(live *[]int,timeout int,consume *int,server int,args *AppendEntryArgs,reply *AppendEntryReply){
	done := make(chan int,1)
	 //????????????????????????
	go rf.sendAppendEntry(done,server,args,reply)
	//???????????????log????????????next
	if(len(args.Entries) > 0){
	rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
	}
	//????????????
	time.Sleep(time.Duration(timeout))
	select{
	case <-done:
		if(reply.Success){
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		*live = append(*live,1)
		}else{
			//??????next?????????
			//???????????????????????????????????????
			if(rf.matchIndex[server] > 0){
			rf.matchIndex[server]--}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		//	fmt.Println("not match")
		    *live = append(*live,0)
		}
			break
	default:
		//??????next????????? ??????????
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
		//Figure8???????????????
		//term2:leader1 :2[2] ?????????????????? ???commit
		//term3:leader5 :2[3]
		//term4:leader1 ??????2[2]??????????????? ??????commit??????????????????
		//term5:????????????????????????2[2] ?????????????????????2[2]?????????
		//???worker5????????????leader[????????????log ??????term???????????????]
		//????????????2[2]??????term4???commit?????????????????????2[2]??????????????????
		//??????commit???term???????????????????????????????????? ???term???log?????????????????? ???????????????term???log??????
		//?????????????????????????????????????????????term??? ?????????commit??????????????????
		apl := ApplyMsg{}
		apl.CommandValid = true
		apl.CommandIndex = rf.entries[applyindex].Index
		apl.Command = rf.entries[applyindex].Command

		rf.applyCh <- apl
		//fmt.Println("leader",rf.leader,rf.me,"apply",apl.CommandIndex,"[",rf.entries[applyindex].Term,"]","and nonw comm",rf.commitIndex,"term",rf.term,rf.entries)
		applyindex++ //???apply?????????
		rf.lastApplied++ //????????? ????????????applyindex???1
		//???????????????????????????????????????????????????????????????????????????
	}
	//?????????entry ?????????commit ???????????????commit???????????????????????????lastapply??????
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
	
	//else ???????????????
	//match???commit?????????????????????
	rf.nextIndex[rf.atomicGetMe()] = rf.entries[len(rf.entries)-1].Index+1
	rf.matchIndex[rf.atomicGetMe()] = rf.entries[len(rf.entries)-1].Index
	
	
	//?????????commit?????????????????????????????????
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
		//FOLLOWER?????? voteFor??????????????????
		//FOLLOWER??????????????????????????????????????????leader(???????????????????????? ??????setState???clearVote & grantNum)
		//?????????????????????????????? ???Follower?????????????????????
		//?????????????????????????????????????????????????????? ?????????live????????????????????????term ????????????????????????????????????????????????term
			//?????????????????????????????????
		if(rf.state == STATEFOLLOWER){
			rf.apply(STATEFOLLOWER)
			rand.Seed(time.Now().UnixNano())
			randTime := rand.Intn(150)
			time.Sleep(time.Duration(100000000+randTime*1000000)) 
			//candidatetimeout + heartbeat???????????? > ??????followersleep??????
			//?????????????????????leader???????????????
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
					continue //??????????????????
				}
		}else if rf.state == STATECANDIDATE{
			//???FOLLOWER???????????? voteFor????????????
			//??????<0?????????????????????????????? ???????????????????????????leader
			if(rf.atomicVoteFor(rf.atomicGetMe())){
			//	add := rand.Intn(len(rf.peers))
				rf.term_lock.Lock()
				rf.term ++
				rf.term_lock.Unlock()
			//	fmt.Println("term",rf.term,"C",rf.me,"commit",rf.commitIndex)
				//?????????????????????&??????????????????
				live := []int{}
				consume := len(rf.peers)
				for candi,_ := range rf.peers{
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				//??????sendRequestVote?????????grantNum
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
					//?????????????????????omi ?????? or ??????
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
			rf.sendToPeersOnce(50000000)
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
	rf.applyCh = applyCh //??????????????? ????????????????????????
	rf.entries = []Entry{}
	dumb := Entry{}
	dumb.Term = 0
	dumb.Index = 0 //??????
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
//??????N?????????
//?????????????????????commit?????????