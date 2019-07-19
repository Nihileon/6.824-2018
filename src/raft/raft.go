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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	//Persistent state on all servers:
	currentTerm int      //latest term server has seen ,初始0，自增1
	votedFor    int         //currentTerm下投票给谁
	log         []LogEntity //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders:
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	state   int32 //raft's state
	applyCh chan ApplyMsg

	timeout   time.Duration
	heartbeat time.Duration
	timer     *time.Timer
	votedNum  int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// generate random duration(ms)
func RandDuration(min, max int) time.Duration {
	randMs := rand.Intn(max-min) + min
	return time.Duration(randMs) * time.Millisecond
}

const (
	ELECTION_TIMEOUT_MIN  = 400
	ELECTION_TIMEOUT_MAX  = 800
	HEARTBEAT_TIMEOUT_MIN = 150
	HEARTBEAT_TIMEOUT_MAX = 200
)

//情况1 某个follower没有收到leader的信息但是leader仍然存活
//情况2 某个leader挂了

//TODO 作为follower的工作： 定时检查自己是否接收到信息（通信机制的话似乎不需要？）、超时没有收到心跳：变为candidate
//TODO 作为leader的工作：定时发送心跳包给所有人
//TODO 作为candidate的工作：设置定时器并等待投票结束

//TODO 处理RPC请求：收到心跳包则重置定时器；收到投票请求则投票给对方；对于自己发起的投票，收到投票后该怎么做？
//TODO 如何知道自己收到的票是否为大部分人的投票
//TODO 如果收到了超过一半的票但是还没有成为主节点

const (
	LEADER    = int32(0)
	CANDIDATE = int32(1)
	FOLLOWER  = int32(2)
)

type LogEntity struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
	//RequestVote RPC
	Term         int // candidate's term
	CandidateId  int   // candidate requesting vote
	LastLogIndex int   //index of candidate’s last log entry
	LastLogTerm  int   // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term          int // current term, for candidate to update itself
	IsVoteGranted bool  // true means candidate received vote
}

//AppendEntries RPC
type AppendEntitiesArgs struct {
	//TODO AppendEntities
	Term         int       // leader's term
	LeaderID     int         //so follower can redirect clients
	PrevLogIndex int         //index of log entry immediately preceding new ones
	PrevLogTerm  int         //term of prevLogIndex entry
	Entries      []LogEntity //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         //leader’s commitIndex
}

type AppendEntitiesReply struct {
	Term      int //currentTerm, for leader to update itself
	IsSuccess bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d vote rpc handler %d", rf.me, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.IsVoteGranted = false
		return
	}
	//TODO 且日志至少和自己一样新
	//if rf.votedFor == -1 || rf.votedFor == args.CandidateId  {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		//TODO 如果也是candidate怎么办
		rf.state = FOLLOWER
		reply.Term = args.Term
		reply.IsVoteGranted = true


	// Your code here (2A, 2B).
}

// AppendEntities RPC handler.
func (rf *Raft) AppendEntities(args *AppendEntitiesArgs, reply *AppendEntitiesReply) {

	//TODO 附加日志
	//TODO 需要考虑任期和leader的id号(是否会在一个任期内出现多个leader
	//如果 term < currentTerm 就返回 false
	//TODO 如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
	//TODO 如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的附加日志中尚未存在的任何新条目
	//TODO 如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d's append entities handler", rf.me)
	if args.Term < rf.currentTerm {
		reply.IsSuccess = false
		reply.Term = rf.currentTerm
		return
	}
	//TODO 如果该peer内的任期小于心跳包的任期， 是否应该更新状态，还是只需要直接设置即可
	rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	//_ = args.LeaderID //TODO ID是个什么玩意
	reply.IsSuccess = true
	reply.Term = args.Term
}

func (rf *Raft) CallVote() {
	/*
		TODO:
			1. 自增Term
			2. 给自己投票
			3. 重置超时计时器
			4. 发送请求投票的 RPC 给其他所有服务器
	*/
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votedNum = 1
	rf.mu.Unlock()
	rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
	for i, _ := range rf.peers {

		if i == rf.me {
			continue
		}
		//TODO fill other 2 arguments
		args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
		if atomic.LoadInt32(&rf.state) != CANDIDATE {
			DPrintf("not a candidate? %d", rf.state)

			return
		}
		go func(server int) {
			var reply RequestVoteReply
			if atomic.LoadInt32(&rf.state) == CANDIDATE && rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.IsVoteGranted == true {
					rf.votedNum++
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.newState(FOLLOWER)
					//TODO 如果被拒绝投票，且返回了一个较大的号数
					// 甚至可能出现-1？
				}
			} else {
				println("this peer has become a leader or send RPC failed")
			}
		}(i)
	}
}

func (rf *Raft) SendHeartbeat() {
	// 如果有5个peer，其中两个正在竞争的时候，突然加进来一个follower#6，那么其中一个candidate#1认为此时有6人，而另外一个#2认为只有5人
	// #6将票投给了#1，原来的2/5投给#1，此时#1认为自己有3/6=1/2，那么#2认为自己有3/5，自己成为leader（没有问题）
	// #6将票投给了#1，原来的3/5投给#1， #1认为自己有4/6,因此成为leader（没有问题
	// 如果有6个peer，其中2个正在竞争，突然加入一个follower#7，candidate#1认为此时有7人，而#2认为有6人。
	// #7将票投给#1， 原来的3/6投给#1， #1有4/7，#2有3/6，因此#1成为leader
	// #6将票投给#1，原来的2/6投给#1， #1有3/6，#2有4/6，因此#2成为leader
	// 不存在两个同时为leader的情况出现

	//TODO 万一是由于没有收到leader发出的包而发起

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		//TODO fill other arguments ， entities is empty for heartbeat
		args := AppendEntitiesArgs{Term: rf.currentTerm, LeaderID: rf.me}

		if atomic.LoadInt32(&rf.state) != LEADER {
			return
		}
		go func(server int) {
			var reply AppendEntitiesReply

			if atomic.LoadInt32( &rf.state) == LEADER && rf.sendAppendEntities(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.IsSuccess == false && reply.Term > rf.currentTerm {
					DPrintf("there is another leader")
					rf.currentTerm = reply.Term
					rf.newState(FOLLOWER)
				}
			} else {
				println("this peer has become a follower or send RPC failed")
			}
		}(i)

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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntities(server int, args *AppendEntitiesArgs, reply *AppendEntitiesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntities", args, reply)
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
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0             //初始0， 自增1
	rf.votedFor = -1               //
	rf.log = make([]LogEntity, 0)  // 初始index 1
	rf.commitIndex = 0             //初始0
	rf.lastApplied = 0             //初始0
	rf.nextIndex = make([]int, 0)  //初始为leader的最新log index + 1
	rf.matchIndex = make([]int, 0) // 初始为0， 自增1
	rf.state = FOLLOWER            //所有raft开始都是follower
	rf.applyCh = applyCh           //
	rf.votedNum = 0                // 票数初始化为0
	rf.timer = time.NewTimer(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.StateLoop()

	return rf
}

func (rf *Raft) newState(state int32) {
	//TODO 在转变成候选人后就
	//TODO 作为follower初始时需要干什么：立即设置定时器并等待
	//TODO 作为leader初始化时：告诉所有人自己已经成为leader
	//TODO 作为candidate初始时：立即开始选举过程
	atomic.StoreInt32(&rf.state, state)
	rf.state = state
	switch atomic.LoadInt32(&rf.state) {
	case CANDIDATE:
		rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
		//TODO 立即开始选举过程
		DPrintf("%d call vote", rf.me)
		rf.CallVote()
	case FOLLOWER:
		rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
		//TODO 原子操作
		rf.votedFor = -1
		//TODO 继续进行等待
	case LEADER:
		rf.timer.Reset(RandDuration(HEARTBEAT_TIMEOUT_MIN, HEARTBEAT_TIMEOUT_MAX))
		//TODO 立即发送心跳包告诉所有人自己已经成为leader
		rf.SendHeartbeat()
	}
}

func (rf *Raft) StateLoop() {
	for {
		//TODO 控制原子操作
		switch atomic.LoadInt32(&rf.state) {
		case FOLLOWER:
			//DPrintf("follower")
			select {
			case <-rf.timer.C:
				DPrintf("follower %d timeout", rf.me)
				rf.newState(CANDIDATE)
			case msg := <-rf.applyCh:
				//TODO
				_ = msg
			default:

			}
		case LEADER:
			select {
			case <-rf.timer.C:
				DPrintf("leader %d send heart beat", rf.me)
				rf.timer.Reset(RandDuration(HEARTBEAT_TIMEOUT_MIN, HEARTBEAT_TIMEOUT_MAX))
				rf.SendHeartbeat()
			case msg := <-rf.applyCh:
				_ = msg
			default:

			}
		case CANDIDATE:
			select {
			case <-rf.timer.C:
				DPrintf("election timeout")
				rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
				//TODO 超时后重新发起投票
				DPrintf("%d call vote ", rf.me)
				rf.CallVote()
			case msg := <-rf.applyCh:
				_ = msg
			default:
				if rf.votedNum > len(rf.peers)/2 {

					DPrintf(" %d become leader", rf.me)
					rf.newState(LEADER)
				}
			}
		}

	}
}
