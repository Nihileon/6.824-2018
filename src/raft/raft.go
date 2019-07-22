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
    "bytes"
    "labgob"
    log2 "log"
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
    currentTerm     int         //latest term server has seen ,初始0，自增1
    currentLeaderID int         //latest leader server id
    votedFor        int         //currentTerm下投票给谁
    log             []LogEntity //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

    //Volatile state on all servers:
    commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
    lastApplied int //index of highest log entry applied to state machine (永远小于commit)(initialized to 0, increases monotonically)

    //Volatile state on leaders:
    nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

    state   int32 //raft's state
    applyCh chan ApplyMsg

    timer      *time.Timer
    votedCount int

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
}

func RandDuration(min, max int) time.Duration {
    randMs := rand.Intn(max-min) + min
    return time.Duration(randMs) * time.Millisecond
}

const (
    ELECTION_TIMEOUT_MIN  = 200
    ELECTION_TIMEOUT_MAX  = 400
    HEARTBEAT_TIMEOUT_MIN = 50
    HEARTBEAT_TIMEOUT_MAX = 80
)

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
    state := atomic.LoadInt32(&rf.state)
    return rf.currentTerm, state == LEADER
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
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.log)
    e.Encode(rf.currentTerm)
    rf.persister.SaveRaftState(w.Bytes())
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
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var log []LogEntity
    var currentTerm int
    if d.Decode(&log) != nil ||
        d.Decode(&currentTerm) != nil {
        log2.Fatal("decode fail")
    }
    rf.log = log
    rf.currentTerm = currentTerm
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    //RequestVote RPC
    Term         int // candidate's term
    CandidateId  int // candidate requesting vote
    LastLogIndex int //index of candidate’s last log entry 应该是当前日志的长度，而不是已提交的日志的长度
    LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term          int  // current term, for candidate to update itself
    IsVoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    DPrintf("%d reply %d's vote request", rf.me, args.CandidateId)
    DPrintf("candidate's term:%d, logIndex:%d, follower's term:%d, logIndex:%d, votefor:%d", args.Term, args.LastLogIndex, rf.currentTerm, len(rf.log)-1, rf.votedFor)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    //DPrintf("%d vote rpc handler %d", rf.me, args.CandidateId)
    logIndex := len(rf.log) - 1
    reply.IsVoteGranted = false
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return
    } else if args.Term == rf.currentTerm {
        DPrintf("%d.currentTerm == %d.term", rf.me, args.CandidateId)
        reply.Term = args.Term
        if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
            rf.votedFor = args.CandidateId
            reply.IsVoteGranted = true
            atomic.StoreInt32(&rf.state, FOLLOWER)
            rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
            DPrintf("%d agree %d's vote request", rf.me, args.CandidateId)

        }
    } else {
        reply.Term = args.Term
        reply.IsVoteGranted = true
        rf.currentTerm = args.Term
        rf.persist()
        rf.votedFor = args.CandidateId
        atomic.StoreInt32(&rf.state, FOLLOWER)
        rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
        DPrintf("%d agree %d's vote request", rf.me, args.CandidateId)
    }

    if args.LastLogTerm < rf.log[logIndex].Term ||
        (args.LastLogTerm == rf.log[logIndex].Term && args.LastLogIndex < logIndex) {
        reply.IsVoteGranted = false
        return
    }
    //}
    // Your code here (2A, 2B).
}

func (rf *Raft) initIndex() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.matchIndex = make([]int, len(rf.peers))
    rf.nextIndex = make([]int, len(rf.peers))
    for i := 0; i < len(rf.peers); i++ {
        rf.matchIndex[i] = len(rf.log) -1
        rf.nextIndex[i] = len(rf.log)
    }
}

func (rf *Raft) CallVote() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    rf.currentTerm++
    rf.persist()
    rf.votedFor = rf.me
    rf.votedCount = 1
    rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
    for i, _ := range rf.peers {
        if i == rf.me {
            continue
        }

        args := RequestVoteArgs{
            Term:         rf.currentTerm,
            CandidateId:  rf.me,
            LastLogTerm:  rf.log[len(rf.log)-1].Term,
            LastLogIndex: rf.log[len(rf.log)-1].Index}

        if atomic.LoadInt32(&rf.state) != CANDIDATE {
            return
        }
        go func(server int) {
            var reply RequestVoteReply
            isOK := rf.sendRequestVote(server, &args, &reply)
            if atomic.LoadInt32(&rf.state) == CANDIDATE && isOK {
                rf.mu.Lock()
                defer rf.mu.Unlock()
                if reply.IsVoteGranted == true {
                    rf.votedCount++
                } else if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.persist()
                    rf.newState(FOLLOWER)
                }
            } else {
            }
        }(i)
    }
}

//AppendEntries RPC
type AppendEntitiesArgs struct {
    Term         int         // leader's term
    LeaderID     int         //so follower can redirect clients
    PrevLogIndex int         //index of log entry immediately preceding new ones PrevLogIndex应该是之前领导者发送过去的日志的最后一条的索引，不是跟随者日志的长度，也不是领导者日志的长度
    PrevLogTerm  int         //term of prevLogIndex entry
    Entities     []LogEntity //log entries to store (empty for heartbeat; may send more than one for efficiency)
    LeaderCommit int         //leader’s commitIndex
}

type AppendEntitiesReply struct {
    Term        int  //currentTerm, for leader to update itself
    IsSuccess   bool // true if follower contained entry matching prevLogIndex and prevLogTerm
    BackToIndex int  //需要回退并发给follower的index（闭区间
}

func Max(a int, b int) int {
    if a > b {
        return a
    }
    return b
}

func Min(a int, b int) int {
    if a > b {
        return b
    }
    return a
}

// AppendEntities RPC handler.
func (rf *Raft) AppendEntities(args *AppendEntitiesArgs, reply *AppendEntitiesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.IsSuccess = false
        return
    } // else args.Term >= rf.currentTerm

    reply.Term = args.Term
    rf.currentTerm = args.Term
    rf.persist()
    rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
    DPrintf("%d reset timeout", rf.me)
    rf.currentLeaderID = args.LeaderID
    if args.Term > rf.currentTerm {
        rf.newState(FOLLOWER)
    } else {
        atomic.StoreInt32(&rf.state, FOLLOWER)
    }
    DPrintf("%d handle %d's heartbeat,args.term=%d, rf.term=%d", rf.me, args.LeaderID, args.Term, rf.currentTerm)
    if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        //此处表明了backIndex一定会在prevLogIndex之下
        backToIndex := Min(len(rf.log)-1, args.PrevLogIndex)
        conflictTerm := rf.log[backToIndex].Term
        //已经commit的是表明绝对会存在而且不会变化的，而跳回到上一个不冲突的周期也需要
        for ; backToIndex > rf.commitIndex && rf.log[backToIndex].Term == conflictTerm; backToIndex-- {
        }
        // backToIndex是第一个不冲突的索引
        reply.BackToIndex = backToIndex
        reply.IsSuccess = false
        return
    } // else args.PrevLogIndex <= len(rf.log)-1 && rf.log[args.prevLogIndex].Term == args.PrevLogTerm

    reply.IsSuccess = true
    var i int
    prevLogIndex := args.PrevLogIndex

    for i = 0; i < len(args.Entities) && i < len(rf.log)-1-prevLogIndex; i++ {
        if rf.log[prevLogIndex+i+1].Index != args.Entities[i].Index ||
            rf.log[prevLogIndex+i+1].Term != args.Entities[i].Term {
            break
        }
    }

    rf.log = append(rf.log[:prevLogIndex+i+1], args.Entities[i:]...)

    rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
    DPrintf("%d current commit index:%d, leader's commit index:%d", rf.me, rf.commitIndex, args.LeaderCommit)
    go rf.applyLog()
    rf.persist()
}

func (rf *Raft) SendEntities() {
    DPrintf("current log num:%d", len(rf.log))
    //注意发送方一定是leader， leader一定不会覆盖自己的记录
    for i, _ := range rf.peers {
        if i == rf.me {
            continue
        }
        if atomic.LoadInt32(&rf.state) != LEADER {
            return
        }
        go rf.sendEntitiesLoop(i)
    }
}

func (rf *Raft) sendEntitiesLoop(server int) {
    DPrintf("%d send heartbeat to %d", rf.me, server)
    rf.mu.Lock()
    DPrintf("match index %d, log len:%d", rf.matchIndex[server], len(rf.log))
    args := AppendEntitiesArgs{
        Term:         rf.currentTerm,
        LeaderID:     rf.me,
        PrevLogIndex: rf.matchIndex[server],
        PrevLogTerm:  rf.log[rf.matchIndex[server]].Term,
        Entities:     rf.log[rf.matchIndex[server]+1:],
        LeaderCommit: rf.commitIndex}
    rf.mu.Unlock()
    var reply AppendEntitiesReply
    for ; atomic.LoadInt32(&rf.state) == LEADER; {
        isOK := rf.sendAppendEntities(server, &args, &reply)
        rf.mu.Lock()
        if !isOK {
            rf.mu.Unlock()
            return
        } else {
            if !reply.IsSuccess && reply.Term > rf.currentTerm {
                rf.currentTerm = reply.Term
                rf.persist()
                rf.newState(FOLLOWER)
            } else if !reply.IsSuccess && reply.BackToIndex < rf.matchIndex[server] {
                backToIndex := reply.BackToIndex
                rf.matchIndex[server] = backToIndex
                args.PrevLogIndex = backToIndex
                args.PrevLogTerm = rf.log[backToIndex].Term
                //发送时只需要发送其下一个到末尾即可
                args.Entities = rf.log[backToIndex+1:]
            } else if reply.IsSuccess {
                DPrintf("success")
                rf.matchIndex[server] = len(rf.log) - 1
                rf.mu.Unlock()
                return
            }
        }
        rf.mu.Unlock()
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
// agreement on the next command to be appended to Raft's log.
// if this server isn't the leader, returns false. otherwise start the agreement and return immediately.
// there is no guarantee that this
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
    var index int
    // Your code here (2B).
    term, isLeader := rf.GetState()
    if isLeader == true {
        rf.mu.Lock()
        defer rf.mu.Unlock()
        index = len(rf.log)
        rf.log = append(rf.log, LogEntity{Command: command, Term: term, Index: index})
        rf.persist()
    }
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

    rf.currentTerm = 0                     //初始0， 自增1
    rf.votedFor = -1                       //
    rf.log = make([]LogEntity, 1)          // 初始index 1
    rf.commitIndex = 0                     //初始0
    rf.lastApplied = 0                     //初始0
    rf.nextIndex = make([]int, 0)          //初始为leader的最新log index + 1
    rf.matchIndex = make([]int, 0)         // 初始为0， 自增1
    atomic.StoreInt32(&rf.state, FOLLOWER) //所有raft开始都是follower
    rf.applyCh = applyCh                   //
    rf.votedCount = 0                      // 票数初始化为0
    rand.Seed(time.Now().Unix())
    rf.timer = time.NewTimer(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
    // Your initialization code here (2A, 2B, 2C).

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())
    go rf.StateLoop()

    return rf
}

func (rf *Raft) newState(state int32) {
    atomic.StoreInt32(&rf.state, state)
    switch state {
    case CANDIDATE:
        rf.mu.Lock()
        rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
        rf.mu.Unlock()
        rf.CallVote()
    case FOLLOWER:
        rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
        rf.votedFor = -1
    case LEADER:
        rf.mu.Lock()
        rf.timer.Reset(RandDuration(HEARTBEAT_TIMEOUT_MIN, HEARTBEAT_TIMEOUT_MAX))
        rf.mu.Unlock()
        rf.initIndex()
        rf.SendEntities()
    }
}

func (rf *Raft) canCommit(logIndex int) bool {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    count := 1
    if logIndex > len(rf.log)-1 {
        return false
    }
    if len(rf.peers) == 1 {
        return true
    }

    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me && rf.matchIndex[i] >= logIndex {
            count++
            if count > len(rf.peers)/2 {
                return true
            }
        }
    }
    return false
}

func (rf *Raft) getLastLog() LogEntity {
    return rf.log[len(rf.log)-1]
}

func (rf *Raft) StateLoop() {
    for {
        time.Sleep(10 * time.Millisecond)
        switch atomic.LoadInt32(&rf.state) {
        case FOLLOWER:
            select {
            case <-rf.timer.C:
                rf.newState(CANDIDATE)
            default:
            }
        case LEADER:
            select {
            case <-rf.timer.C:
                rf.timer.Reset(RandDuration(HEARTBEAT_TIMEOUT_MIN, HEARTBEAT_TIMEOUT_MAX))
                rf.SendEntities()
            default:
                rf.mu.Lock()
                i := rf.commitIndex
                if rf.log[i].Term != rf.currentTerm {
                    for ; i < len(rf.log) && rf.log[i].Term != rf.currentTerm; i++ {
                    }
                }
                rf.mu.Unlock()
                for ; rf.canCommit(i); i++ {
                    rf.mu.Lock()
                    rf.commitIndex = i
                    rf.mu.Unlock()
                }
                go rf.applyLog()
            }
        case CANDIDATE:
            select {
            case <-rf.timer.C:
                rf.mu.Lock()
                rf.timer.Reset(RandDuration(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX))
                rf.mu.Unlock()
                rf.CallVote()
            default:
                rf.mu.Lock()
                votedCount := rf.votedCount
                peerNum := len(rf.peers)
                rf.mu.Unlock()
                if votedCount > peerNum/2 {
                    DPrintf("%d become leader", rf.me)
                    rf.newState(LEADER)
                }
            }
        }

    }
}

func (rf *Raft) applyLog() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.commitIndex > rf.lastApplied {
        for i := rf.lastApplied + 1; i < rf.commitIndex+1; i++ {
            msg := ApplyMsg{
                CommandIndex: i,
                Command:      rf.log[i].Command,
                CommandValid: true,
            }
            rf.applyCh <- msg
            rf.lastApplied++
        }

    }
}
