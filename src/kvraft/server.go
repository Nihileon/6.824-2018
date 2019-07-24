package raftkv

import (
    "labgob"
    "labrpc"
    "log"
    "raft"
    "sync"
    "time"
)

const (
    APPLY_TIMEOUT = 600 * time.Millisecond
)

//TODO 服务器在收到请求之后首先将请求发送给raft，并等待raft的状态
//TODO raft收到command，复制，commit并apply到applyCh才会给服务器返回对应的回复
//TODO 对raft的handler
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

type HandleResult struct {
    key       string
    value     string
    err       Err
    operation string
    clientID  int64
}

type Op struct {
    // Your definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    Key       string
    Value     string
    ClientID  int64
    Seq       int
    Operation string
}

type KVServer struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft
    applyCh chan raft.ApplyMsg

    maxraftstate int // snapshot if log grows this big

    // Your definitions here.
    KVs           map[string]string
    clerkSeq      map[int64]int
    requestRecord map[int]chan HandleResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // Your code here.
    //DPrintf("get something")
    op := Op{Key: args.Key, Operation: "Get", ClientID: args.ClientID, Seq: args.Seq}
    //TODO 对term的判断
    _, isleader := kv.rf.GetState()
    DPrintf("server:%d is leader:%t, get, key:%s", kv.me, isleader, args.Key)
    index, _, isLeader := kv.rf.Start(op)
    kv.mu.Lock()
    //DPrintf("get lock")
    if !isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        //DPrintf("get un lock")
        return
    }
    reply.WrongLeader = false
    kv.requestRecord[index] = make(chan HandleResult)
    kv.mu.Unlock()
    //DPrintf("get un lock")
    select {
    case <-time.After(APPLY_TIMEOUT):
        reply.WrongLeader = true
        return
    case result := <-kv.requestRecord[index]:
        reply.Value = result.value
        reply.Err = result.err
    }

    //DPrintf("server get something")
    //if result.term != term {
    //    reply.WrongLeader = true
    //    return
    //}

    //TODO server发出操作请求后需要等待接收，等待接收的过程不应该阻塞，而应该利用channel，让raft将结果返回给server后再操作KV
    // 由于我们在创建log的时候很快就获得了一个index和对应的term，而index在apply之后是唯一的，因此我们可以将其用作接收结果的标识；
    // 首先我们需要在发出rf.start之前对server加锁，防止其他client在同一时间操作server，接下来我们发出start请求,
    // 并将返回的index作为key，接收channel作为value，放在之前建立的map上，这样当服务器apply log、操作并返回结果的时候就能不阻塞地处理了。

    // TODO 另外，为了避免代码的重复出现，我们需要构建一个handler，对所有来自raft的applyCh的请求进行操作。

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    //DPrintf("server putappend something")
    // Your code here.
    op := Op{Key: args.Key, Value: args.Value, Operation: args.Op, ClientID: args.ClientID, Seq: args.Seq}

    _, isleader := kv.rf.GetState()
    DPrintf("server:%d is leader:%t, put append, key:%s, value:%s, operation:%s", kv.me, isleader, args.Key, args.Value, args.Op)
    if seq, isIn := kv.clerkSeq[op.ClientID]; isIn && op.Seq <= seq {
        reply.WrongLeader = !isleader
        reply.Err = OK
        return
    }
    index, _, isLeader := kv.rf.Start(op)
    kv.mu.Lock()
    reply.Err = OK
    //DPrintf("PutAppend lock")
    if !isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
        //DPrintf("PutAppend un lock")
        return

    }
    reply.WrongLeader = false
    kv.requestRecord[index] = make(chan HandleResult)
    kv.mu.Unlock()
    //DPrintf("PutAppend un lock")

    select {
    case <-time.After(APPLY_TIMEOUT):
        reply.WrongLeader = true
        return
    case result := <-kv.requestRecord[index]:
        reply.Err = result.err
    }

    //DPrintf("putappend get result ")
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
    kv.rf.Kill()
    // Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
    // call labgob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    labgob.Register(Op{})

    kv := new(KVServer)
    kv.me = me
    kv.maxraftstate = maxraftstate

    // You may need initialization code here.

    kv.applyCh = make(chan raft.ApplyMsg)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)

    // You may need initialization code here.
    kv.KVs = make(map[string]string)
    kv.clerkSeq = make(map[int64]int)
    kv.requestRecord = make(map[int]chan HandleResult)
    //DPrintf("start server : %d", me)
    go kv.StartLoop()

    return kv

}

func (kv *KVServer) StartLoop() {
    for {
        time.Sleep(10 * time.Millisecond)
        select {
        //type ApplyMsg struct {
        //    CommandValid bool
        //    Command      interface{}
        //    CommandIndex int
        //}
        //TODO 在接收apply之后将命令转化为原来的op，并进行操作，将结果返回给get或putAppend请求
        case msg := <-kv.applyCh:
            if msg.CommandValid {
                op := msg.Command.(Op)

                result := HandleResult{
                    key:       op.Key,
                    value:     op.Value,
                    err:       OK,
                    operation: op.Operation,}
                kv.mu.Lock()
                //DPrintf("StartLoop lock")
                if seq, isIn := kv.clerkSeq[op.ClientID]; seq < op.Seq || op.Operation == "Get" || !isIn {
                    kv.clerkSeq[op.ClientID] = op.Seq
                    switch op.Operation {
                    case "Get":
                        if value, isIn := kv.KVs[op.Key]; !isIn {
                            result.err = ErrNoKey
                        } else {
                            result.value = value
                        }
                    case "Put":
                        kv.KVs[op.Key] = op.Value
                    case "Append":
                        if val, isIn := kv.KVs[op.Key]; isIn {
                            kv.KVs[op.Key] = val + op.Value
                        } else {
                            kv.KVs[op.Key] = op.Value
                        }
                    }
                    if ch, isIn := kv.requestRecord[msg.CommandIndex]; isIn {
                        ch <- result
                        delete(kv.requestRecord, msg.CommandIndex)
                    }

                } else if isIn {
                    delete(kv.requestRecord, msg.CommandIndex)
                }

                kv.mu.Unlock()
                //DPrintf("StartLoop un lock")

            } else {

            }
        }
    }
}

// 对于putappend请求，一共有两种处理的可能
