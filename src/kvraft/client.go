package raftkv

import (
    "labrpc"
    "time"
)
import "crypto/rand"
import "math/big"

const (
    SEND_FAIL_INTERVAL = time.Duration(400 * time.Millisecond)
)

//TODO 容错性：
// 防止同一个请求由于某个leader 突然失效而导致重复handle
// 1. log中有一个请求ID，如果这个请求ID未在上一个cache的log中出现过则说明不是重复请求，否则将请求抛弃
type Clerk struct {
    servers  []*labrpc.ClientEnd
    clientID int64
    seq      int
    leaderID int
    // You will have to modify this struct.
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
    ck := new(Clerk)
    ck.servers = servers
    // You'll have to add code here.
    ck.clientID = nrand()
    ck.leaderID = 0
    ck.seq = 0
    return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
    // You will have to modify this function.
    //TODO 在失败第一次后暂停一段时间（大于心跳选举时长），再次无间隔循环，再次循环到leader如果继续错误则再次暂停一个选举时长
    //TODO 补充args
    args := GetArgs{Key: key, ClientID: ck.clientID, Seq: ck.seq}
    ck.seq++
    DPrintf("client get key:%s", args.Key)
    for i := ck.leaderID; ; {
        var reply GetReply
        isOK := ck.sendGet(i, &args, &reply)
        if i == ck.leaderID && isOK && !reply.WrongLeader && reply.Err == OK {
            DPrintf("get successfully,key:%s , value:%s", args.Key, reply.Value)
            return reply.Value
            //TODO 这两个ERR有什么鬼鬼区别❓
        } else if i == ck.leaderID && (!isOK || reply.WrongLeader) {
            DPrintf("get failed and try again")
            time.Sleep(SEND_FAIL_INTERVAL)
        } else if isOK && !reply.WrongLeader {
            DPrintf("get successfully by other leader, key:%s, value:%s",args.Key,  reply.Value)
            ck.leaderID = i
            return reply.Value
        } else {
            DPrintf("get fail and do nothing")

        }
        i = (i + 1) % len(ck.servers)
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    // You will have to modify this function.
    //TODO 补充args
    args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, Seq: ck.seq}
    ck.seq++
    DPrintf("client putappend  op:%s, key:%s value:%s", args.Op, args.Key, args.Value)
    for i := ck.leaderID; ; {
        var reply PutAppendReply
        isOK := ck.sendPutAppend(i, &args, &reply)
        //DPrintf("put append  to %d finish, can get reply %t ,isleader %t ", i,isOK, !reply.WrongLeader)
        if i == ck.leaderID && isOK && !reply.WrongLeader {
            DPrintf("putappend successfully op:%s, key:%s value:%s", args.Op, args.Key, args.Value)
            return
            //TODO 这两个ERR有什么鬼鬼区别❓
        } else if i == ck.leaderID && (!isOK || reply.WrongLeader) {
            //DPrintf("putappend faild and sleep")
            time.Sleep(SEND_FAIL_INTERVAL)
        } else if isOK && !reply.WrongLeader {
            DPrintf("putappend successfully by other leader op:%s, key:%s value:%s", args.Op, args.Key, args.Value)
            ck.leaderID = i
            return
        } else {
            //DPrintf("putappend failed and do nothing")
        }
        i = (i + 1) % len(ck.servers)
    }
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
    //DPrintf("get server id %d", server)
    ok := ck.servers[server].Call("KVServer.Get", args, reply)
    return ok
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
    //DPrintf("putappend server id %d", server)
    ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
    return ok
}

func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}
