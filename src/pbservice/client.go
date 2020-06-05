package pbservice

import (
	"fmt"
	"net/rpc"
	"time"
	"viewservice"
)

// You'll probably need to uncomment this:

type Clerk struct {
	vs *viewservice.Clerk
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	// Your code here.
	args := &GetArgs{key}
	var reply GetReply
	reply.Err = ErrWrongServer

	ok := false
	for !ok || reply.Err != OK {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = ck.vs.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Get", args, &reply)
		}

		//key not exist
		if reply.Err == ErrNoKey {
			return KeyInexsitence
		}
		time.Sleep(viewservice.PingInterval)
	}

	return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	// Your code here.
	args := &PutArgs{key, value}
	var reply PutReply

	ok := false
	for !ok {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = ck.vs.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Put", args, &reply)
		}
		// time.Sleep(viewservice.PingInterval)   // sleep will abort locks
	}
	if reply.Err != OK {
		fmt.Println(reply.Err)
	}

}

//
// tell the primary to delete key from db
// must keep trying until it succeeds.
//
func (ck *Clerk) Delete(key string) {
	// Your code here.
	args := &DeleteArgs{key}
	var reply DeleteReply

	ok := false
	for !ok {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = ck.vs.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Delete", args, &reply)
		}
		// time.Sleep(viewservice.PingInterval)   // sleep will abort locks
	}
	if reply.Err != OK {
		fmt.Println(reply.Err)
	}

}

// receive DB if a backup is built
func MoveDB(backup string, db map[string]string) {
	args := &MoveDBArgs{db}
	var reply MoveDBReply
	ok := call(backup, "PBServer.MoveDB", args, &reply)
	if !ok {
		// fmt.Println("Call MoveDB error")
	}
	if reply.Err != OK {
		fmt.Println(reply.Err)
	}
}
