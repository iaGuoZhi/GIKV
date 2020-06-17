package viewservice

import (
	"fmt"
	"net/rpc"
)

// Clerk ... live in client and maintain some metadata of viewserver
type Clerk struct {
	me     string // client's unix rpc address
	server string // viewservice's unix rpc address
}

// MakeClerk ... create a vs clerk
func MakeClerk(me string, server string) *Clerk {
	ck := new(Clerk)
	ck.me = me
	ck.server = server
	return ck
}

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
	return false
}

// Ping ... client ping viewserver using rpc
func (ck *Clerk) Ping(viewnum uint) (View, error) {
	// prepare the arguments.
	args := &PingArgs{}
	args.Me = ck.me
	args.Viewnum = viewnum
	var reply PingReply

	// send an RPC request, wait for the reply.
	ok := call(ck.server, "ViewServer.Ping", args, &reply)
	if ok == false {
		return View{}, fmt.Errorf("Ping(%v) failed", viewnum)
	}

	return reply.View, nil
}

// Get ... client get latest view of viewserver
func (ck *Clerk) Get() (View, bool) {
	args := &GetArgs{}
	var reply GetReply
	ok := call(ck.server, "ViewServer.Get", args, &reply)
	if ok == false {
		return View{}, false
	}
	return reply.View, true
}

// Primary ... return current view's primary
func (ck *Clerk) Primary() string {
	v, ok := ck.Get()
	if ok {
		return v.Primary
	}
	return ""
}
