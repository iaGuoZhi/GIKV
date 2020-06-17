package pbservice

import (
	"fmt"
	"net/rpc"
	"time"
	"viewservice"
)

// Clerk ... store in client
type Clerk struct {
	vs *viewservice.Clerk
}

// MakeClerk ...
func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
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

// Get ... get key's value from primary
func (ck *Clerk) Get(key string) string {
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

// Put ... tell primay to update key's value and forward put operation to slaeves
// must keep trying until it succeeds
func (ck *Clerk) Put(key string, value string) {
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
	}
	if reply.Err != OK {
		fmt.Println(reply.Err)
	}

}

// Delete ... tell primary to delete key from db and forward delete operation to slaves
func (ck *Clerk) Delete(key string) {
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
	}
	if reply.Err != OK {
		fmt.Println(reply.Err)
	}

}

// MoveDB ... move db's data to a new joined backup
func MoveDB(backup string, db map[string]string) {
	args := &MoveDBArgs{db}
	var reply MoveDBReply
	ok := call(backup, "PBServer.MoveDB", args, &reply)
	if !ok {
		//fmt.Println("Call MoveDB error")
	}
	if reply.Err != OK {
		fmt.Println(reply.Err)
	}
}
