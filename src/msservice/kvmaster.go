package msservice

import (
	"fmt"
	"net/rpc"
	"os"
	"pbservice"
	"strconv"
	"time"
	"viewservice"
)

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "pb-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
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
func (master *Master) Get(args *pbservice.GetArgs, reply *pbservice.GetReply) error {

	reply.Err = pbservice.ErrWrongServer

	ok := false
	for !ok || reply.Err != pbservice.OK {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Get", args, &reply)
		}

		//key not exist
		if reply.Err == pbservice.ErrNoKey {
			reply.Value = pbservice.KeyInexsitence
		}
		time.Sleep(viewservice.PingInterval)
	}

	return nil
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (master *Master) Put(args *pbservice.PutArgs, reply *pbservice.PutReply) error {

	ok := false
	for !ok {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Put", args, &reply)
		}
		// time.Sleep(viewservice.PingInterval)   // sleep will abort locks
	}
	if reply.Err != pbservice.OK {
		fmt.Println(reply.Err)
	}
	return nil
}

//
// tell the primary to delete key from db
// must keep trying until it succeeds.
//
func (master *Master) Delete(args *pbservice.DeleteArgs, reply *pbservice.DeleteReply) error {
	ok := false
	for !ok {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Delete", args, &reply)
		}
		// time.Sleep(viewservice.PingInterval)   // sleep will abort locks
	}
	if reply.Err != pbservice.OK {
		fmt.Println(reply.Err)
	}
	return nil
}
