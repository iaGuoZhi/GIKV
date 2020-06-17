package msservice

import (
	"log"
	"pbservice"
	"strconv"
	"strings"
	"time"
	"utilservice"
	"viewservice"
)

// Get ... look for correct worker using lazy stragety
func (master *Master) Get(args *pbservice.GetArgs, reply *pbservice.GetReply) error {

	found := false
	// first try, the nearest two physical node
	node1, err1 := master.consistent.Get(args.Key)
	if err1 != nil {
		panic(err1)
	}
	if node1 != "" {
		master.get(args, reply, node1)
		if reply.Err == pbservice.OK {
			found = true
		}
	}

	if found == false {
		nodes, err2 := master.consistent.GetN(args.Key, master.consistent.Size())
		if err2 != nil {
			panic(err2)
		}
		for i := 1; i < master.consistent.Size(); i++ {
			if nodes[i] != "" {
				master.get(args, reply, nodes[i])
				if reply.Err == pbservice.OK {
					found = true
					break
				}
			}
		}
		if found {
			putArgs := pbservice.PutArgs{Key: args.Key, Value: reply.Value}
			putReply := pbservice.PutReply{}
			master.Put(&putArgs, &putReply)
		}
	}

	if strings.Compare(reply.Value, pbservice.KeyInexsitence) == 0 {
		reply.Value = ""
	}
	return nil
}

func (master *Master) get(args *pbservice.GetArgs, reply *pbservice.GetReply, workerLabelStr string) error {

	if utilservice.DebugMode {
		log.Printf("%s => %s\n", args.Key, workerLabelStr)
	}
	reply.Err = pbservice.ErrWrongServer

	workerLable, err2 := strconv.Atoi(workerLabelStr)
	if err2 != nil {
		panic(err2)
	}

	ok := false
	for !ok || reply.Err != pbservice.OK {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.workers[workerLable].vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Get", args, &reply)
		}

		//key not exist
		if reply.Err == pbservice.ErrNoKey {
			reply.Value = pbservice.KeyInexsitence
			return nil
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

	workerLabelStr, err1 := master.consistent.Get(args.Key)
	if err1 != nil {
		panic(err1)
	}
	if utilservice.DebugMode {
		log.Printf("%s => %s\n", args.Key, workerLabelStr)
	}

	workerLable, err2 := strconv.Atoi(workerLabelStr)
	if err2 != nil {
		panic(err2)
	}

	ok := false
	for !ok || reply.Err != pbservice.OK {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.workers[workerLable].vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Put", args, &reply)
		}
	}
	return nil
}

//
// tell the primary to delete key from db
// must keep trying until it succeeds.
//
func (master *Master) Delete(args *pbservice.DeleteArgs, reply *pbservice.DeleteReply) error {
	workerLabelStr, err1 := master.consistent.Get(args.Key)
	if err1 != nil {
		panic(err1)
	}
	if utilservice.DebugMode {
		log.Printf("%s => %s\n", args.Key, workerLabelStr)
	}

	workerLable, err2 := strconv.Atoi(workerLabelStr)
	if err2 != nil {
		panic(err2)
	}

	putArgs := pbservice.PutArgs{Key: args.Key, Value: pbservice.KeyInexsitence}
	putReply := pbservice.PutReply{}

	ok := false
	for !ok || putReply.Err != pbservice.OK {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.workers[workerLable].vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Put", &putArgs, &putReply)
		}
	}
	reply.Err = pbservice.OK
	return nil
}
