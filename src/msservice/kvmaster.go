package msservice

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"pbservice"
	"strconv"
	"time"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
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

	return false
}

func (master *Master) getWorkInfo() {
	conn, _, err0 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err0 != nil {
		panic(err0)
	}

	// get worker label
	workerLabels, _, err1 := conn.Children(zkservice.WorkerPath)
	if err1 != nil {
		panic(err1)
	}
	// get each worker primary and viewserver address
	for _, label := range workerLabels {
		in, err2 := strconv.Atoi(label)
		if err2 != nil {
			log.Println("worker label to int fail")
		}
		primaryAddr, _, err3 := conn.Get(zkservice.GetWorkPrimayPath(in))
		if err3 != nil {
			panic(err3)
		}
		vhost, _, err4 := conn.Get(zkservice.GetWorkViewServerPath(in))
		if err4 != nil {
			panic(err4)
		}

		worker := master.workers[in]
		worker.label = in
		worker.primaryRPCAddress = string(primaryAddr)
		worker.vshost = string(vhost)
		worker.vck = viewservice.MakeClerk("", worker.vshost)
		master.workers[in] = worker
	}
}

func (master *Master) initDht() {
	for k := range master.workers {
		lableStr := strconv.Itoa(k)
		master.consistent.Add(lableStr)
	}
}

// AddWorker ... add new worker(viewserver+primary+backup) to master's workers and consistent
func (master *Master) AddWorker(args *AddWorkerArgs, reply *AddWorkerReply) error {
	workPrimayPath := zkservice.GetWorkPrimayPath(args.label)
	workViewServerPath := zkservice.GetWorkPrimayPath(args.label)

	exists, _, err1 := master.conn.Exists(workPrimayPath)
	if err1 != nil {
		reply.err = ErrOther
		return err1
	}
	if exists != true {
		reply.err = ErrNoZnode
		return nil
	}

	exists, _, err1 = master.conn.Exists(workViewServerPath)
	if err1 != nil {
		reply.err = ErrOther
		return err1
	}
	if exists != true {
		reply.err = ErrNoZnode
		return nil
	}

	if _, exists = master.workers[args.label]; exists {
		reply.err = ErrAlreadyAdded
		return nil
	}

	worker := master.workers[args.label]
	worker.label = args.label
	worker.vshost = args.vshost
	worker.primaryRPCAddress = args.primaryRPCAddress
	worker.vck = viewservice.MakeClerk("", worker.vshost)
	master.workers[args.label] = worker

	//add to consistent
	master.consistent.Add(strconv.Itoa(args.label))

	reply.err = OK
	return nil
}

// Get ... look for correct worker
func (master *Master) Get(args *pbservice.GetArgs, reply *pbservice.GetReply) error {

	found := false
	// first try, the nearest two physical node
	node1, node2, err1 := master.consistent.GetTwo(args.Key)
	if err1 != nil {
		panic(err1)
	}
	if node1 != "" {
		master.get(args, reply, node1)
		if reply.Err == pbservice.OK {
			found = true
			return nil
		}
	}
	if node2 != "" {
		master.get(args, reply, node2)
		if reply.Err == pbservice.OK {
			found = true
		}
	}

	if found == false {
		nodes, err2 := master.consistent.GetN(args.Key, master.consistent.Size())
		if err2 != nil {
			panic(err2)
		}
		for i := 2; i < master.consistent.Size(); i++ {
			if nodes[i] != "" {
				master.get(args, reply, nodes[i])
				if reply.Err == pbservice.OK {
					found = true
					break
				}
			}
		}
	}

	if found {
		putArgs := pbservice.PutArgs{Key: args.Key, Value: reply.Value}
		putReply := pbservice.PutReply{}
		master.Put(&putArgs, &putReply)
	}
	return nil
}

func (master *Master) get(args *pbservice.GetArgs, reply *pbservice.GetReply, workerLabelStr string) error {

	log.Printf("%s => %s\n", args.Key, workerLabelStr)
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
	log.Printf("%s => %s\n", args.Key, workerLabelStr)

	workerLable, err2 := strconv.Atoi(workerLabelStr)
	if err2 != nil {
		panic(err2)
	}

	ok := false
	for !ok {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.workers[workerLable].vck.Get()
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
	workerLabelStr, err1 := master.consistent.Get(args.Key)
	if err1 != nil {
		panic(err1)
	}
	log.Printf("%s => %s\n", args.Key, workerLabelStr)

	workerLable, err2 := strconv.Atoi(workerLabelStr)
	if err2 != nil {
		panic(err2)
	}

	ok := false
	for !ok {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.workers[workerLable].vck.Get()
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
