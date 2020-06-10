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
		master.dht.Add(lableStr)
	}
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

	workerLabelStr, err1 := master.dht.Get(args.Key)
	if err1 != nil {
		panic(err1)
	}
	log.Printf("%s => %s\n", args.Key, workerLabelStr)

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

	workerLabelStr, err1 := master.dht.Get(args.Key)
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
	workerLabelStr, err1 := master.dht.Get(args.Key)
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
