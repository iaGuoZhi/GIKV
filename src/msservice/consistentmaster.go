package msservice

import (
	"log"
	"strconv"
	"strings"
	"time"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

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

func (master *Master) initConsistent() {
	for k := range master.workers {
		lableStr := strconv.Itoa(k)
		master.consistent.Add(lableStr)
	}
}

// AddWorker ... add new worker(viewserver+primary+backup) to master's workers and consistent
// this function should only be called by client, and master should doing master job now. after current master
// finished add worker, it will sync to slave
func (master *Master) AddWorker(args *AddWorkerArgs, reply *AddWorkerReply) error {

	// check current master is doing master job now or not
	byteInfo, _, err0 := master.conn.Get(zkservice.MasterMasterPath)
	if err0 != nil {
		reply.err = ErrOther
		return err0
	}
	if strings.Compare(master.myRPCAddress, string(byteInfo)) != 0 {
		log.Println(master.myRPCAddress)
		log.Println(string(byteInfo))
		log.Fatalln("this master is not doing master job now")
	}

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
