package msservice

import (
	"log"
	"strconv"
	"strings"
	"viewservice"
	"zkservice"
)

func (master *Master) SlaveAddWorker(args *AddWorkerArgs, reply *AddWorkerReply) error {

	// check current master is doing master job now or not
	byteInfo, _, err0 := master.conn.Get(zkservice.MasterMasterPath)
	if err0 != nil {
		reply.err = ErrOther
		return err0
	}
	if strings.Compare(master.myRPCAddress, string(byteInfo)) == 0 {
		log.Fatalln("this master should be slave")
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

func (master *Master) syncAddWorker(args *AddWorkerArgs, reply *AddWorkerReply) error {

}
