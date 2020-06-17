package msservice

import (
	"fmt"
	"log"
	"pbservice"
	"strconv"
	"utilservice"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

func (master *Master) addWorkerByZkLabel(label string) {

	in, err2 := strconv.Atoi(label)
	if err2 != nil {
		log.Println("worker label to int fail")
	}

	vsExist, _, vsExistChan, err2 := master.conn.ExistsW(zkservice.GetWorkViewServerPath(in))
	if err2 != nil {
		panic(err2)
	}

	if !vsExist {
		go func() {
			e := <-vsExistChan
			if e.Type != zk.EventNodeCreated {
				log.Println("watch: unexpected type")
			}
			vsExist = true
		}()
	}

	// wait for znode: worker/in/viewserver estabilish
	for !vsExist {

	}

	vhost, _, err4 := master.conn.Get(zkservice.GetWorkViewServerPath(in))
	if err4 != nil {
		panic(err4)
	}

	worker := master.workers[in]
	worker.label = in
	worker.vshost = string(vhost)
	worker.vck = viewservice.MakeClerk("", worker.vshost)
	master.workers[in] = worker

	// add worker into consistent hashing
	master.consistent.Add(label)
}

func (master *Master) removeWorker(label string) {
	in, err2 := strconv.Atoi(label)
	if err2 != nil {
		log.Println("worker label to int fail")
	}

	if master.bmaster == true {
		args := pbservice.DropDBArgs{MyWorkerLabel: label, MasterPRCAddress: master.myRPCAddress}
		reply := pbservice.DropDBReply{Err: pbservice.OK}
		ok := false
		for !ok || reply.Err != pbservice.OK {
			vok := false
			var view viewservice.View
			for !vok {
				view, vok = master.workers[in].vck.Get()
			}

			primaryAddress := view.Primary
			if primaryAddress != "" {
				ok = call(primaryAddress, "PBServer.DropDB", &args, &reply)
			}
			if reply.Err != pbservice.OK {
				fmt.Println(reply.Err)
			}
		}
	}

	delete(master.workers, in)
	master.consistent.Remove(label)
}

func (master *Master) getWorkInfo() {

	// get worker label
	workerLabels, _, err1 := master.conn.Children(zkservice.WorkerPath)
	if err1 != nil {
		panic(err1)
	}
	// get each worker primary and viewserver address
	for _, label := range workerLabels {
		master.addWorkerByZkLabel(label)
	}

	go func() {
		for {
			children, _, childCh, err2 := master.conn.ChildrenW(zkservice.WorkerPath)
			if err2 != nil {
				panic(err2)
			}
			select {
			case chEvent := <-childCh:
				{
					if chEvent.Type != zk.EventNodeChildrenChanged {
						continue
					}

					curChildren, _, err3 := master.conn.Children(zkservice.WorkerPath)
					if err3 != nil {
						panic(err3)
					}

					addChildren, deleteChildren := utilservice.CompareZkChildren(curChildren, children)

					if addChildren != nil {
						for _, childLabel := range addChildren {
							master.addWorkerByZkLabel(childLabel)
						}
						continue
					}

					if deleteChildren != nil {
						if utilservice.DebugMode {
							log.Println(deleteChildren)
							log.Println(children)
						}
						for _, childLabel := range deleteChildren {
							master.removeWorker(childLabel)
						}
						continue
					}
				}
			}
		}
	}()
}

// GetNextNode ... called by Worker primary, when it had to drop, it should forward data to data's next server in consistent hashing
func (master *Master) GetNextNode(args *pbservice.ConsistentNextArgs, reply *pbservice.ConsistentNextReply) error {
	next, _ := master.consistent.GetNext(args.Key, args.CurServerLabel)

	if next == "" {
		reply.Err = ErrOther
	} else {
		reply.Err = OK
		reply.NextWorkerLabel = next

		nextIn, err1 := strconv.Atoi(next)
		if err1 != nil {
			panic(err1)
		}

		vok := false
		var view viewservice.View
		for !vok {
			view, vok = master.workers[nextIn].vck.Get()
		}
		reply.NextWorkerPrimaryRPCAddress = view.Primary
	}

	return nil
}
