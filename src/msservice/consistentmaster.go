package msservice

import (
	"log"
	"strconv"
	"time"
	"utilservice"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

func (master *Master) getWorkInfoByZkLabel(label string) {

	in, err2 := strconv.Atoi(label)
	if err2 != nil {
		log.Println("worker label to int fail")
	}

	primaryExist, _, primaryExistChan, err1 := master.conn.ExistsW(zkservice.GetWorkPrimaryPath(in))
	if err1 != nil {
		panic(err1)
	}

	vsExist, _, vsExistChan, err2 := master.conn.ExistsW(zkservice.GetWorkViewServerPath(in))
	if err2 != nil {
		panic(err2)
	}

	if !primaryExist {
		go func() {
			// only trigger once
			e := <-primaryExistChan
			if e.Type != zk.EventNodeCreated {
				log.Println("watch: unexpected type")
			}
			primaryExist = true
		}()
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

	// wait for znode: worker/in/primary, worker/in/viewserver estabilish
	for !primaryExist || !vsExist {

	}

	primaryAddr, _, err3 := master.conn.Get(zkservice.GetWorkPrimaryPath(in))
	if err3 != nil {
		panic(err3)
	}
	vhost, _, err4 := master.conn.Get(zkservice.GetWorkViewServerPath(in))
	if err4 != nil {
		panic(err4)
	}

	worker := master.workers[in]
	worker.label = in
	worker.primaryRPCAddress = string(primaryAddr)
	worker.vshost = string(vhost)
	worker.vck = viewservice.MakeClerk("", worker.vshost)
	master.workers[in] = worker

	// add worker into consistent hashing
	master.consistent.Add(label)
}

func (master *Master) getWorkInfo() {

	// get worker label
	workerLabels, _, err1 := master.conn.Children(zkservice.WorkerPath)
	if err1 != nil {
		panic(err1)
	}
	// get each worker primary and viewserver address
	for _, label := range workerLabels {
		master.getWorkInfoByZkLabel(label)
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
							master.getWorkInfoByZkLabel(childLabel)
						}
					}

					if deleteChildren != nil {

					}
				}
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()
}
