package msservice

import (
	"fmt"
	"log"
	"pbservice"
	"testing"
	"time"
	"utilservice"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

func TestMasterSlaveSync(t *testing.T) {
	// create worker
	var vshosts [10]string
	var primaryhosts [10]string

	// record rpc address of viewserver and primary into zk
	conn, _, err1 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err1 != nil {
		panic(err1)
	}

	// init zk environment
	zkservice.InitEnv(conn)

	// record worker info in zk
	for i := range vshosts {
		vshosts[i] = port("viewserver", i+1)
		viewservice.StartServer(vshosts[i])

		primaryhosts[i] = port("worker1-node1", i+1)
		pbservice.StartServer(vshosts[i], primaryhosts[i])

		zkservice.CreateWorkParentPath(i+1, conn)
		var acls = zk.WorldACL(zk.PermAll)
		_, err2 := conn.Create(zkservice.GetWorkViewServerPath(i+1), []byte(vshosts[i]), 0, acls) //persistent znode
		if err2 != nil {
			panic(err2)
		}

		_, err3 := conn.Create(zkservice.GetWorkPrimayPath(i+1), []byte(primaryhosts[i]), 0, acls)
		if err3 != nil {
			panic(err3)
		}
	}

	// create master
	masters := [3]Master{}
	processName := [3]int{1, 2, 3}

	for i := 0; i < 3; i++ {
		masters[i].label = processName[i]
		masters[i].init()
	}

	var keys [100]string
	var values [100]string
	for i := 0; i < 10; i++ {
		keys[i] = utilservice.RandStringBytesMaskImpr(10)
		values[i] = utilservice.RandStringBytesMaskImpr(20)
		log.Println(keys[i], values[i])

		args := pbservice.PutArgs{Key: keys[i], Value: values[i]}
		reply := pbservice.PutReply{}
		masters[i%3].Put(&args, &reply)

		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[i%3].Get(&getArgs, &getReply)
		log.Println(getReply.Value)

		if getReply.Value != values[i] {
			log.Println("get value incorrect")
		}
	}

	// add new workers
	var vshostsNew [3]string
	var primaryhostsNew [3]string

	for i := range vshostsNew {
		vshostsNew[i] = port("viewserver-new", i+1)
		viewservice.StartServer(vshostsNew[i])

		primaryhostsNew[i] = port("worker-new1-node1", i+1)
		pbservice.StartServer(vshostsNew[i], primaryhostsNew[i])

		zkservice.CreateWorkParentPath(10+i+1, conn)
		var acls = zk.WorldACL(zk.PermAll)
		_, err2 := conn.Create(zkservice.GetWorkViewServerPath(i+10+1), []byte(vshostsNew[i]), 0, acls) //persistent znode
		if err2 != nil {
			panic(err2)
		}

		_, err3 := conn.Create(zkservice.GetWorkPrimayPath(i+10+1), []byte(primaryhostsNew[i]), 0, acls)
		if err3 != nil {
			panic(err3)
		}

		args := AddWorkerArgs{vshost: vshostsNew[i], primaryRPCAddress: primaryhostsNew[i], label: i + 10 + 1}
		reply := AddWorkerReply{}
		masters[0].AddWorker(&args, &reply)
		if reply.err != OK {
			log.Fatalln(reply.err)
		}
		log.Println(reply.err)
	}

	// test put-get is correct after add new node
	for i := 0; i < 10; i++ {
		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[i%3].Get(&getArgs, &getReply)
		log.Println(getReply.Value)

		getReply = pbservice.GetReply{}
		masters[i%3].Get(&getArgs, &getReply)
		log.Println(getReply.Value)

		if getReply.Err != pbservice.OK || getReply.Value != values[i] {
			log.Fatalln("after add node, get value incorrect")
		}
	}

	// test added node has worked
	for i := 0; i < 10; i++ {
		keys[i] = utilservice.RandStringBytesMaskImpr(10)
		values[i] = utilservice.RandStringBytesMaskImpr(20)
		log.Println(keys[i], values[i])

		args := pbservice.PutArgs{Key: keys[i], Value: values[i]}
		reply := pbservice.PutReply{}
		masters[i%3].Put(&args, &reply)

		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[i%3].Get(&getArgs, &getReply)
		log.Println(getReply.Value)

		if getReply.Err != pbservice.OK || getReply.Value != values[i] {
			log.Fatalln("get value incorrect")
		}
	}

	fmt.Println("TestAddWorkerMidWay Pass")
	fmt.Println()
}
