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

func TestBasicOperation(t *testing.T) {
	conn, _, err1 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err1 != nil {
		panic(err1)
	}

	// init zk environment
	zkservice.InitEnv(conn)

	// create worker
	pbservice.StartWorker(1, conn)

	// create master
	master := Master{}
	master.init(0)

	args := pbservice.PutArgs{Key: "hello", Value: "world"}
	reply := pbservice.PutReply{}
	master.Put(&args, &reply)

	getArgs := pbservice.GetArgs{Key: "hello"}
	getReply := pbservice.GetReply{}
	master.Get(&getArgs, &getReply)

	if getReply.Value != "world" {
		log.Println("get value incorect")
	}

	deleteArgs := pbservice.DeleteArgs{Key: "hello"}
	deleteReply := pbservice.DeleteReply{}
	master.Delete(&deleteArgs, &deleteReply)

	getArgs = pbservice.GetArgs{Key: "hello"}
	getReply = pbservice.GetReply{}
	master.Get(&getArgs, &getReply)

	if getReply.Value != "" {
		log.Println("get value incorect")
	}

	fmt.Println("Test Put-get and DeleteGet Pass")
}
func TestMultiMasterSingleWorker(t *testing.T) {

	conn, _, err1 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err1 != nil {
		panic(err1)
	}

	// init zk environment
	zkservice.InitEnv(conn)

	// create worker
	pbservice.StartWorker(1, conn)

	// create master
	masters := [3]Master{}
	processName := [3]int{1, 2, 3}

	for i := 0; i < 3; i++ {
		masters[i].init(processName[i])
	}

	args := pbservice.PutArgs{Key: "hello", Value: "world"}
	reply := pbservice.PutReply{}
	masters[0].Put(&args, &reply)

	getArgs := pbservice.GetArgs{Key: "hello"}
	getReply := pbservice.GetReply{}
	masters[0].Get(&getArgs, &getReply)

	log.Println(getReply.Value)
	if getReply.Value != "world" {
		log.Println("get value incorect")
	}

	fmt.Println("TestMultiMasterSingleWorker Pass")
	fmt.Println()
}

func TestMultiMasterMultiWorker(t *testing.T) {
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

	// create worker parent path
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

		_, err3 := conn.Create(zkservice.GetWorkPrimaryPath(i+1), []byte(primaryhosts[i]), 0, acls)
		if err3 != nil {
			panic(err3)
		}
	}

	// create master
	masters := [3]Master{}
	processName := [3]int{1, 2, 3}

	for i := 0; i < 3; i++ {
		masters[i].init(processName[i])
	}

	var keys [100]string
	var values [100]string
	for i := 0; i < 10; i++ {
		keys[i] = utilservice.RandStringBytesMaskImpr(10)
		values[i] = utilservice.RandStringBytesMaskImpr(20)
		log.Println(keys[i], values[i])

		args := pbservice.PutArgs{Key: keys[i], Value: values[i]}
		reply := pbservice.PutReply{}
		masters[0].Put(&args, &reply)

		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[0].Get(&getArgs, &getReply)
		log.Println(getReply.Value)

		if getReply.Value != values[i] {
			log.Println("get value incorrect")
		}
	}

	fmt.Println("TestMultiMasterMultiWorker Pass")
	fmt.Println()
}
