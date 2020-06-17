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

func TestAddWorkerMidWay(t *testing.T) {

	// record rpc address of viewserver and primary into zk
	conn, _, err1 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err1 != nil {
		panic(err1)
	}

	// init zk environment
	zkservice.InitEnv(conn)

	// record worker info in zk
	for i := 0; i < 10; i++ {
		pbservice.StartWorker(i, conn)
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
		if utilservice.DebugMode {
			log.Println(keys[i], values[i])
		}

		args := pbservice.PutArgs{Key: keys[i], Value: values[i]}
		reply := pbservice.PutReply{}
		masters[0].Put(&args, &reply)

		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[0].Get(&getArgs, &getReply)
		utilservice.MyPrintln(getReply.Value)

		if getReply.Value != values[i] {
			log.Fatalln("get value incorrect")
		}
	}
	fmt.Println("Basic get-put Pass")

	// add new workers
	var vshostsNew [3]string
	var primaryhostsNew [3]string

	for i := range vshostsNew {
		vshostsNew[i] = port("viewserver-new", i+1)
		viewservice.StartServer(vshostsNew[i])

		primaryhostsNew[i] = port("worker-new1-node1", i+1)
		pbservice.StartServer(vshostsNew[i], primaryhostsNew[i])

		zkservice.CreateWorkParentPath(10+i+1, conn)
		_, err2 := conn.Create(zkservice.GetWorkViewServerPath(i+10+1), []byte(vshostsNew[i]), 0, zk.WorldACL(zk.PermAll)) //persistent znode
		if err2 != nil {
			panic(err2)
		}
		_, err3 := conn.Create(zkservice.GetWorkPrimaryPath(i+10+1), []byte(primaryhostsNew[i]), 0, zk.WorldACL(zk.PermAll))
		if err3 != nil {
			panic(err3)
		}

		// master will update worker table through zookeeper watch stragety
	}

	// test put-get is correct after add new node
	for i := 0; i < 10; i++ {
		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[0].Get(&getArgs, &getReply)

		utilservice.MyPrintln(getReply.Value)

		getReply = pbservice.GetReply{}
		masters[0].Get(&getArgs, &getReply)
		utilservice.MyPrintln(getReply.Value)

		if getReply.Err != pbservice.OK || getReply.Value != values[i] {
			log.Fatalln("after add node, get value incorrect")
		}
	}
	fmt.Println("Get-put after add worker Pass")

	// test added node has worked
	for i := 0; i < 10; i++ {
		keys[i] = utilservice.RandStringBytesMaskImpr(10)
		values[i] = utilservice.RandStringBytesMaskImpr(20)
		if utilservice.DebugMode {
			log.Println(keys[i], values[i])
		}

		args := pbservice.PutArgs{Key: keys[i], Value: values[i]}
		reply := pbservice.PutReply{}
		masters[0].Put(&args, &reply)

		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[0].Get(&getArgs, &getReply)
		utilservice.MyPrintln(getReply.Value)

		if getReply.Err != pbservice.OK || getReply.Value != values[i] {
			log.Fatalln("get value incorrect")
		}
	}
	fmt.Println("Added workers service request Pass")

	// test all master can handle request
	for i := 0; i < 10; i++ {
		keys[i] = utilservice.RandStringBytesMaskImpr(10)
		values[i] = utilservice.RandStringBytesMaskImpr(20)
		if utilservice.DebugMode {
			log.Println(keys[i], values[i])
		}

		args := pbservice.PutArgs{Key: keys[i], Value: values[i]}
		reply := pbservice.PutReply{}
		masters[i%3].Put(&args, &reply)

		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[(i+1)%3].Get(&getArgs, &getReply)
		utilservice.MyPrintln(getReply.Value)

		if getReply.Err != pbservice.OK || getReply.Value != values[i] {
			log.Fatalln("get value incorrect")
		}
	}
	fmt.Println("All masters handle request Pass")

	fmt.Println("TestAddWorkerMidWay Pass")
}

func TestDeleteWorkerMidWay(t *testing.T) {

	// record rpc address of viewserver and primary into zk
	conn, _, err1 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err1 != nil {
		panic(err1)
	}

	// init zk environment
	zkservice.InitEnv(conn)

	// record worker info in zk
	for i := 0; i < 10; i++ {
		pbservice.StartWorker(i, conn)
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
		if utilservice.DebugMode {
			log.Println(keys[i], values[i])
		}

		args := pbservice.PutArgs{Key: keys[i], Value: values[i]}
		reply := pbservice.PutReply{}
		masters[0].Put(&args, &reply)

		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[0].Get(&getArgs, &getReply)
		utilservice.MyPrintln(getReply.Value)

		if getReply.Value != values[i] {
			log.Fatalln("get value incorrect")
		}
	}
	fmt.Println("Basic get-put Pass")

	// delete part worker
	for i := 0; i < 3; i++ {
		workerPath := zkservice.GetWokrParentPath(i + 1)
		zkservice.RecursiveDelete(conn, workerPath)
		time.Sleep(time.Second)
	}

	// test put-get is correct after delete worker
	for i := 0; i < 10; i++ {
		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[0].Get(&getArgs, &getReply)

		utilservice.MyPrintln(getReply.Value)

		getReply = pbservice.GetReply{}
		masters[0].Get(&getArgs, &getReply)
		utilservice.MyPrintln(getReply.Value)

		if getReply.Err != pbservice.OK || getReply.Value != values[i] {
			log.Fatalln("after delete node, get value incorrect")
		}
	}
	fmt.Println("Get-put after delete worker Pass")

	// test all master can handle request
	for i := 0; i < 10; i++ {
		keys[i] = utilservice.RandStringBytesMaskImpr(10)
		values[i] = utilservice.RandStringBytesMaskImpr(20)
		if utilservice.DebugMode {
			log.Println(keys[i], values[i])
		}

		args := pbservice.PutArgs{Key: keys[i], Value: values[i]}
		reply := pbservice.PutReply{}
		masters[i%3].Put(&args, &reply)

		getArgs := pbservice.GetArgs{Key: keys[i]}
		getReply := pbservice.GetReply{}
		masters[(i+1)%3].Get(&getArgs, &getReply)
		utilservice.MyPrintln(getReply.Value)

		if getReply.Err != pbservice.OK || getReply.Value != values[i] {
			log.Fatalln("get value incorrect")
		}
	}
	fmt.Println("All masters handle request Pass")

	fmt.Println("TestDropWorkerMidWay Pass")
}
