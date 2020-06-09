package msservice

import (
	"fmt"
	"pbservice"
	"testing"
	"time"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

func TestKvBasic(t *testing.T) {
	// create worker

	vshost := port("viewserver", 1)
	viewservice.StartServer(vshost)

	primaryhost := port("worker1-node1", 1)
	pbservice.StartServer(vshost, primaryhost)

	// record rpc address of viewserver and primary into zk
	conn, _, err1 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err1 != nil {
		panic(err1)
	}

	var acls = zk.WorldACL(zk.PermAll)
	_, err2 := conn.Create(zkservice.WorkerViewServerPath, []byte(vshost), zk.FlagEphemeral, acls)
	if err2 != nil {
		panic(err2)
	}

	_, err3 := conn.Create(zkservice.WorkerPrimayPath, []byte(primaryhost), zk.FlagEphemeral, acls)
	if err3 != nil {
		panic(err3)
	}

	// create master
	masters := [3]Master{}
	processName := [3]int{1, 2, 3}

	for i := 0; i < 3; i++ {
		masters[i].label = processName[i]
		masters[i].init()
	}

	conn, _, err0 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err0 != nil {
		panic(err0)
	}

	masters[0].getWorkInfo()

	args := pbservice.PutArgs{Key: "hello", Value: "world"}
	reply := pbservice.PutReply{}
	masters[0].Put(&args, &reply)

	getArgs := pbservice.GetArgs{Key: "hello"}
	getReply := pbservice.GetReply{}
	masters[0].Get(&getArgs, &getReply)

	fmt.Println(getReply.Value)

	fmt.Println("TestKvBasic Pass")
}
