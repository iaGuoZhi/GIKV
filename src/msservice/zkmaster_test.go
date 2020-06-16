package msservice

import (
	"fmt"
	"log"
	"pbservice"
	"testing"
	"time"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

func check(target string, got string) {
	if target != got {
		log.Fatalf("Get(%v), expected %v", got, target)
	}
}
func TestZkBasic(t *testing.T) {
	// wait for znode in other testcase dead
	time.Sleep(time.Duration(2) * time.Second)

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

	// check master value
	masterValue, _, err1 := conn.Get(zkservice.MasterMasterPath)
	if err1 != nil {
		panic(err1)
	}
	log.Println(string(masterValue))
	fmt.Println(masters[0].myRPCAddress)

	// check process list
	processPath := string(zkservice.MasterProcessPath)
	processs, _, err2 := conn.Children(processPath)
	if err2 != nil {
		panic(err2)
	}
	for i := 0; i < 3; i++ {
		//check(processName[i], processs[i])
		log.Println(processs[i])
	}
	log.Println("pass TestBasic")
	fmt.Println()
}

func TestZkWatchWorker(t *testing.T) {
	conn, _, err0 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err0 != nil {
		panic(err0)
	}

	zkservice.InitEnv(conn)

	master := Master{}
	master.label = 1
	master.init()

	err1 := zkservice.CreateWorkParentPath(1, conn)
	if err1 != nil {
		panic(err1)
	}

	vshost := port("viewserver", 1)
	viewservice.StartServer(vshost)

	primaryhost := port("worker1-node1", 1)
	pbservice.StartServer(vshost, primaryhost)

	var acls = zk.WorldACL(zk.PermAll)
	_, err2 := conn.Create(zkservice.GetWorkViewServerPath(1), []byte(vshost), 0, acls) //persistent znode
	if err2 != nil {
		panic(err2)
	}

	_, err3 := conn.Create(zkservice.GetWorkPrimaryPath(1), []byte(primaryhost), 0, acls)
	if err3 != nil {
		panic(err3)
	}

	//check console
	time.Sleep(time.Hour)
}
