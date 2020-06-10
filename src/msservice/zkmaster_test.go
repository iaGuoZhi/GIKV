package msservice

import (
	"fmt"
	"log"
	"testing"
	"time"
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
	masterValue, _, err1 := conn.Get(zkservice.MasterPath)
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
	time.Sleep(time.Hour)
}
