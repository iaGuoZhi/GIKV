package msservice

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func check(target string, got string) {
	if target != got {
		log.Fatalf("Get(%v), expected %v", got, target)
	}
}
func TestZkBasic(t *testing.T) {
	masters := [3]Master{}
	processName := [3]string{"p1", "p2", "p3"}

	for i := 0; i < 3; i++ {
		masters[i].nodeName = "myconn"
		masters[i].sn = processName[i]
		masters[i].init()
	}

	conn, _, err0 := zk.Connect([]string{"127.0.0.1"}, time.Second)
	if err0 != nil {
		panic(err0)
	}

	// check master value
	masterPath := string("/myconn/master")
	masterValue, _, err1 := conn.Get(masterPath)
	if err1 != nil {
		panic(err1)
	}
	log.Println(string(masterValue))
	check("8010", string(masterValue))

	// check process list
	processPath := string("/myconn/process_list")
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