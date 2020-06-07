package msservice

import (
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func check(target string, got string) {
	if target != got {
		log.Fatalf("Get(%v), expected %v", got, target)
	}
}
func TestBasic(t *testing.T) {
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
	time.Sleep(2 * time.Second)
}

func TestSync(t *testing.T) {
	masters := [3]Master{}
	processName := [3]string{"p1", "p2", "p3"}

	for i := 0; i < 3; i++ {
		masters[i].nodeName = "myconn"
		masters[i].sn = processName[i]
		masters[i].init()
	}

	// write data to MasterPort
	go func() {
		conn, err := net.Dial("tcp", ":8010")
		if err != nil {
			panic(err)
		}
		conn.Write([]byte("12"))
		fmt.Printf("12")

	}()
	time.Sleep(time.Hour)
	log.Println("pass TestSync")
}
