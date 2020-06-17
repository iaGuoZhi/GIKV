package msservice

import (
	"consistentservice"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"viewservice"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	OK              = "OK"
	ErrNoZnode      = "NoZnode"
	ErrAlreadyAdded = "ErrAlreadyAdded"
	ErrOther        = "ErrOther"
)

type Err string

type Work struct {
	label  int
	vshost string
	vck    *viewservice.Clerk
}

// Master ... tbd
type Master struct {
	label   int
	newName string
	bmaster bool
	conn    *zk.Conn

	// for test
	dead       bool
	unreliable bool

	// rpc
	myRPCAddress string
	l            net.Listener
	mu           sync.Mutex

	// worker message
	workers map[int]Work

	// for distributed hash table
	consistent *consistentservice.Consistent
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "ms-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	panic(err)
	return false
}
