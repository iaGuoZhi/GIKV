package msservice

import (
	"consistentservice"
	"net"
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

type AddWorkerArgs struct {
	label             int
	primaryRPCAddress string
	vshost            string
}
type AddWorkerReply struct {
	err Err
}

type Work struct {
	label             int
	primaryRPCAddress string
	vshost            string
	vck               *viewservice.Clerk
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

	// backups
	backupsRPCAddress map[int]string

	// worker message
	workers map[int]Work

	// for distributed hash table
	consistent *consistentservice.Consistent
}

type slaveMgr struct {
	connMap map[int]*net.TCPConn
	sessid  int
	sync.RWMutex
}

var masterListenPort string = "8010"
var proxyAddr string = "127.0.0.1:2100"

var packetSizeMax uint32 = 1024
var msgPoolSize uint32 = 102400
