package msservice

import (
	"dhtservice"
	"net"
	"sync"
	"viewservice"

	"github.com/samuel/go-zookeeper/zk"
)

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
	primaryRPCAddress string
	vshost            string
	vck               *viewservice.Clerk

	// worker message
	workers map[int]Work

	// for distributed hash table
	dht *dhtservice.Consistent

	// for data sync
	/*connProxy      net.Conn
	msgQueue       chan string
	masterPort     string
	slavemgr       *slaveMgr
	slv2masterConn net.Conn*/
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
