package msservice

import (
	"net"
	"sync"
	"viewservice"

	"github.com/samuel/go-zookeeper/zk"
)

// Master ... tbd
type Master struct {
	nodeName string
	sn       string
	newName  string
	bmaster  bool
	conn     *zk.Conn

	// worker message
	primaryRPCAddress string
	vshost            string
	vck               *viewservice.Clerk

	// for data sync
	connProxy      net.Conn
	msgQueue       chan string
	masterPort     string
	slavemgr       *slaveMgr
	slv2masterConn net.Conn
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
