package msservice

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type slaveMgr struct {
	connMap map[int]*net.TCPConn
	sessid  int
	sync.RWMutex
}

func (slavemgr *slaveMgr) RemConn(sessid int) {
	slavemgr.Lock()
	defer slavemgr.Unlock()
	delete(slavemgr.connMap, sessid)
}

func (slavemgr *slaveMgr) BroadcastData(data []byte) {
	slavemgr.Lock()
	defer slavemgr.Unlock()

	size := len(data)
	if size == 0 {
		return
	}

	sbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sbuf, uint32(size))

	for _, conn := range slavemgr.connMap {
		conn.Write(sbuf)
		conn.Write(data)
	}
}

func (slavemgr *slaveMgr) AddConn(conn *net.TCPConn) int {
	slavemgr.Lock()
	defer slavemgr.Unlock()

	slavemgr.sessid++
	slavemgr.connMap[slavemgr.sessid] = conn
	return slavemgr.sessid
}

// Myconn ... tbd
type Myconn struct {
	nodeName string
	sn       string
	newName  string
	bmaster  bool
	coon     *zk.Conn

	// for data sync
	msgQueue       chan string
	masterPort     string
	slavemgr       *slaveMgr
	slv2masterConn net.Conn
}

var masterZkNode string = "master"
var processZkNode string = "process_list"
var masterListenPort = "8010"

var packetSizeMax uint32 = 1024
var msgPoolSize uint32 = 102400

func (myconn *Myconn) getProcessNodeName(val string) string {
	return fmt.Sprintf("/%s/%s/%s", myconn.nodeName, processZkNode, val)
}

func (myconn *Myconn) getNodeName(val string) string {
	return fmt.Sprintf("/%s", filepath.Join(myconn.nodeName, val))
}

func (myconn *Myconn) init() {
	acls := zk.WorldACL(zk.PermAll)

	c, _, err0 := zk.Connect([]string{"127.0.0.1"}, time.Second)
	if err0 != nil {
		panic(err0)
	}
	myconn.coon = c
	myconn.masterPort = masterListenPort
	//check parent node exist
	exists, _, err1 := myconn.coon.Exists(fmt.Sprintf("/%s", myconn.nodeName))
	if err1 != nil {
		panic(err1)
	}
	if !exists {
		myconn.coon.Create(fmt.Sprintf("/%s", myconn.nodeName), []byte{}, 0, acls)
	}

	//try get master
	myconn.tryMaster()

	//create own temp node
	processNode := myconn.getProcessNodeName(myconn.sn)
	log.Println("node now is:", processNode)
	exists, _, err1 = myconn.coon.Exists(processNode)
	if err1 != nil {
		panic(err1)
	}
	if !exists {
		ret, err2 := myconn.coon.Create(processNode, []byte{}, zk.FlagEphemeral, acls)
		if err2 != nil {
			panic(err2)
		}
		log.Println("create self node: ", ret)
	}
}

func (myconn *Myconn) tryMaster() {
	acls := zk.WorldACL(zk.PermAll)
	masterNode := myconn.getNodeName(masterZkNode)

	_, err1 := myconn.coon.Create(masterNode, []byte(myconn.masterPort), zk.FlagEphemeral, acls)
	if err1 == nil {
		log.Println("now process is master")
		myconn.bmaster = true
		myconn.initMaster()
	} else {
		//panic(err1)
		myconn.bmaster = false
		masterByteInfo, _, err := myconn.coon.Get(masterNode)
		if err != nil {
			log.Println("fatal err:", err)
		}
		log.Println("current process is slave, master info: ", string(masterByteInfo))
		myconn.initSlave2MasterConn(string(masterByteInfo))

		// add watch on master process
		exists, _, evtCh, err0 := myconn.coon.ExistsW(masterNode)
		if err0 != nil || !exists {
			myconn.tryMaster()
		} else {
			myconn.handleMasterDownEvt(evtCh)
		}
	}
}

func (myconn *Myconn) initMaster() {
	myconn.slavemgr = &slaveMgr{connMap: make(map[int]*net.TCPConn), sessid: 0}

	// listen for slaves
	tcpAddr, err1 := net.ResolveTCPAddr("tcp4", ":"+myconn.masterPort)
	if err1 != nil {
		panic(err1)
	}
	tcpListener, err2 := net.ListenTCP("tcp4", tcpAddr)
	if err2 != nil {
		panic(err2)
	}

	go func() {
		for {
			tcpConn, err3 := tcpListener.AcceptTCP()
			if err3 != nil {
				panic(err3)
			}
			sessid := myconn.slavemgr.AddConn(tcpConn)
			log.Println(fmt.Sprintf("slave client:%s has connected! sessid: %d\n", tcpConn.RemoteAddr().String(), sessid))
			defer tcpConn.Close()
		}
	}()
}

func (myconn *Myconn) initSlave2MasterConn(master string) {
	var err1 error
	myconn.slv2masterConn, err1 = net.Dial("tcp", ":"+master)
	if err1 != nil {
		log.Println("initSlave2MasterConn failed: ", err1)
		return
	}

	go func() {
		for {
			hsize := make([]byte, 4)
			if _, err := io.ReadFull(myconn.slv2masterConn, hsize); err != nil {
				log.Println(err)
				myconn.slv2masterConn.Close()
				myconn.slv2masterConn = nil
				return
			}

			hsval := binary.LittleEndian.Uint32(hsize)
			if hsval > packetSizeMax {
				log.Println("packet size:", hsval, ",exceed max val:", packetSizeMax)
				myconn.slv2masterConn.Close()
				return
			}

			hbuf := make([]byte, hsval)
			if _, err := io.ReadFull(myconn.slv2masterConn, hbuf); err != nil {
				log.Println("read buf err:", err)
				myconn.slv2masterConn.Close()
				myconn.slv2masterConn = nil
				return
			}
			hbufstr := string(hbuf)
			myconn.msgQueue <- hbufstr
			log.Println("push into queue:", hbufstr, ", size:", len(myconn.msgQueue))
		}
	}()
}

func (myconn *Myconn) onMasterDown() {
	myconn.tryMaster()
}

func (myconn *Myconn) handleMasterDownEvt(ch <-chan zk.Event) {
	go func(chv <-chan zk.Event) {
		e := <-chv
		log.Println("handleMasterDownEvt: ", e)
		myconn.onMasterDown()
	}(ch)
}
