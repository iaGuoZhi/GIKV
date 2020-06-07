package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"path/filepath"

	"github.com/samuel/go-zookeeper/zk"
)

type slaveMgr struct {
	connMap map[int]*net.TCPConn
	sessid  int
	sync.RWMutex
}

func (this *slaveMgr) RemConn(sessid int) {
	this.Lock()
	defer this.Unlock()
	delete(this.connMap, sessid)
}

func (this *slaveMgr) BroadcastData(data []byte) {
	this.Lock()
	defer this.Unlock()

	size := len(data)
	if size == 0 {
		return
	}
	sbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sbuf, uint32(size))

	for _, conn := range this.connMap {
		conn.Write(sbuf)
		conn.Write(data)
	}
}

func (this *slaveMgr) AddConn(conn *net.TCPConn) int {
	this.Lock()
	defer this.Unlock()
	this.sessid++
	this.connMap[this.sessid] = conn
	return this.sessid
}

type Myconn struct {
	nodeName  string
	sn        string
	newName   string
	conn      *zk.Conn
	connProxy net.Conn
	msgQueue  chan string

	bmaster          bool
	masterListenPort string
	slvMgr           *slaveMgr
	slv2masterConn   net.Conn
}

var MASTER_ZK_NODE string = "master"
var PROCESS_ZK_NODE string = "process_list"
var MASTER_LISTEN string = ":8010"
var PROXY_ADDR string = "127.0.0.1:2100"

var PACKET_SIZE_MAX uint32 = 1024
var MSG_POOL_SIZE uint32 = 102400

func main() {

	node_sn := flag.String("s", "", "node name or sn")
	node_master_port := flag.String("p", "", "node port when as master")
	flag.Parse()
	if *node_sn == "" {
		panic("need node_sn input ")
	}
	if *node_master_port == "" {
		panic("need node_master_port input ")
	}

	g_conn := &Myconn{nodeName: "myconn", sn: *node_sn, msgQueue: make(chan string, MSG_POOL_SIZE), masterListenPort: *node_master_port}
	g_conn.init()

	time.Sleep(time.Hour)
}

func (this *Myconn) getProcessNodeName(val string) string {
	return fmt.Sprintf("/%s/%s/%s", this.nodeName, PROCESS_ZK_NODE, val)
}

func (this *Myconn) getNodeName(val string) string {
	return fmt.Sprintf("/%s", filepath.Join(this.nodeName, val))
}

func (this *Myconn) tryMaster() {
	if this.slv2masterConn != nil {
		this.slv2masterConn.Close()
	}
	acls := zk.WorldACL(zk.PermAll)
	masterNode := this.getNodeName(MASTER_ZK_NODE)
	_, err1 := this.conn.Create(masterNode, []byte(this.masterListenPort), zk.FlagEphemeral, acls)
	if err1 == nil {
		log.Println("now process is master ")
		this.bmaster = true
		this.initType([]byte("master"))
		this.initMaster()
	} else {
		this.bmaster = false
		btary, _, err := this.conn.Get(masterNode)
		if err != nil {
			log.Println("fatal err:", err)
		}
		this.initType([]byte("slave"))
		log.Println("now process is slave, master info: ", string(btary))
		this.initSlave2MasterConn(string(btary))
		//todo add watch on this process
		has, _, evtMaster, err0 := this.conn.ExistsW(masterNode)
		if err0 != nil || !has {
			this.tryMaster()
		} else {
			this.handleMasterDownEvt(evtMaster)
		}
	}
}

func (this *Myconn) initSlave2MasterConn(master string) {
	var err error
	this.slv2masterConn, err = net.Dial("tcp", ":"+master)
	if err != nil {
		log.Println("initSlave2MasterConn failed:", err)
		return
	}
	//start read thread
	go func() {
		for {
			hsize := make([]byte, 4)
			if _, err := io.ReadFull(this.slv2masterConn, hsize); err != nil {
				log.Println(err)
				this.slv2masterConn.Close()
				this.slv2masterConn = nil
				return
			}

			hsval := binary.LittleEndian.Uint32(hsize)
			if hsval > PACKET_SIZE_MAX {
				log.Println("packet size:", hsval, ",exceed max val:", PACKET_SIZE_MAX)
				this.slv2masterConn.Close()
				return
			}

			hbuf := make([]byte, hsval)
			if _, err := io.ReadFull(this.slv2masterConn, hbuf); err != nil {
				log.Println("read buf err:", err)
				this.slv2masterConn.Close()
				this.slv2masterConn = nil
				return
			}
			hbuf_str := string(hbuf)
			this.msgQueue <- hbuf_str
			log.Println("push into queue:", hbuf_str, ", size:", len(this.msgQueue))
		}
	}()

}

func (this *Myconn) initProxyConn() bool {
	if this.connProxy != nil {
		return true
	}
	var err error
	this.connProxy, err = net.Dial("tcp", PROXY_ADDR)
	if err != nil {
		log.Println("initProxyConn failed:", err)
		return false
	}
	//start read thread
	go func() {
		for {
			hsize := make([]byte, 4)
			if _, err := io.ReadFull(this.connProxy, hsize); err != nil {
				log.Println(err)
				this.connProxy.Close()
				this.connProxy = nil
				return
			}

			hsval := binary.LittleEndian.Uint32(hsize)
			if hsval > PACKET_SIZE_MAX {
				log.Println("packet size:", hsval, ",exceed max val:", PACKET_SIZE_MAX)
				this.connProxy.Close()
				this.connProxy = nil
				return
			}

			hbuf := make([]byte, hsval)
			if _, err := io.ReadFull(this.connProxy, hbuf); err != nil {
				log.Println("read buf err:", err)
				this.connProxy.Close()
				this.connProxy = nil
				return
			}
			hbuf_str := string(hbuf)
			this.msgQueue <- hbuf_str
			if this.bmaster {
				this.slvMgr.BroadcastData(hbuf)
				log.Println("push into slaves queue:", hbuf_str)
			}
			log.Println("push into queue:", hbuf_str, ", size:", len(this.msgQueue))
		}
	}()
	return true
}

func (this *Myconn) sendProxy(bt []byte) {
	bout := bytes.NewBuffer([]byte{})
	binary.Write(bout, binary.LittleEndian, uint32(len(bt)))
	binary.Write(bout, binary.LittleEndian, bt)
	this.connProxy.Write(bout.Bytes())
}

func (this *Myconn) initMaster() {
	this.slvMgr = &slaveMgr{connMap: make(map[int]*net.TCPConn), sessid: 0}

	//listen for slaves
	tcpAddr, err := net.ResolveTCPAddr("tcp4", ":"+this.masterListenPort)
	if err != nil {
		panic(err)
	}
	tcpListener, err := net.ListenTCP("tcp4", tcpAddr)
	if err != nil {
		panic(err)
	}
	//defer tcpListener.Close()

	go func() {
		for {
			tcpConn, err := tcpListener.AcceptTCP()
			if err != nil {
				panic(err)
			}
			sessid := this.slvMgr.AddConn(tcpConn)
			log.Println(fmt.Sprintf("slave client:%s has connected! sessid: %d\n", tcpConn.RemoteAddr().String(), sessid))
			defer tcpConn.Close()
			//go handleConn(tcpConn, sessid)
		}
	}()

}

func (this *Myconn) initType(bt []byte) {
	//connect to proxy
	if this.initProxyConn() == false {
		log.Println("initType failed !!!")
		return
	}
	this.sendProxy(bt)
}

func (this *Myconn) init() {
	acls := zk.WorldACL(zk.PermAll)

	c, _, err := zk.Connect([]string{"127.0.0.1"}, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	this.conn = c

	//check parent node exists
	has, _, err0 := this.conn.Exists(fmt.Sprintf("/%s", this.nodeName))
	if err0 != nil {
		panic(err0)
	}
	if !has {
		this.conn.Create(fmt.Sprintf("/%s", this.nodeName), []byte{}, 0, acls)
	}

	//try get master
	this.tryMaster()

	//sync msg queue to slaves

	//create own temp node
	node := this.getProcessNodeName(this.sn)
	log.Println("node now is:", node)
	ret, err := this.conn.Create(node, []byte(this.sn), zk.FlagEphemeral, acls)
	if err != nil {
		panic(err)
	}
	log.Println("create self sess node:", ret)
}

func (this *Myconn) onMasterDown() {
	this.tryMaster()
}

func (this *Myconn) handleMasterDownEvt(ch <-chan zk.Event) {
	go func(chv <-chan zk.Event) {
		e := <-chv
		log.Println("handleMasterDownEvt:", e)
		this.onMasterDown()
	}(ch)
}
