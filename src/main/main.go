package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"path/filepath"

	"github.com/samuel/go-zookeeper/zk"
)

type Myconn struct {
	nodeName string
	sn       string
	newName  string
	bmaster  bool
	conn     *zk.Conn
}

var MASTER_ZK_NODE string = "master"
var PROCESS_ZK_NODE string = "process_list"
var MASTER_LISTEN string = ":8010"

func main() {

	node_sn := flag.String("s", "", "node name or sn")
	flag.Parse()
	if *node_sn == "" {
		panic("need node_sn input ")
	}

	g_conn := &Myconn{nodeName: "myconn", sn: *node_sn}
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
	acls := zk.WorldACL(zk.PermAll)
	masterNode := this.getNodeName(MASTER_ZK_NODE)
	fmt.Println(this.sn)
	_, err1 := this.conn.Create(masterNode, []byte(this.sn), zk.FlagEphemeral, acls)
	if err1 == nil {
		log.Println("now process is master ")
		this.bmaster = true
	} else {
		this.bmaster = false
		btary, _, err := this.conn.Get(masterNode)
		if err != nil {
			log.Println("fatal err:", err)
		}
		log.Println("now process is slave, master info: ", string(btary))
		//todo add watch on this process
		has, _, evtMaster, err0 := this.conn.ExistsW(masterNode)
		if err0 != nil || !has {
			this.tryMaster()
		} else {
			this.handleMasterDownEvt(evtMaster)
		}
	}
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

	//create own temp node
	node := this.getProcessNodeName(this.sn)
	log.Println("node now is:", node)
	ret, err := this.conn.Create(node, []byte{}, zk.FlagEphemeral, acls)
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
		log.Println("handleMasterDownEvt: ", e)
		this.onMasterDown()
	}(ch)
}
