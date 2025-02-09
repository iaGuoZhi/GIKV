package msservice

import (
	"consistentservice"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
	"utilservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

// Init ...
func (master *Master) Init(label int) {
	master.init(label)
}

func (master *Master) init(label int) {

	master.label = label
	c, _, err0 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
	if err0 != nil {
		panic(err0)
	}
	master.conn = c
	//check root path exist
	exists, _, err1 := master.conn.Exists(zkservice.RootPath)
	if err1 != nil || exists == false {
		panic(err1)
	}

	master.workers = make(map[int]Work)
	master.consistent = consistentservice.New()

	// getWorkInfo
	master.getWorkInfo()

	// start rpc server
	master.startServer()

	//try get master
	master.tryMaster()

	//create own process node
	master.createProcessNode()
}

func (master *Master) tryMaster() {

	acls := zk.WorldACL(zk.PermAll)
	_, err1 := master.conn.Create(zkservice.MasterMasterPath, []byte(master.myRPCAddress), 0, acls)
	if err1 == nil {
		utilservice.MyPrintln("now process is master")
		master.bmaster = true
		fmt.Println("[ZooKeeper: ] create path: ", zkservice.MasterMasterPath)
		fmt.Println("[ZooKeeper: ] new master RPC address: ", master.myRPCAddress)
	} else {
		master.bmaster = false
		// create slave node
		slaveNodePath := filepath.Join(zkservice.MasterSlavePath, strconv.Itoa(master.label))
		_, err2 := master.conn.Create(slaveNodePath, []byte(master.myRPCAddress), 0, acls)
		if err2 != nil {
			log.Println("create zk slave node fail path: ", slaveNodePath)
		}
		fmt.Println("[ZooKeeper: ] create path: ", slaveNodePath)

		masterByteInfo, _, err := master.conn.Get(zkservice.MasterMasterPath)
		if err != nil {
			log.Println("fatal err:", err)
		}
		if utilservice.DebugMode {
			log.Println("current process is slave, master info: ", string(masterByteInfo))
		}

		// add watch on master process
		exists, _, evtCh, err0 := master.conn.ExistsW(zkservice.MasterMasterPath)
		if err0 != nil || !exists {
			master.onMasterDown()
		} else {
			master.handleMasterDownEvt(evtCh)
		}
	}
}

func (master *Master) onMasterDown() {
	master.dropSlave()
	master.tryMaster()
}

func (master *Master) dropSlave() {
	slaveNodePath := filepath.Join(zkservice.MasterSlavePath, strconv.Itoa(master.label))
	zkservice.RecursiveDelete(master.conn, slaveNodePath)
}

func (master *Master) handleMasterDownEvt(ch <-chan zk.Event) {
	go func(chv <-chan zk.Event) {
		e := <-chv
		log.Println("handleMasterDownEvt: ", e)
		master.onMasterDown()
	}(ch)
}

// start rpc server which handle request from client
func (master *Master) startServer() {
	// master rpc
	master.myRPCAddress = port("master", master.label)
	rpcs := rpc.NewServer()
	rpcs.Register(master)
	os.Remove(master.myRPCAddress)

	l, err0 := net.Listen("unix", master.myRPCAddress)
	if err0 != nil {
		log.Fatal("Listen error: ", err0)
	}
	master.l = l

	if utilservice.DebugMode {
		fmt.Printf("master %d start rpc server", master.label)
	}

	go func() {
		for master.dead == false {
			conn, err := master.l.Accept()
			if err == nil && master.dead == false {
				if master.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if master.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && master.dead == false {
				fmt.Printf("Master(%v) accept: %v\n", master.myRPCAddress, err.Error())
				master.kill()
			}
		}
	}()
}

func (master *Master) kill() {
	master.dead = true
	master.l.Close()
}

func (master *Master) createProcessNode() {
	exists, _, err1 := master.conn.Exists(zkservice.MasterProcessPath)
	if err1 != nil || exists == false {
		panic(err1)
	}

	processNode := filepath.Join(zkservice.MasterProcessPath, strconv.Itoa(master.label))
	if utilservice.DebugMode {
		log.Println("node now is:", processNode)
	}
	exists, _, err1 = master.conn.Exists(processNode)
	if err1 != nil {
		panic(err1)
	}
	if !exists {
		acls := zk.WorldACL(zk.PermAll)
		ret, err2 := master.conn.Create(processNode, []byte(master.myRPCAddress), 0, acls)
		if err2 != nil {
			panic(err2)
		}
		fmt.Println("[ZooKeeper: ] create path: ", processNode)
		if utilservice.DebugMode {
			log.Println("create self node: ", ret)
		}
	}
}
