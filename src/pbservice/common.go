package pbservice

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

// PBServer ... a PBServer may be primary or backup, which is dominated by view server.
// every PBServer in a worker node has the same db content, every put or delete operation
// on primary will be forwarded to backups
// In order to simulate the unreliability under distributed condition, dead variable
// are used to test
type PBServer struct {
	mu sync.Mutex
	l  net.Listener

	me        string
	vs        *viewservice.Clerk
	view      viewservice.View
	db        map[string]string
	connected bool

	// testing
	dead bool
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	KeyInexsitence = "___KEY__INEXISTENCE"

	ErrZeroWorker = "ErrZeroWorker"
)

type Err string

type PutArgs struct {
	Key   string
	Value string
}

type PutReply struct {
	Err Err
}

type DeleteArgs struct {
	Key string
}

type DeleteReply struct {
	Err Err
}

type KillArgs struct {
}

type KillReply struct {
}
type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveDBArgs struct {
	DB map[string]string
}

type MoveDBReply struct {
	Err Err
}

type DropDBArgs struct {
	MyWorkerLabel    string
	MasterPRCAddress string
}

type DropDBReply struct {
	Err Err
}

type ConsistentNextArgs struct {
	CurServerLabel string
	Key            string
}

type ConsistentNextReply struct {
	NextWorkerLabel             string
	NextWorkerPrimaryRPCAddress string
	Err                         Err
}

func port(tag string, host int) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "pb-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

// StartWorker ... encapsulate worker's start procedure
func StartWorker(workerLabel int, conn *zk.Conn) error {
	vshost := port("viewserver", workerLabel)
	viewservice.StartServer(vshost)

	var servers [3]string
	for i := 0; i < 3; i++ {
		prefix := fmt.Sprintf("worker%d-node", workerLabel)
		servers[i] = port(prefix, i)
		StartServer(vshost, servers[i])
	}

	// create zk path for viewserver
	zkservice.CreateWorkParentPath(workerLabel, conn)
	var acls = zk.WorldACL(zk.PermAll)
	vspath := zkservice.GetWorkViewServerPath(workerLabel)
	_, err2 := conn.Create(vspath, []byte(vshost), zk.FlagEphemeral, acls) //临时节点
	if err2 != nil {
		panic(err2)
	}
	fmt.Println("[ZooKeeper: ] create path: ", vspath)

	return nil
}
