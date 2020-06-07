package msservice

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"pbservice"
	"strconv"
	"time"
	"viewservice"
)

var runningWorksNumber map[int]bool

// Clerk ... viewservice.Clerk's wrap
type Clerk struct {
	vs *viewservice.Clerk
}

// MakeClerk ... makeclerk
func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	return ck
}

// Worker ... work metadata
type Worker struct {
	vshost  string
	vs      *viewservice.ViewServer
	vck     *viewservice.Clerk
	ck      *Clerk
	servers [viewservice.ServerNums]*pbservice.PBServer
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

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
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

	// fmt.Println(err)
	return false
}

func (worker *Worker) initWorker() {
	runningWorksNumber = make(map[int]bool)
}

// StartWorker ... call by master, worker.vshost should be set before
func (worker *Worker) StartWorker(workerNumber int) error {

	if isrunning, existed := runningWorksNumber[workerNumber]; existed && isrunning {
		log.Fatalf("worker number has been used")
	}

	worker.vshost = port("viewserver", workerNumber)
	worker.vs = viewservice.StartServer(worker.vshost)
	time.Sleep(time.Second)

	worker.vck = viewservice.MakeClerk("", worker.vshost)
	worker.ck = MakeClerk(worker.vshost, "")
	for i := 0; i < viewservice.ServerNums; i++ {
		worker.servers[i] = pbservice.StartServer(worker.vshost, port("worker-node"+string(workerNumber), i+1))
	}
	runningWorksNumber[workerNumber] = true

	fmt.Printf("worker %d start\n", workerNumber)
	return nil
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (worker *Worker) Get(key string) string {
	// Your code here.
	args := &pbservice.GetArgs{Key: key}
	var reply pbservice.GetReply
	reply.Err = pbservice.ErrWrongServer

	ok := false
	for !ok || reply.Err != pbservice.OK {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = worker.vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Get", args, &reply)
		}

		//key not exist
		if reply.Err == pbservice.ErrNoKey {
			return pbservice.KeyInexsitence
		}
		time.Sleep(viewservice.PingInterval)
	}

	return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (worker *Worker) Put(key string, value string) {
	args := &pbservice.PutArgs{Key: key, Value: value}
	var reply pbservice.PutReply

	ok := false
	for !ok {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = worker.vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Put", args, &reply)
		}
		// time.Sleep(viewservice.PingInterval)   // sleep will abort locks
	}
	if reply.Err != pbservice.OK {
		fmt.Println(reply.Err)
	}
}

//
// tell the primary to delete key from db
// must keep trying until it succeeds.
//
func (worker *Worker) Delete(key string) {
	// Your code here.
	args := &pbservice.DeleteArgs{Key: key}
	var reply pbservice.DeleteReply

	ok := false
	for !ok {
		vok := false
		var view viewservice.View
		for !vok {
			view, vok = worker.vck.Get()
		}
		srv := view.Primary
		if srv != "" {
			ok = call(srv, "PBServer.Delete", args, &reply)
		}
		// time.Sleep(viewservice.PingInterval)   // sleep will abort locks
	}
	if reply.Err != pbservice.OK {
		fmt.Println(reply.Err)
	}
}
