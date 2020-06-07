package msservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"pbservice"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

var runningWorksNumber map[int]bool

// Worker ... work metadata
type Worker struct {
	vshost  string
	vs      *viewservice.ViewServer
	vck     *viewservice.Clerk
	servers [viewservice.ServerNums]*pbservice.PBServer

	dead       bool // for testing
	unreliable bool // for testing

	mu        sync.Mutex
	l         net.Listener
	connected bool
	me        string
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
func StartWorker(workerNumber int) *Worker {

	if isrunning, existed := runningWorksNumber[workerNumber]; existed && isrunning {
		log.Fatalf("worker number has been used")
	}
	//runningWorksNumber[workerNumber] = true

	worker := new(Worker)

	// view server
	worker.vshost = port("viewserver", workerNumber)
	worker.vs = viewservice.StartServer(worker.vshost)
	time.Sleep(time.Second)

	// db servers
	worker.vck = viewservice.MakeClerk("", worker.vshost)
	for i := 0; i < viewservice.ServerNums; i++ {
		worker.servers[i] = pbservice.StartServer(worker.vshost, port("worker-node"+string(workerNumber), i+1))
	}

	worker.dead = false
	worker.connected = true
	worker.unreliable = false

	// worker rpc
	worker.me = port("worker", workerNumber)
	rpcs := rpc.NewServer()
	rpcs.Register(worker)
	os.Remove(worker.me)
	l, err1 := net.Listen("unix", worker.me)
	if err1 != nil {
		log.Fatal("listen error: ", err1)
	}
	worker.l = l

	fmt.Printf("worker %d start\n", workerNumber)

	go func() {
		for worker.dead == false {
			conn, err := worker.l.Accept()
			if err == nil && worker.dead == false {
				if worker.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if worker.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && worker.dead == false {
				fmt.Printf("Worker(%v) accept: %v\n", worker.me, err.Error())
				worker.kill()
			}
		}
	}()

	return worker
}

func (worker *Worker) kill() {
	worker.dead = true
	worker.l.Close()
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (worker *Worker) Get(args *pbservice.GetArgs, reply *pbservice.GetReply) error {
	// Your code here.
	if !worker.connected {
		reply.Err = pbservice.ErrWrongServer
		return nil
	}

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
			reply.Value = pbservice.KeyInexsitence
		}
		time.Sleep(viewservice.PingInterval)
	}

	return nil
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (worker *Worker) Put(args *pbservice.PutArgs, reply *pbservice.PutReply) error {

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
	return nil
}

//
// tell the primary to delete key from db
// must keep trying until it succeeds.
//
func (worker *Worker) Delete(args *pbservice.DeleteArgs, reply *pbservice.DeleteReply) error {
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
	return nil
}
