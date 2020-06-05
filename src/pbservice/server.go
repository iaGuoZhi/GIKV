package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	view       viewservice.View
	db         map[string]string
	connected  bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	if !pb.connected { // lose connection with viewserver
		reply.Err = ErrWrongServer
		return nil
	}

	reply.Err = OK
	pb.mu.Lock()
	val, ok := pb.db[args.Key] // unsafe
	pb.mu.Unlock()
	if ok {
		reply.Value = val
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
	}

	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	// fmt.Println("Put", *args)
	if !pb.connected { // lose connection with viewserver
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.view.Primary == pb.me {
		pb.mu.Lock()
		reply.Err = OK
		pb.db[args.Key] = args.Value

		// send to backup
		for i := 0; i < viewservice.BackupNums; i++ {
			ok := false
			for !ok {
				if pb.view.Backup[i] == "" {
					break // no backup
				}
				ok = call(pb.view.Backup[i], "PBServer.ForwardPut", args, reply)
			}
		}
		pb.mu.Unlock()
	} else {
		// fmt.Println("I am not primary")
		ok := false
		for !ok {
			ok = call(pb.view.Primary, "PBServer.Put", args, reply)
		}
	}
	return nil
}

// forward to backup
func (pb *PBServer) ForwardPut(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	reply.Err = OK
	pb.db[args.Key] = args.Value
	pb.mu.Unlock()
	return nil
}

// Receive DB
func (pb *PBServer) MoveDB(args *MoveDBArgs, reply *MoveDBReply) error {
	reply.Err = OK
	pb.mu.Lock()
	if len(pb.db) != 0 { // not an empty db
		// fmt.Println("Clear DB:", len(pb.db))
		for key, _ := range pb.db {
			delete(pb.db, key)
		}
	}

	for k, v := range args.DB {
		pb.db[k] = v
	}
	pb.mu.Unlock()
	// fmt.Println("Move DB:", len(args.DB))
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	backups := pb.view.Backup
	v, e := pb.vs.Ping(pb.view.Viewnum)
	if e == nil {
		pb.connected = true
		pb.view = v
		newbks := pb.view.Backup
		// Primary must move DB to new backup
		for i := 0; i < viewservice.BackupNums; i++ {
			if backups[i] != newbks[i] && newbks[i] != "" && pb.me == pb.view.Primary {
				// fmt.Println("Move to backup:", newbk, "sizw:",len(pb.db))
				pb.mu.Lock()
				MoveDB(newbks[i], pb.db) // defined in client.go
				pb.mu.Unlock()
			}
		}
	} else {
		pb.connected = false
		// fmt.Println(e)
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{0, "", [2]string{"", ""}}
	pb.db = make(map[string]string)
	pb.connected = true

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
