package pbservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
	"utilservice"
	"viewservice"
)

// Get ... return key's value directly
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
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

// Put ... put value in db and forward put operation to backups
func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
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
		ok := false
		for !ok {
			ok = call(pb.view.Primary, "PBServer.Put", args, reply)
		}
	}
	return nil
}

// ForwardPut ... only used by backup
func (pb *PBServer) ForwardPut(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	reply.Err = OK
	pb.db[args.Key] = args.Value
	pb.mu.Unlock()
	return nil
}

// Delete ... delete key in db and forward delete to backups
func (pb *PBServer) Delete(args *DeleteArgs, reply *DeleteReply) error {

	if !pb.connected { // lose connection with viewserver
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.view.Primary == pb.me {
		pb.mu.Lock()
		reply.Err = OK
		delete(pb.db, args.Key)
		// send to backup
		for i := 0; i < viewservice.BackupNums; i++ {
			ok := false
			for !ok {
				if pb.view.Backup[i] == "" {
					break // no backup
				}
				ok = call(pb.view.Backup[i], "PBServer.ForwardDelete", args, reply)
			}
		}
		pb.mu.Unlock()
	} else {
		// fmt.Println("I am not primary")
		ok := false
		for !ok {
			ok = call(pb.view.Primary, "PBServer.Remove", args, reply)
		}
	}
	return nil
}

// ForwardDelete ... only used by backups
func (pb *PBServer) ForwardDelete(args *DeleteArgs, reply *DeleteReply) error {
	pb.mu.Lock()
	reply.Err = OK
	delete(pb.db, args.Key)
	pb.mu.Unlock()
	return nil
}

// MoveDB ... move db to a new joined node or recovered node
func (pb *PBServer) MoveDB(args *MoveDBArgs, reply *MoveDBReply) error {
	reply.Err = OK
	pb.mu.Lock()
	if len(pb.db) != 0 { // not an empty db
		// fmt.Println("Clear DB:", len(pb.db))
		for key := range pb.db {
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

// DropDB when a worker node is delete, DropDB will forward it'sdata to other worker node
func (pb *PBServer) DropDB(args *DropDBArgs, reply *DropDBReply) error {
	fmt.Println("drop")
	reply.Err = OK
	pb.mu.Lock()
	defer pb.mu.Unlock()

	masterRPCAddress := args.MasterPRCAddress

	if len(pb.db) != 0 {
		for k, v := range pb.db {
			nextNodeArgs := ConsistentNextArgs{CurServerLabel: args.MyWorkerLabel, Key: k}
			nextNodeReply := ConsistentNextReply{}
			ok := false
			for ok == false || nextNodeReply.Err != OK {
				ok = call(masterRPCAddress, "Master.GetNextNode", &nextNodeArgs, &nextNodeReply)
			}
			nextWorkerLabel := nextNodeReply.NextWorkerLabel
			nextWorkerPrimaryRPCAddress := nextNodeReply.NextWorkerPrimaryRPCAddress

			if utilservice.DebugMode {
				log.Printf("data %s from %s forward to %s\n", k, args.MyWorkerLabel, nextWorkerLabel)
			}

			putArgs := PutArgs{Key: k, Value: v}
			putReply := PutReply{}

			ok = false
			for !ok {
				utilservice.MyPrintln(nextWorkerPrimaryRPCAddress)
				ok = call(nextWorkerPrimaryRPCAddress, "PBServer.Put", &putArgs, &putReply)
				if putReply.Err != OK {
					log.Println(putReply.Err)
				}
			}
		}
	}
	return nil
}

// tick ... ping viewserver periodically , transite to new view
// primary should also manage transfer data to new backup.
func (pb *PBServer) tick() {
	backups := pb.view.Backup
	v, e := pb.vs.Ping(pb.view.Viewnum)
	if e == nil {
		pb.connected = true
		pb.view = v
		newbks := pb.view.Backup
		// Primary must move DB to new backup
		for i := 0; i < viewservice.BackupNums; i++ {
			if backups[i] != newbks[i] && newbks[i] != "" && pb.me == pb.view.Primary {
				pb.mu.Lock()
				MoveDB(newbks[i], pb.db)
				pb.mu.Unlock()
			}
		}
	} else {
		pb.connected = false
	}
}

// KillPBServer ... for testing
func (pb *PBServer) killPBServer() {
	pb.dead = true
	pb.l.Close()
}

// StartServer ... initialize PBServer and create a thread listen to master's request
// also should ping viewserver periodically
func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)

	pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: [2]string{"", ""}}
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

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				go rpcs.ServeConn(conn)

			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.killPBServer()
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
