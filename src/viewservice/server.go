package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	view     View    // current view
	newView  View    // next view
	update   bool    // can view be updated?
	promote  [2]bool // can backup 1&2 be promoted
	lastPing map[string]time.Time
}

// Simpe Error
type Err struct {
	info string
}

func (e *Err) Error() string {
	return e.info + "\n"
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	vs.lastPing[args.Me] = time.Now()
	vs.mu.Unlock()
	v := &vs.view
	nv := &vs.newView // next view
	if v.Viewnum < args.Viewnum {
		return &Err{"Viewnum bigger!"}
	}

	// first time
	if v.Viewnum == 0 {
		nv.Primary = args.Me
		nv.Viewnum++
		*v = *nv
		reply.View = *v
		return nil
	}

	// is backup initialized?(can be promoted?)
	if v.Backup[0] == args.Me {
		vs.promote[0] = (args.Viewnum == v.Viewnum)
	}
	if v.Backup[1] == args.Me {
		vs.promote[1] = (args.Viewnum == v.Viewnum)
	}

	// Restarted primary treated as dead
	if v.Primary == args.Me && args.Viewnum == 0 {
		nv.Primary = ""
	}

	// change next view (like state machine)
	if nv.Primary == "" && nv.Backup[0] == "" && nv.Backup[1] == "" {
		nv.Primary = args.Me
		nv.Viewnum++
	} else {
		if nv.Primary == "" {
			if vs.promote[0] {
				nv.Primary = nv.Backup[0]
				nv.Backup[0] = "" // go to next if
				nv.Viewnum++
			}
		}
		if nv.Backup[0] == "" {
			if vs.promote[1] {
				if nv.Primary == "" {
					nv.Primary = nv.Backup[1]
					nv.Backup[1] = ""
					nv.Viewnum++
				} else {
					nv.Backup[0] = nv.Backup[1]
					if nv.Primary != args.Me && nv.Backup[0] != args.Me {
						nv.Backup[1] = args.Me
						nv.Viewnum++
					} else {
						nv.Backup[1] = ""
					}
				}
			}
		}

		if nv.Backup[0] == "" && nv.Primary != args.Me && nv.Backup[1] != args.Me {
			nv.Backup[0] = args.Me
			nv.Viewnum++
		}

		if nv.Backup[1] == "" && nv.Primary != args.Me && nv.Backup[0] != args.Me {
			nv.Backup[1] = args.Me
			nv.Viewnum++
		}
	}

	// update(and its condition)
	vs.update = vs.update || ((v.Primary == args.Me) && (v.Viewnum == args.Viewnum))
	// indeed update
	fmt.Println(vs.update)
	if vs.update && nv.Viewnum != v.Viewnum {
		nv.Viewnum = v.Viewnum + 1
		*v = *nv
		vs.update = false
	}
	reply.View = *v
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	nt := time.Now()
	pm := &vs.newView.Primary
	bk1 := &vs.newView.Backup[0]
	bk2 := &vs.newView.Backup[1]

	vs.mu.Lock()
	if *pm != "" && nt.Sub(vs.lastPing[*pm]) > DeadPings*PingInterval {
		fmt.Println("Primary has died", *pm)
		*pm = ""
		vs.newView.Viewnum++
	}
	if *bk1 != "" && nt.Sub(vs.lastPing[*bk1]) > DeadPings*PingInterval {
		fmt.Println("Backup1 has died", *bk1)
		*bk1 = ""
		vs.newView.Viewnum++
	}
	if *bk2 != "" && nt.Sub(vs.lastPing[*bk2]) > DeadPings*PingInterval {
		fmt.Println("Backup2 has died", *bk2)
		*bk2 = ""
		vs.newView.Viewnum++
	}
	vs.mu.Unlock()

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{0, "", [2]string{"", ""}}
	vs.view = View{0, "", [2]string{"", ""}}
	vs.update = false
	vs.promote = [2]bool{false, false}
	vs.lastPing = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
