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
	view     View // current view
	newView  View // next view
	update   bool // can view be updated?
	promote  bool // can backup be promoted to primary
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
	if v.Backup == args.Me {
		vs.promote = (args.Viewnum == v.Viewnum)
	}

	// Restarted primary treated as dead
	if v.Primary == args.Me && args.Viewnum == 0 {
		nv.Primary = ""
	}

	// change next view (like state machine)
	if nv.Primary == "" && nv.Backup == "" {
		nv.Primary = args.Me
		nv.Viewnum++
	} else if nv.Primary == "" {
		if vs.promote {
			nv.Primary = nv.Backup
			nv.Backup = args.Me
			if nv.Primary == nv.Backup {
				nv.Backup = ""
			}
			nv.Viewnum++
		}
	} else if nv.Backup == "" {
		if nv.Primary != args.Me {
			nv.Backup = args.Me
			nv.Viewnum++
		}
	} else {
		if args.Me != nv.Primary && args.Me != nv.Backup {
			// do nothing
		}
	}

	// update(and its condition)
	vs.update = vs.update || ((v.Primary == args.Me) && (v.Viewnum == args.Viewnum))
	// indeed update
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
	bk := &vs.newView.Backup

	vs.mu.Lock()
	if *pm != "" && nt.Sub(vs.lastPing[*pm]) > DeadPings*PingInterval {
		// fmt.Println("Primary has died", *pm)
		*pm = ""
		vs.newView.Viewnum++
	}
	if *bk != "" && nt.Sub(vs.lastPing[*bk]) > DeadPings*PingInterval {
		// fmt.Println("Backup has died", *bk)
		*bk = ""
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
	vs.view = View{0, "", ""}
	vs.view = View{0, "", ""}
	vs.update = false
	vs.promote = false
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
