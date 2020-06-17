package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

// Ping ... handle client's remote ping event
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

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

	// change next view
	if nv.Primary == "" && nv.Backup[0] == "" && nv.Backup[1] == "" {
		nv.Primary = args.Me
		nv.Viewnum++
	} else {
		if nv.Primary == "" {
			if vs.promote[0] && nv.Backup[0] != "" {
				nv.Primary = nv.Backup[0]
				nv.Backup[0] = "" // go to next if
				nv.Viewnum++
			}
		}
		if nv.Backup[0] == "" {
			if vs.promote[1] && nv.Backup[1] != "" {
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
	if vs.update && nv.Viewnum != v.Viewnum {
		nv.Viewnum = v.Viewnum + 1
		*v = *nv
		vs.update = false
	}
	reply.View = *v
	return nil
}

// Get ... serve client get rpc
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	reply.View = vs.view
	return nil
}

// tick ... tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view accordingly.
func (vs *ViewServer) tick() {

	nt := time.Now()
	pm := &vs.newView.Primary
	bk1 := &vs.newView.Backup[0]
	bk2 := &vs.newView.Backup[1]

	vs.mu.Lock()
	if *pm != "" && nt.Sub(vs.lastPing[*pm]) > DeadPings*PingInterval {
		//fmt.Println("Primary has died", *pm)
		*pm = ""
		vs.newView.Viewnum++
	}
	if *bk1 != "" && nt.Sub(vs.lastPing[*bk1]) > DeadPings*PingInterval {
		//fmt.Println("Backup1 has died", *bk1)
		*bk1 = ""
		vs.newView.Viewnum++
	}
	if *bk2 != "" && nt.Sub(vs.lastPing[*bk2]) > DeadPings*PingInterval {
		//fmt.Println("Backup2 has died", *bk2)
		*bk2 = ""
		vs.newView.Viewnum++
	}
	vs.mu.Unlock()

}

// KillViewServer ... kill viewserver down for testing.
func (vs *ViewServer) killViewServer() {
	vs.dead = true
	vs.l.Close()
}

// StartServer ... initialize view server and listen to client's rpc
func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.view = View{0, "", [2]string{"", ""}}
	vs.view = View{0, "", [2]string{"", ""}}
	vs.update = false
	vs.promote = [2]bool{false, false}
	vs.lastPing = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// receive connections from clients.
	os.Remove(vs.me)
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

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
				vs.killViewServer()
			}
		}
	}()

	// tick self
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
