package viewservice

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func check(t *testing.T, ck *Clerk, p string, b1 string, b2 string, n uint) {
	view, _ := ck.Get()
	if view.Primary != p {
		t.Fatalf("wanted primary %v, got %v", p, view.Primary)
	}
	if view.Backup[0] != b1 {
		t.Fatalf("wanted backup1 %v, got %v", b1, view.Backup[0])
	}
	if view.Backup[1] != b2 {
		t.Fatalf("wanted backup2 %v, got %v", b2, view.Backup[1])
	}
	if n != 0 && n != view.Viewnum {
		t.Fatalf("wanted viewnum %v, got %v", n, view.Viewnum)
	}
	if ck.Primary() != p {
		t.Fatalf("wanted primary %v, got %v", p, ck.Primary())
	}
}

func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "viewserver-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func Test1(t *testing.T) {
	runtime.GOMAXPROCS(4)

	vshost := port("v")
	vs := StartServer(vshost)

	ck1 := MakeClerk(port("1"), vshost)
	ck2 := MakeClerk(port("2"), vshost)
	ck3 := MakeClerk(port("3"), vshost)
	ck4 := MakeClerk(port("4"), vshost)

	if ck1.Primary() != "" {
		t.Fatalf("there was a primary too soon")
	}

	// very first primary
	fmt.Printf("Test: First primary ...\n")

	for i := 0; i < DeadPings*2; i++ {
		view, _ := ck1.Ping(0)
		if view.Primary == ck1.me {
			break
		}
		time.Sleep(PingInterval)
	}
	check(t, ck1, ck1.me, "", "", 1)
	fmt.Printf("  ... Passed\n")

	// very first backup
	fmt.Printf("Test: First backup ...\n")

	{
		vx, _ := ck1.Get()
		for i := 0; i < DeadPings*2; i++ {
			ck1.Ping(1)
			view, _ := ck2.Ping(0)
			if view.Backup[0] == ck2.me {
				break
			}
			time.Sleep(PingInterval)
		}
		check(t, ck1, ck1.me, ck2.me, "", vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// second backup
	fmt.Printf("Test: Second backup ...\n")

	{
		vx, _ := ck1.Get()
		for i := 0; i < DeadPings*2; i++ {
			ck1.Ping(vx.Viewnum)
			ck2.Ping(vx.Viewnum)
			view, _ := ck3.Ping(0)
			if view.Backup[1] == ck3.me {
				break
			}
			time.Sleep(PingInterval)
		}
		check(t, ck1, ck1.me, ck2.me, ck3.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// primary dies, backup should take over
	fmt.Printf("Test: Backup takes over if primary fails ...\n")

	{
		vx, _ := ck1.Get()
		ck1.Ping(vx.Viewnum)
		for i := 0; i < DeadPings*2; i++ {
			v, _ := ck2.Ping(vx.Viewnum)
			ck3.Ping(vx.Viewnum)
			if v.Primary == ck2.me && v.Backup[0] == ck3.me && v.Backup[1] == "" {
				break
			}
			time.Sleep(PingInterval)
		}
		check(t, ck2, ck2.me, ck3.me, "", vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// revive ck1, should become second backup
	fmt.Printf("Test: Restarted server becomes second backup ...\n")

	{
		vx, _ := ck2.Get()
		ck2.Ping(vx.Viewnum)
		for i := 0; i < DeadPings*2; i++ {
			ck1.Ping(0)
			ck3.Ping(vx.Viewnum)
			v, _ := ck2.Ping(vx.Viewnum)
			if v.Primary == ck2.me && v.Backup[0] == ck3.me && v.Backup[1] == ck1.me {
				break
			}
			time.Sleep(PingInterval)
		}
		check(t, ck2, ck2.me, ck3.me, ck1.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	// start ck4, kill the primary (ck2), the first backup (ck3)
	// should become the server, the second backup(ck1) become
	// first backup, and ck4 become second backup
	fmt.Printf("Test: Idle third server becomes backup if primary fails ...\n")

	{
		vx, _ := ck2.Get()
		ck2.Ping(vx.Viewnum)
		for i := 0; i < DeadPings*2; i++ {
			ck4.Ping(0)
			v, _ := ck3.Ping(vx.Viewnum)
			ck1.Ping(vx.Viewnum)
			if v.Primary == ck3.me && v.Backup[0] == ck1.me && v.Backup[1] == ck4.me {
				break
			}
			vx = v
			time.Sleep(PingInterval)
		}
		check(t, ck1, ck3.me, ck1.me, ck4.me, vx.Viewnum+1)
	}
	fmt.Printf("  ... Passed\n")

	vs.killViewServer()
}
