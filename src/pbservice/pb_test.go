package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
	"viewservice"
)

func check(ck *Clerk, key string, value string) {
	v := ck.Get(key)

	if v != value {
		log.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
}

func TestFunc(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "func"
	vshost := port(tag+"v", 1)
	viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)
	ck := MakeClerk(vshost, "")
	fmt.Printf("Test: Get,Put and Delete ...\n")
	const nservers = 3
	var sa [nservers]*PBServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartServer(vshost, port(tag, i+1))
	}

	for iters := 0; iters < viewservice.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary != "" && view.Backup[0] != "" && view.Backup[1] != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	// give p+b time to ack, initialize
	time.Sleep(viewservice.PingInterval * viewservice.DeadPings)

	// put
	ck.Put("001", "0001")
	ck.Put("002", "0002")

	// get
	check(ck, "001", "0001")
	check(ck, "002", "0002")
	check(ck, "003", KeyInexsitence)

	// delete
	ck.Delete("001")
	check(ck, "001", KeyInexsitence)
	ck.Delete("006") //delete inexsitent key
	ck.Put("001", "0001+")
	check(ck, "001", "0001+")

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].killPBServer()
	}
	time.Sleep(time.Second)
}

// test sequential backup join and primary fail
func TestBasicFail(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "basic"
	vshost := port(tag+"v", 1)
	viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	ck := MakeClerk(vshost, "")

	fmt.Printf("Test: Single primary, no backup ...\n")

	s1 := StartServer(vshost, port(tag, 1))

	deadtime := viewservice.PingInterval * viewservice.DeadPings
	time.Sleep(deadtime * 2)
	if vck.Primary() != s1.me {
		t.Fatal("first primary never formed view")
	}

	ck.Put("111", "v1")
	check(ck, "111", "v1")

	ck.Put("2", "v2")
	check(ck, "2", "v2")

	ck.Put("1", "v1a")
	check(ck, "1", "v1a")

	fmt.Printf("  ... Passed\n")

	// add a backup

	fmt.Printf("Test: Add a backup ...\n")

	s2 := StartServer(vshost, port(tag, 2))
	for i := 0; i < viewservice.DeadPings*2; i++ {
		v, _ := vck.Get()
		if v.Backup[0] == s2.me {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	v, _ := vck.Get()
	if v.Backup[0] != s2.me {
		t.Fatal("backup never came up")
	}

	ck.Put("3", "33")
	check(ck, "3", "33")

	// add another backup
	fmt.Printf("Test: Add a backup ...\n")

	s3 := StartServer(vshost, port(tag, 3))
	for i := 0; i < viewservice.DeadPings*2; i++ {
		v, _ := vck.Get()
		if v.Backup[1] == s3.me {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	v, _ = vck.Get()
	if v.Backup[1] != s3.me {
		t.Fatal("backup never came up")
	}

	ck.Put("3b", "33b")
	check(ck, "3b", "33b")

	// give the backup time to initialize
	time.Sleep(3 * viewservice.PingInterval)

	ck.Put("4", "44")
	check(ck, "4", "44")

	fmt.Printf("  ... Passed\n")

	// killPBServer the primary

	fmt.Printf("Test: Primary failure ...\n")

	s1.killPBServer()
	for i := 0; i < viewservice.DeadPings*2; i++ {
		v, _ := vck.Get()
		if v.Primary == s2.me {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	v, _ = vck.Get()
	if v.Primary != s2.me {
		t.Fatal("backup1 never switched to primary")
	}
	if v.Backup[0] != s3.me {
		t.Fatal("backup2 never switched to backup1")
	}

	check(ck, "1", "v1a")
	check(ck, "3", "33")
	check(ck, "3b", "33b")
	check(ck, "4", "44")

	fmt.Printf("  ... Passed\n")

	s1.killPBServer()
	s2.killPBServer()
	s3.killPBServer()
	time.Sleep(time.Second)
}

// put on the same key concurrently, check whether priamry and backups have same value
func TestConcurrentSame(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "cs"
	vshost := port(tag+"v", 1)
	viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	fmt.Printf("Test: Concurrent Put()s to the same key ...\n")

	const nservers = 3
	var sa [nservers]*PBServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartServer(vshost, port(tag, i+1))
	}

	for iters := 0; iters < viewservice.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary != "" && view.Backup[0] != "" && view.Backup[1] != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	// give p+b time to ack, initialize
	time.Sleep(viewservice.PingInterval * viewservice.DeadPings)

	done := false

	view1, _ := vck.Get()
	const nclients = 3
	const nkeys = 2
	for xi := 0; xi < nclients; xi++ {
		go func(i int) {
			ck := MakeClerk(vshost, "")
			rr := rand.New(rand.NewSource(int64(os.Getpid() + i)))
			for done == false {
				k := strconv.Itoa(rr.Int() % nkeys)
				v := strconv.Itoa(rr.Int())
				ck.Put(k, v)
			}
		}(xi)
	}

	time.Sleep(5 * time.Second)
	done = true
	time.Sleep(time.Second)

	// read from primary
	ck := MakeClerk(vshost, "")
	var vals [nkeys]string
	for i := 0; i < nkeys; i++ {
		vals[i] = ck.Get(strconv.Itoa(i))
		if vals[i] == "" {
			t.Fatalf("Get(%v) failed from primary", i)
		}
	}

	// killPBServer the primary
	for i := 0; i < nservers; i++ {
		if view1.Primary == sa[i].me {
			sa[i].killPBServer()
			break
		}
	}
	for iters := 0; iters < viewservice.DeadPings*2; iters++ {
		view, _ := vck.Get()
		if view.Primary == view1.Backup[0] {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}
	view2, _ := vck.Get()
	if view2.Primary != view1.Backup[0] {
		t.Fatal("wrong Primary")
	}

	// read from old backup
	for i := 0; i < nkeys; i++ {
		z := ck.Get(strconv.Itoa(i))
		if z != vals[i] {
			t.Fatalf("Get(%v) from backup; wanted %v, got %v", i, vals[i], z)
		}
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].killPBServer()
	}
	time.Sleep(time.Second)
}

// constant put/get while crashing and restarting servers
func TestRepeatedCrash(t *testing.T) {
	runtime.GOMAXPROCS(4)

	tag := "rc"
	vshost := port(tag+"v", 1)
	viewservice.StartServer(vshost)
	time.Sleep(time.Second)
	vck := viewservice.MakeClerk("", vshost)

	fmt.Printf("Test: Repeated failures/restarts ...\n")

	const nservers = 4
	var sa [nservers]*PBServer
	for i := 0; i < nservers; i++ {
		sa[i] = StartServer(vshost, port(tag, i+1))
	}

	for i := 0; i < viewservice.DeadPings; i++ {
		v, _ := vck.Get()
		if v.Primary != "" && v.Backup[0] != "" && v.Backup[1] != "" {
			break
		}
		time.Sleep(viewservice.PingInterval)
	}

	// wait a bit for primary to initialize backup
	time.Sleep(viewservice.DeadPings * viewservice.PingInterval)

	done := false

	go func() {
		// killPBServer and restart servers
		rr := rand.New(rand.NewSource(int64(os.Getpid())))
		for done == false {
			i := rr.Int() % nservers
			// fmt.Printf("%v killPBServering %v\n", ts(), 5001+i)
			sa[i].killPBServer()

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * viewservice.PingInterval * viewservice.DeadPings)

			sa[i] = StartServer(vshost, port(tag, i+1))

			// wait long enough for new view to form, backup to be initialized
			time.Sleep(2 * viewservice.PingInterval * viewservice.DeadPings)
		}
	}()

	const nth = 2
	var cha [nth]chan bool
	for xi := 0; xi < nth; xi++ {
		cha[xi] = make(chan bool)
		go func(i int) {
			ok := false
			defer func() { cha[i] <- ok }()
			ck := MakeClerk(vshost, "")
			data := map[string]string{}
			rr := rand.New(rand.NewSource(int64(os.Getpid() + i)))
			for done == false {
				k := strconv.Itoa((i * 1000000) + (rr.Int() % 10))
				wanted, ok := data[k]
				if ok {
					v := ck.Get(k)
					if v != wanted {
						t.Fatalf("key=%v wanted=%v got=%v", k, wanted, v)
					}
				}
				nv := strconv.Itoa(rr.Int())
				ck.Put(k, nv)
				data[k] = nv
				// if no sleep here, then server tick() threads do not get
				// enough time to Ping the viewserver.
				time.Sleep(10 * time.Millisecond)
			}
			ok = true
		}(xi)
	}

	time.Sleep(20 * time.Second)
	done = true

	for i := 0; i < nth; i++ {
		ok := <-cha[i]
		if ok == false {
			t.Fatal("child failed")
		}
	}

	ck := MakeClerk(vshost, "")
	ck.Put("aaa", "bbb")
	if v := ck.Get("aaa"); v != "bbb" {
		t.Fatalf("final Put/Get failed")
	}

	fmt.Printf("  ... Passed\n")

	for i := 0; i < nservers; i++ {
		sa[i].killPBServer()
	}
	time.Sleep(time.Second)
}
