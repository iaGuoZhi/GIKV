package consistentservice

import (
	"fmt"
	"log"
	"strings"
	"testing"
)

func ExampleNew() {
	c := New()
	c.Add("cacheA")
	c.Add("cacheB")
	c.Add("cacheC")
	users := []string{"user_mcnulty", "user_bunk", "user_omar", "user_bunny", "user_stringer"}
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
	// Output:
	// user_mcnulty => cacheA
	// user_bunk => cacheA
	// user_omar => cacheA
	// user_bunny => cacheC
	// user_stringer => cacheC
}

func ExampleAdd() {
	c := New()
	c.Add("cacheA")
	c.Add("cacheB")
	c.Add("cacheC")
	users := []string{"user_mcnulty", "user_bunk", "user_omar", "user_bunny", "user_stringer"}
	fmt.Println("initial state [A, B, C]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
	c.Add("cacheD")
	c.Add("cacheE")
	fmt.Println("\nwith cacheD, cacheE [A, B, C, D, E]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
	// Output:
	// initial state [A, B, C]
	// user_mcnulty => cacheA
	// user_bunk => cacheA
	// user_omar => cacheA
	// user_bunny => cacheC
	// user_stringer => cacheC
	//
	// with cacheD, cacheE [A, B, C, D, E]
	// user_mcnulty => cacheE
	// user_bunk => cacheA
	// user_omar => cacheA
	// user_bunny => cacheE
	// user_stringer => cacheE
}

func ExampleRemove() {
	c := New()
	c.Add("cacheA")
	c.Add("cacheB")
	c.Add("cacheC")
	users := []string{"user_mcnulty", "user_bunk", "user_omar", "user_bunny", "user_stringer"}
	fmt.Println("initial state [A, B, C]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
	c.Remove("cacheC")
	fmt.Println("\ncacheC removed [A, B]")
	for _, u := range users {
		server, err := c.Get(u)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s => %s\n", u, server)
	}
	// Output:
	// initial state [A, B, C]
	// user_mcnulty => cacheA
	// user_bunk => cacheA
	// user_omar => cacheA
	// user_bunny => cacheC
	// user_stringer => cacheC
	//
	// cacheC removed [A, B]
	// user_mcnulty => cacheA
	// user_bunk => cacheA
	// user_omar => cacheA
	// user_bunny => cacheB
	// user_stringer => cacheB
}

func TestGetNext(t *testing.T) {
	c := New()
	c.Add("cacheA")
	c.Add("cacheB")
	c.Add("cacheC")
	c.Add("cacheD")
	c.Add("cacheE")
	c.Add("cacheF")
	users := []string{"user_mcnulty", "user_bunk", "user_omar", "user_bunny", "user_stringer"}
	fmt.Println("initial state [A, B, C, D, E, F]")
	for _, u := range users {
		servers, err := c.GetN(u, 6)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(servers)
		for i := range servers {
			nextServer, err1 := c.GetNext(u, servers[i])
			if err1 != nil {
				log.Fatal(err1)
			}
			fmt.Println(nextServer)
			if i == 5 {
				if !(strings.Compare("", nextServer) == 0) {
					log.Fatalln("last node's next should be empty  ", nextServer)
				}
			} else {
				if !(strings.Compare(nextServer, servers[i+1]) == 0) {
					log.Fatalln("next server incorrect  ", nextServer, servers[i+1])
				}
			}
		}
	}
}
