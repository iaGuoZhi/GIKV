package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"msservice"
	"net/rpc"
	"os"
	"pbservice"
	"strings"
	"time"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

var strarted = false
var startedMasters []string

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
	panic(err)
	return false
}

func printPrefix() {
	fmt.Print("GIKV> ")
}

func printInvalidCmd(t string) {
	if t != "" {
		fmt.Println("GIKV> unknow command " + t)
	}
}

func doPut(key string, value string) {
	args := pbservice.PutArgs{Key: key, Value: value}
	reply := pbservice.PutReply{}

	random := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(startedMasters))

	ok := false
	for !ok {
		ok = call(startedMasters[random], "Master.Put", &args, &reply)
	}
}

func doGet(key string) string {
	args := pbservice.GetArgs{Key: key}
	reply := pbservice.GetReply{}

	random := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(startedMasters))

	ok := false
	for !ok {
		ok = call(startedMasters[random], "Master.Get", &args, &reply)
	}
	return reply.Value
}

func doDelete(key string) {
	args := pbservice.DeleteArgs{Key: key}
	reply := pbservice.DeleteReply{}

	random := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(startedMasters))

	ok := false
	for !ok {
		ok = call(startedMasters[random], "Master.Delete", &args, &reply)
	}
}

func handleCmd(text string) {
	tokens := strings.Split(text, " ")

	if len(tokens) == 3 && tokens[0] == "put" || (len(tokens) == 2 && (tokens[0] == "get" || tokens[0] == "delete")) {
		if strarted == false {
			fmt.Println("GIKV has not been started")
			return
		}
		switch tokens[0] {
		case "put":
			{
				doPut(tokens[1], tokens[2])
			}
		case "get":
			{
				val := doGet(tokens[1])
				fmt.Println(val)
			}
		case "delete":
			{
				doDelete(tokens[1])
			}
		}
	} else {
		printInvalidCmd(text)
	}
}

func get(r *bufio.Reader) string {
	t, _ := r.ReadString('\n')
	return strings.TrimSpace(t)
}

func shouldContinue(text string) bool {
	if strings.EqualFold("exit", text) {
		return false
	}
	return true
}

func help() {
	fmt.Println("Welcome to GIKV! ")
	fmt.Println("GIKV is a distributed key-value store written by Guo-zhi using Go language")
	fmt.Println("This Are the Avaliable commands: ")
	fmt.Println("start  ----- start GIKV")
	fmt.Println("get $key  ----- get value of the key")
	fmt.Println("put $key $value  ----- update key's value ")
	fmt.Println("delete $key  ----- delete key from GIKV")
	fmt.Println("exit  ----- exit GIKV	 ")
}

func main() {
	commands := map[string]interface{}{
		"help":  help,
		"start": startGIKV,
	}
	reader := bufio.NewReader(os.Stdin)
	help()
	printPrefix()
	text := get(reader)
	for ; shouldContinue(text); text = get(reader) {
		if value, exists := commands[text]; exists {
			value.(func())()
		} else {
			handleCmd(text)
		}
		printPrefix()
	}
	fmt.Println("Bye!")

}

func startGIKV() {
	if !strarted {
		conn, _, err1 := zk.Connect([]string{zkservice.ZkServer}, time.Second)
		if err1 != nil {
			panic(err1)
		}

		zkservice.InitEnv(conn)

		for i := 0; i < 10; i++ {
			pbservice.StartWorker(i, conn)
		}

		// create master
		masters := [3]msservice.Master{}
		processName := [3]int{1, 2, 3}

		for i := 0; i < 3; i++ {
			masters[i].Init(processName[i])
		}

		children, _, err2 := conn.Children(zkservice.MasterProcessPath)
		if err2 != nil {
			panic(err2)
		}

		for _, child := range children {
			path := zkservice.GetMasterProcessPath(child)
			master, _, err3 := conn.Get(path)
			if err3 != nil {
				panic(err3)
			}
			startedMasters = append(startedMasters, string(master))
		}

		if len(startedMasters) <= 0 {
			fmt.Println("start GIKV fail")
		} else {
			strarted = true
			fmt.Println("GIKV started successfully")
		}
	} else {
		fmt.Println("GIKV already started")
	}
}
