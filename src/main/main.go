package main

import (
	"bufio"
	"fmt"
	"msservice"
	"net/rpc"
	"os"
	"pbservice"
	"strconv"
	"strings"
	"time"
	"viewservice"
	"zkservice"

	"github.com/samuel/go-zookeeper/zk"
)

var strarted = false

var conn *zk.Conn

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

	masterRPCAddress, _, err2 := conn.Get(zkservice.MasterMasterPath)
	if err2 != nil {
		panic(err2)
	}

	ok := false
	for !ok {
		ok = call(string(masterRPCAddress), "Master.Put", &args, &reply)
	}
}

func doGet(key string) string {
	args := pbservice.GetArgs{Key: key}
	reply := pbservice.GetReply{}

	masterRPCAddress, _, err2 := conn.Get(zkservice.MasterMasterPath)
	if err2 != nil {
		panic(err2)
	}

	ok := false
	for !ok {
		ok = call(string(masterRPCAddress), "Master.Get", &args, &reply)
	}
	return reply.Value
}

func doDelete(key string) {
	args := pbservice.DeleteArgs{Key: key}
	reply := pbservice.DeleteReply{}

	masterRPCAddress, _, err2 := conn.Get(zkservice.MasterMasterPath)
	if err2 != nil {
		panic(err2)
	}

	ok := false
	for !ok {
		ok = call(string(masterRPCAddress), "Master.Delete", &args, &reply)
	}
}

func doKill(kind string, target string) bool {

	switch kind {
	case "-m":
		{
			zkservice.RecursiveDelete(conn, zkservice.MasterMasterPath)
			time.Sleep(time.Second)
			fmt.Println("master has been killed")
			return true
		}
	case "-v":
		{
			workerLabel, err1 := strconv.Atoi(target)
			if err1 != nil {
				return false
			}

			workerPath := zkservice.GetWokrParentPath(workerLabel)
			err2 := zkservice.RecursiveDelete(conn, workerPath)
			if err2 != nil {
				return false
			}
			time.Sleep(time.Second)
			fmt.Printf("viewserver %s as well as its worker has been killed \n", target)
			return true
		}
	case "-p":
		{
			workerLabel, err3 := strconv.Atoi(target)
			if err3 != nil {
				panic(err3)
			}
			vsPath := zkservice.GetWorkViewServerPath(workerLabel)
			vsAddress, _, err4 := conn.Get(vsPath)
			if err4 != nil {
				panic(err4)
			}

			vck := viewservice.MakeClerk("", string(vsAddress))
			vok := false
			var view viewservice.View
			for vok == false {
				view, vok = vck.Get()
			}
			primaryAddress := view.Primary
			killArgs := pbservice.KillArgs{}
			killReply := pbservice.KillReply{}
			call(primaryAddress, "PBServer.Kill", &killArgs, &killReply)
			time.Sleep(time.Millisecond * 100)
			fmt.Printf("primary of worker %s has been killed \n", target)
			return true
		}
	}
	return false
}

func doAdd(kind string, target string) bool {
	label, err := strconv.Atoi(target)
	if err != nil {
		return false
	}

	switch kind {
	case "-m":
		{
			master := msservice.Master{}
			master.Init(label)
			time.Sleep(time.Millisecond * 100)
			fmt.Printf("Master %s has been created\n", target)
			return true
		}
	case "-v":
		{
			pbservice.StartWorker(label, conn)
			time.Sleep(time.Millisecond * 100)
			fmt.Printf("ViewServer as well as its worker %s has been created\n", target)
			return true
		}
	}
	return false
}
func handleCmd(text string) {
	tokens := strings.Split(text, " ")

	if len(tokens) == 3 && (tokens[0] == "put" || tokens[0] == "kill" || tokens[0] == "add") || (len(tokens) == 2 && (tokens[0] == "get" || tokens[0] == "delete")) {
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
		case "kill":
			{
				if doKill(tokens[1], tokens[2]) == false {
					printInvalidCmd(text)
				}
			}
		case "add":
			{
				if doAdd(tokens[1], tokens[2]) == false {
					printInvalidCmd(text)
				}
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
	fmt.Println("ls  ----- get current master,slave,viewserver,primary,backup")
	fmt.Println("kill  ----- kill master(-m $master) or viewserver(-v $worker) or primary(-p $worker)")
	fmt.Println("add  ----- add master(-m $master) or viewserver(-v $worker)")
	fmt.Println("get $key  ----- get value of the key")
	fmt.Println("put $key $value  ----- update key's value ")
	fmt.Println("delete $key  ----- delete key from GIKV")
	fmt.Println("exit  ----- exit GIKV	 ")
}

func main() {
	commands := map[string]interface{}{
		"help":  help,
		"start": startGIKV,
		"ls":    lsGIKV,
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
		var err1 error
		conn, _, err1 = zk.Connect([]string{zkservice.ZkServer}, time.Second)
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

		strarted = true
		fmt.Println("GIKV started successfully")

	} else {
		fmt.Println("GIKV already started")
	}
}

func lsGIKV() {
	if !strarted {
		fmt.Println("GIKV has't been started")
		return
	}

	var masterAddress string
	var slavesAddress []string
	var workers []string
	var viewservers []string
	var primarys []string
	var backups [][2]string

	masterAddressByte, _, err2 := conn.Get(zkservice.MasterMasterPath)
	if err2 != nil {
		panic(err2)
	}
	masterAddress = string(masterAddressByte)

	slaves, _, err3 := conn.Children(zkservice.MasterSlavePath)
	if err3 != nil {
		panic(err3)
	}
	for _, slave := range slaves {
		slavePath := zkservice.GetMasterSlavePath(slave)
		slaveAddress, _, err3 := conn.Get(slavePath)
		if err3 != nil {
			panic(err3)
		}
		slavesAddress = append(slavesAddress, string(slaveAddress))
	}

	workers, _, err2 = conn.Children(zkservice.WorkerPath)
	if err2 != nil {
		panic(err2)
	}

	for _, worker := range workers {
		workerLabel, err3 := strconv.Atoi(worker)
		if err3 != nil {
			panic(err3)
		}
		vsPath := zkservice.GetWorkViewServerPath(workerLabel)
		vsAddress, _, err4 := conn.Get(vsPath)
		if err4 != nil {
			panic(err4)
		}
		viewservers = append(viewservers, string(vsAddress))

		vck := viewservice.MakeClerk("", string(vsAddress))
		vok := false
		var view viewservice.View
		for vok == false {
			view, vok = vck.Get()
		}
		primarys = append(primarys, view.Primary)
		backups = append(backups, view.Backup)
	}
	fmt.Println("GIKV current servers:")
	fmt.Println("Master:")
	fmt.Println(masterAddress)
	fmt.Println("Slaves of Master")
	fmt.Println(slavesAddress)
	fmt.Println("Workers")
	for i := range workers {
		fmt.Printf("Worker %s :\n", workers[i])
		fmt.Printf("    Viewserver: %s\n", viewservers[i])
		fmt.Printf("    Primary: %s\n", primarys[i])
		fmt.Println("    Backups: ", backups[i])
	}
}
