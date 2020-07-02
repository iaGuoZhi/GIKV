package zkservice

import (
	"fmt"
	"log"

	"github.com/samuel/go-zookeeper/zk"
)

/*每次重新部署时，运行前zk的结构

/GIKV
/GIKV/Master
/GIKV/Master/Slave
/GIKV/Master/Process
/GIKV/Worker
*/

//path
const MasterPath = string("/GIKV/Master")
const MasterMasterPath = string("/GIKV/Master/Master")
const MasterSlavePath = string("/GIKV/Master/Slave")
const MasterProcessPath = string("/GIKV/Master/Process")
const RootPath = string("/GIKV")
const WorkerPath = string("/GIKV/Worker")
const WorkerPrimayPath = string("/GIKV/Worker/Primary")
const WorkerViewServerPath = string("/GIKV/Worker/ViewServer")
const WorkerBackupPath = string("/GIKV/Worker/Backup")

const ZkServer = string("127.0.0.1:2181")

//node
const RootNode = string("GIKV")
const MasterNode = string("Master")
const MasterMasterNode = string("Master")
const MasterSlaveNode = string("Slave")
const WorkerNode = string("Worker")
const WorkerViewServerNOde = string("ViewServer")
const WorkerBackupNode = string("Backup")
const WorkerPrimaryNode = string("Primary")
const MasterProcessNode = string("Process")

func GetWorkPrimaryPath(label int) string {
	return fmt.Sprintf("/GIKV/Worker/%d/Primary", label)
}

func GetWorkViewServerPath(label int) string {
	return fmt.Sprintf("/GIKV/Worker/%d/ViewServer", label)
}

func GetWorkBackupPath(workerLabel int, slaveLabel int) string {
	return fmt.Sprintf("/GIKV/Worker/%d/Slave/%d", workerLabel, slaveLabel)
}

func GetWokrParentPath(workerLabel int) string {
	return fmt.Sprintf("/GIKV/Worker/%d", workerLabel)
}

func GetMasterProcessPath(masterLabel string) string {
	return fmt.Sprintf("/GIKV/Master/Process/%s", masterLabel)
}

func GetMasterSlavePath(slaveLabel string) string {
	return fmt.Sprintf("/GIKV/Master/Slave/%s", slaveLabel)
}

func CreateWorkParentPath(workerLabel int, conn *zk.Conn) error {
	exists, _, err0 := conn.Exists(WorkerPath)
	if err0 != nil {
		panic(err0)
	}
	if exists == false {
		// 临时节点
		_, err0 = conn.Create(WorkerPath, []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
		if err0 != nil {
			panic(err0)
		}
	}
	parentPath := GetWokrParentPath(workerLabel)
	fmt.Println("[ZooKeeper: ] create path: ", parentPath)
	_, err1 := conn.Create(parentPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err1 != nil {
		panic(err1)
	}
	return nil
}

func RecursiveDelete(c *zk.Conn, path string) error {
	children, _, err := c.Children(path)
	if err != nil && err != zk.ErrNoNode {
		log.Fatalf("err finding children of %s", path)
		return err
	}
	for _, child := range children {
		err := RecursiveDelete(c, path+"/"+child)
		if err != nil && err != zk.ErrNoNode {
			log.Fatalf("err deleting %s", child)
			return err
		}
	}

	// get version
	_, stat, err := c.Get(path)
	if err != nil && err != zk.ErrNoNode {
		log.Fatalf("err getting version of %s", path)
		return err
	}

	if err := c.Delete(path, stat.Version); err != nil && err != zk.ErrNoNode {
		return err
	}
	return nil
}

// InitEnv ... init zookeeper environment
func InitEnv(conn *zk.Conn) {
	RecursiveDelete(conn, RootPath)

	_, err := conn.Create(RootPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}

	_, err = conn.Create(WorkerPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	fmt.Println("[ZooKeeper: ] create path: ", WorkerPath)

	RecursiveDelete(conn, MasterPath)
	_, err = conn.Create(MasterPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	fmt.Println("[ZooKeeper: ] create path: ", MasterPath)

	_, err = conn.Create(MasterSlavePath, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	fmt.Println("[ZooKeeper: ] create path: ", MasterSlavePath)

	_, err = conn.Create(MasterProcessPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		panic(err)
	}
	fmt.Println("[ZooKeeper: ] create path: ", MasterProcessPath)
}
