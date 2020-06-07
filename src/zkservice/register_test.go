package zookeeper

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	servers := []string{"127.0.0.1:2181"}
	client, err := NewClient(servers, "/api", 10)
	if err != nil {
		panic(err)
	}

	defer client.Close()

	node1 := &ServiceNode{"user", "127.0.0.1", 4000}
	node2 := &ServiceNode{"user", "127.0.0.1", 4001}
	node3 := &ServiceNode{"user", "127.0.0.1", 4002}

	if err := client.Register(node1); err != nil {
		panic(err)
	}
	if err := client.Register(node2); err != nil {
		panic(err)
	}
	if err := client.Register(node3); err != nil {
		panic(err)
	}

	node, err := client.GetRandomNode("user")
	if err != nil {
		panic(err)
	}

	t.Log(node)

}
