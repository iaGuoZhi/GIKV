package zookeeper

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ServiceNode struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type SdClient struct {
	zkServers []string // List of zk servers
	zkRoot    string   // Zk root path
	conn      *zk.Conn // Connection of zk
}

// New client of server discover.
func NewClient(zkServers []string, zkRoot string, timeout int) (*SdClient, error) {
	client := new(SdClient)
	client.zkServers = zkServers
	client.zkRoot = zkRoot

	conn, _, err := zk.Connect(zkServers, time.Duration(timeout)*time.Second)
	if err != nil {
		return nil, err
	}
	client.conn = conn

	if err := client.ensureRoot(); err != nil {
		return nil, err
	}
	return client, nil
}

// Close client.
func (s *SdClient) Close() {
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *SdClient) Register(node *ServiceNode) error {
	if err := s.ensureName(node.Name); err != nil {
		return err
	}
	path := s.zkRoot + "/" + node.Name + "/n"
	fmt.Println(node)
	data, err := json.Marshal(node)
	fmt.Println(data)
	fmt.Println(path)
	if err != nil {
		return err
	}
	_, err = s.conn.CreateProtectedEphemeralSequential(path, data, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

func (s *SdClient) ensureRoot() error {
	exists, _, err := s.conn.Exists(s.zkRoot)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.conn.Create(s.zkRoot, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

func (s *SdClient) ensureName(name string) error {
	path := s.zkRoot + "/" + name
	exists, _, err := s.conn.Exists(path)
	if err != nil {
		return err
	}
	if !exists {
		_, err := s.conn.Create(path, []byte(""), 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

// Get random Nodes by name.
func (s *SdClient) GetRandomNode(name string) (*ServiceNode, error) {
	path := s.zkRoot + "/" + name

	// Get service node by path
	children, _, err := s.conn.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return &ServiceNode{}, nil
		}
		return nil, err
	}

	var nodes []*ServiceNode
	for _, child := range children {
		fullPath := path + "/" + child
		data, _, err := s.conn.Get(fullPath)
		if err != nil {
			if err == zk.ErrNoNode {
				continue
			}
			return nil, err
		}
		node := new(ServiceNode)
		err = json.Unmarshal(data, node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	if nodes != nil {
		rand.Seed(time.Now().Unix())
		return nodes[rand.Intn(len(nodes))], nil
	} else {
		return nil, errors.New("service node list is nil")
	}
}
