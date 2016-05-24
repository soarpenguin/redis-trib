package main

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	//"time"

	"github.com/Sirupsen/logrus"
	//"github.com/gosexy/redis"
	"github.com/garyburd/redigo/redis"
)

const (
	UnusedHashSlot = iota
	NewHashSlot
)

type NodeInfo struct {
	host string
	port uint

	name       string
	addr       string
	flags      []string
	replicate  string
	pingSent   int
	pingRecv   int
	linkStatus string
	slots      map[int]int
	migrating  map[int]string
	importing  map[int]string
}

func (self *NodeInfo) HasFlag(flag string) bool {
	for _, f := range self.flags {
		if strings.Contains(f, flag) {
			return true
		}
	}
	return false
}

func (self *NodeInfo) String() string {
	return fmt.Sprintf("%s:%d", self.host, self.port)
}

type ClusterNode struct {
	r        redis.Conn
	info     *NodeInfo
	dirty    bool
	friends  [](*NodeInfo)
	replicas [](*NodeInfo)
}

func NewClusterNode(addr string) (node *ClusterNode) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		logrus.Fatal(err)
		return nil
	}

	p, _ := strconv.ParseUint(port, 10, 0)
	node = &ClusterNode{
		r: nil,
		info: &NodeInfo{
			host:      host,
			port:      uint(p),
			slots:     make(map[int]int),
			migrating: make(map[int]string),
			importing: make(map[int]string),
			replicate: "",
		},
		dirty: false,
	}

	return node
}

func (self *ClusterNode) Friends() []*NodeInfo {
	return self.friends
}

func (self *ClusterNode) Slots() map[int]int {
	return self.info.slots
}

func (self *ClusterNode) HasFlag(flag string) bool {
	for _, f := range self.info.flags {
		if strings.Contains(f, flag) {
			return true
		}
	}
	return false
}

func (self *ClusterNode) String() string {
	return fmt.Sprintf("%s:%d", self.info.host, self.info.port)
}

func (self *ClusterNode) Connect() (err error) {
	if self.r != nil {
		return nil
	}

	addr := fmt.Sprintf("%s:%d", self.info.host, self.info.port)
	//client, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
	client, err := redis.Dial("tcp", addr)
	if err != nil {
		logrus.Errorf("Sorry, can't connect to node %s", addr)
		return err
	}

	if _, err = client.Do("PING"); err != nil {
		logrus.Errorf("Sorry, ping node %s failed", addr)
		return err
	}

	self.r = client
	return nil
}

func (self *ClusterNode) Call(cmd string, args ...interface{}) (interface{}, error) {
	err := self.Connect()
	if err != nil {
		return nil, err
	}

	return self.r.Do(cmd, args...)
}

func (self *ClusterNode) AssertCluster() bool {
	info, err := redis.String(self.Call("INFO", "cluster"))
	if err != nil {
		return false
	}

	return strings.Contains(info, "cluster_enabled:1")
}

func (self *ClusterNode) AssertEmpty() bool {
	info, err := redis.String(self.Call("INFO", "keyspace"))
	if err != nil {
		return false
	}

	return strings.Contains(info, "# Keyspace")
}

func (self *ClusterNode) LoadInfo(getfriends bool) (err error) {
	var result string
	if result, err = redis.String(self.Call("CLUSTER", "NODES")); err != nil {
		return err
	}

	nodes := strings.Split(result, "\n")
	for _, val := range nodes {
		parts := strings.Split(val, " ")
		if len(parts) <= 3 {
			continue
		}

		sent, _ := strconv.ParseInt(parts[4], 0, 32)
		recv, _ := strconv.ParseInt(parts[5], 0, 32)
		host, port, _ := net.SplitHostPort(parts[1])
		p, _ := strconv.ParseUint(port, 10, 0)

		node := &NodeInfo{
			name:       parts[0],
			addr:       parts[1],
			flags:      strings.Split(parts[2], ","),
			replicate:  parts[3],
			pingSent:   int(sent),
			pingRecv:   int(recv),
			linkStatus: parts[6],

			host:      host,
			port:      uint(p),
			slots:     make(map[int]int),
			migrating: make(map[int]string),
			importing: make(map[int]string),
		}

		if parts[3] == "-" {
			node.replicate = ""
		}

		if strings.Contains(parts[2], "myself") {
			self.info = node
			for i := 8; i < len(parts); i++ {
				slots := parts[i]
				if strings.Contains(slots, "<") {
					slotStr := strings.Split(slots, "-<-")
					slotId, _ := strconv.Atoi(slotStr[0])
					self.info.importing[slotId] = slotStr[1]
				} else if strings.Contains(slots, ">") {
					slotStr := strings.Split(slots, "->-")
					slotId, _ := strconv.Atoi(slotStr[0])
					self.info.migrating[slotId] = slotStr[1]
				} else if strings.Contains(slots, "-") {
					slotStr := strings.Split(slots, "-")
					firstId, _ := strconv.Atoi(slotStr[0])
					lastId, _ := strconv.Atoi(slotStr[1])
					self.AddSlots(firstId, lastId)
				} else {
					firstId, _ := strconv.Atoi(slots)
					self.AddSlots(firstId, firstId)
				}
			}
		} else if getfriends {
			self.friends = append(self.friends, node)
		}
	}
	return nil
}

func (self *ClusterNode) AddSlots(start, end int) {
	for i := start; i <= end; i++ {
		self.info.slots[i] = NewHashSlot
	}
	self.dirty = true
}

func (self *ClusterNode) SetAsReplica(nodeId string) {
	self.info.replicate = nodeId
	self.dirty = true
}

func (self *ClusterNode) FlushNodeConfig() {
	if !self.dirty {
		return
	}

	if self.info.replicate != "" {
		// run replicate cmd
	} else {
		// run addslots cmd
	}

	self.dirty = false
}

func (self *ClusterNode) InfoString() (result string) {
	return ""
}

func (self *ClusterNode) GetConfigSignature() string {
	config := []string{}

	result, err := redis.String(self.Call("CLUSTER", "NODES"))
	if err != nil {
		return ""
	}

	nodes := strings.Split(result, "\n")
	for _, val := range nodes {
		parts := strings.Split(val, " ")
		if len(parts) <= 3 {
			continue
		}

		sig := parts[0] + ":"

		slots := []string{}
		if len(parts) > 7 {
			for i := 8; i < len(parts); i++ {
				p := parts[i]
				if !strings.Contains(p, "[") {
					slots = append(slots, p)
				}
			}
		}
		if len(slots) == 0 {
			continue
		}
		sort.Strings(slots)
		sig = sig + strings.Join(slots, ",")

		config = append(config, sig)
	}

	sort.Strings(config)
	return strings.Join(config, "|")
}

func (self *ClusterNode) Info() *NodeInfo {
	return self.info
}

func (self *ClusterNode) IsDirty() bool {
	return self.dirty
}

func (self *ClusterNode) R() redis.Conn {
	return self.r
}

func (self *ClusterNode) Replicate() string {
	return self.info.replicate
}
