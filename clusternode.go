package main

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
)

const (
	UnusedHashSlot = iota
	NewHashSlot
	AssignedHashSlot
)

var verbose bool = false

///////////////////////////////////////////////////////////
// detail info for redis node.
type NodeInfo struct {
	host string
	port uint

	name       string
	addr       string
	flags      []string
	replicate  string
	pingSent   int
	pingRecv   int
	weight     int
	balance    int
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

//////////////////////////////////////////////////////////
// struct of redis cluster node.
type ClusterNode struct {
	r             redis.Conn
	info          *NodeInfo
	dirty         bool
	friends       [](*NodeInfo)
	replicasNodes [](*ClusterNode)
}

func NewClusterNode(addr string) (node *ClusterNode) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		logrus.Fatalf("New cluster node error: %s!", err)
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

func (self *ClusterNode) Host() string {
	return self.info.host
}

func (self *ClusterNode) Port() uint {
	return self.info.port
}

func (self *ClusterNode) Name() string {
	return self.info.name
}

func (self *ClusterNode) HasFlag(flag string) bool {
	for _, f := range self.info.flags {
		if strings.Contains(f, flag) {
			return true
		}
	}
	return false
}

func (self *ClusterNode) Replicate() string {
	return self.info.replicate
}

func (self *ClusterNode) SetReplicate(nodeId string) {
	self.info.replicate = nodeId
	self.dirty = true
}

func (self *ClusterNode) Weight() int {
	return self.info.weight
}

func (self *ClusterNode) SetWeight(w int) {
	self.info.weight = w
}

func (self *ClusterNode) Balance() int {
	return self.info.balance
}

func (self *ClusterNode) SetBalance(balance int) {
	self.info.balance = balance
}

func (self *ClusterNode) Slots() map[int]int {
	return self.info.slots
}

func (self *ClusterNode) Migrating() map[int]string {
	return self.info.migrating
}

func (self *ClusterNode) Importing() map[int]string {
	return self.info.importing
}

func (self *ClusterNode) R() redis.Conn {
	return self.r
}

func (self *ClusterNode) Info() *NodeInfo {
	return self.info
}

func (self *ClusterNode) IsDirty() bool {
	return self.dirty
}

func (self *ClusterNode) Friends() []*NodeInfo {
	return self.friends
}

func (self *ClusterNode) ReplicasNodes() []*ClusterNode {
	return self.replicasNodes
}

func (self *ClusterNode) AddReplicasNode(node *ClusterNode) {
	self.replicasNodes = append(self.replicasNodes, node)
}

func (self *ClusterNode) String() string {
	return fmt.Sprintf("%s:%d", self.info.host, self.info.port)
}

func (self *ClusterNode) Connect(abort bool) (err error) {
	if self.r != nil {
		return nil
	}

	if verbose {
		logrus.Debug("Connecting to node %s", self.String())
	}

	addr := fmt.Sprintf("%s:%d", self.info.host, self.info.port)
	//client, err := redis.DialTimeout("tcp", addr, 0, 1*time.Second, 1*time.Second)
	client, err := redis.Dial("tcp", addr, redis.DialConnectTimeout(60*time.Second))
	if err != nil {
		if abort {
			logrus.Fatalf("Sorry, connect to node %s failed in abort mode!", addr)
		} else {
			logrus.Errorf("Sorry, can't connect to node %s!", addr)
			return err
		}
	}

	if _, err = client.Do("PING"); err != nil {
		if abort {
			logrus.Fatalf("Sorry, ping node %s failed in abort mode!", addr)
		} else {
			logrus.Errorf("Sorry, ping node %s failed!", addr)
			return err
		}
	}

	self.r = client
	return nil
}

func (self *ClusterNode) Call(cmd string, args ...interface{}) (interface{}, error) {
	err := self.Connect(true)
	if err != nil {
		return nil, err
	}

	return self.r.Do(cmd, args...)
}

func (self *ClusterNode) Dbsize() (int, error) {
	return redis.Int(self.Call("DBSIZE"))
}

func (self *ClusterNode) ClusterAddNode(addr string) (ret string, err error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("Bad format of host:port: %s!", addr)
	}
	return redis.String(self.Call("CLUSTER", "meet", host, port))
}

func (self *ClusterNode) ClusterReplicateWithNodeID(nodeid string) (ret string, err error) {
	return redis.String(self.Call("CLUSTER", "replicate", nodeid))
}

func (self *ClusterNode) ClusterForgetNodeID(nodeid string) (ret string, err error) {
	return redis.String(self.Call("CLUSTER", "forget", nodeid))
}

func (self *ClusterNode) ClusterNodeShutdown() (err error) {
	self.r.Send("SHUTDOWN")
	if err = self.r.Flush(); err != nil {
		return err
	}
	return nil
}

func (self *ClusterNode) ClusterCountKeysInSlot(slot int) (int, error) {
	return redis.Int(self.Call("CLUSTER", "countkeysinslot", slot))
}

func (self *ClusterNode) ClusterGetKeysInSlot(slot int, pipeline int) (string, error) {
	return redis.String(self.Call("CLUSTER", "getkeysinslot", slot, pipeline))
}

func (self *ClusterNode) ClusterSetSlot(slot int, cmd string) (string, error) {
	return redis.String(self.Call("CLUSTER", "setslot", slot, cmd, self.Name()))
}

func (self *ClusterNode) AssertCluster() bool {
	info, err := redis.String(self.Call("INFO", "cluster"))
	if err != nil ||
		!strings.Contains(info, "cluster_enabled:1") {
		return false
	}

	return true
}

func (self *ClusterNode) AssertEmpty() bool {

	info, err := redis.String(self.Call("INFO"))
	db0, e := redis.String(self.Call("INFO", "db0"))
	if err != nil || !strings.Contains(info, "cluster_known_nodes:1") ||
		e != nil || strings.Trim(db0, " ") == "" {
		logrus.Fatalf("Node %s is not empty. Either the node already knows other nodes (check with CLUSTER NODES) or contains some key in database 0.", self.String())
	}

	return true
}

func (self *ClusterNode) LoadInfo(getfriends bool) (err error) {
	var result string
	if result, err = redis.String(self.Call("CLUSTER", "NODES")); err != nil {
		return err
	}

	nodes := strings.Split(result, "\n")
	for _, val := range nodes {
		// name addr flags role ping_sent ping_recv link_status slots
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

func (self *ClusterNode) FlushNodeConfig() {
	if !self.dirty {
		return
	}

	if self.Replicate() != "" {
		// run replicate cmd
		if _, err := self.ClusterReplicateWithNodeID(self.Replicate()); err != nil {
			// If the cluster did not already joined it is possible that
			// the slave does not know the master node yet. So on errors
			// we return ASAP leaving the dirty flag set, to flush the
			// config later.
			return
		}
	} else {
		// TODO: run addslots cmd
		var array []int
		for s, value := range self.Slots() {
			if value == NewHashSlot {
				array = append(array, s)
				self.info.slots[s] = AssignedHashSlot
			}
			self.ClusterAddSlots(array)
		}
	}

	self.dirty = false
}

// XXX: check the error for call CLUSTER addslots
func (self *ClusterNode) ClusterAddSlots(args ...interface{}) (ret string, err error) {
	return redis.String(self.Call("CLUSTER", "addslots", args))
}

// XXX: check the error for call CLUSTER delslots
func (self *ClusterNode) ClusterDelSlots(args ...interface{}) (ret string, err error) {
	return redis.String(self.Call("CLUSTER", "delslots", args))
}

func (self *ClusterNode) ClusterBumpepoch() (ret string, err error) {
	return redis.String(self.Call("CLUSTER", "bumpepoch"))
}

func (self *ClusterNode) InfoString() (result string) {
	var role = "M"

	if !self.HasFlag("master") {
		role = "S"
	}

	keys := make([]int, 0, len(self.Slots()))

	for k := range self.Slots() {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	slotstr := MergeNumArray2NumRange(keys)

	if self.Replicate() != "" && self.dirty {
		result = fmt.Sprintf("S: %s %s", self.info.name, self.String())
	} else {
		// fix myself flag not the first element of []slots
		result = fmt.Sprintf("%s: %s %s\n\t   slots:%s (%d slots) %s",
			role, self.info.name, self.String(), slotstr, len(self.Slots()), strings.Join(self.info.flags[1:], ","))
	}

	if self.Replicate() != "" {
		result = result + fmt.Sprintf("\n\t   replicates %s", self.Replicate())
	} else {
		result = result + fmt.Sprintf("\n\t   %d additional replica(s)", len(self.replicasNodes))
	}

	return result
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

///////////////////////////////////////////////////////////
// some useful struct contains cluster node.
type ClusterArray []ClusterNode

func (c ClusterArray) Len() int {
	return len(c)
}

func (c ClusterArray) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c ClusterArray) Less(i, j int) bool {
	return len(c[i].Slots()) < len(c[j].Slots())
}

type MovedNode struct {
	Source ClusterNode
	Slot   int
}
