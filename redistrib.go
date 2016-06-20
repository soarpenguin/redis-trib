package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
)

var strToLower = strings.ToLower

const (
	ClusterHashSlots          = 16384
	MigrateDefaultTimeout     = 60000
	MigrateDefaultPipeline    = 10
	RebalanceDefaultThreshold = 2
)

type RedisTrib struct {
	nodes       [](*ClusterNode)
	fix         bool
	errors      []error
	timeout     int
	replicasNum int // used for create command -replicas
}

func NewRedisTrib() (rt *RedisTrib) {
	rt = &RedisTrib{
		fix:     false,
		timeout: MigrateDefaultTimeout,
	}

	return rt
}

func (self *RedisTrib) AddNode(node *ClusterNode) {
	self.nodes = append(self.nodes, node)
}

func (self *RedisTrib) ResetNodes() {
	self.nodes = []*ClusterNode{}
}

func (self *RedisTrib) ClusterError(err string) {
	self.errors = append(self.errors, errors.New(err))
	logrus.Errorf(err)
}

func (self *RedisTrib) Errors() []error {
	return self.errors
}

func (self *RedisTrib) ReplicasNum() int {
	return self.replicasNum
}

func (self *RedisTrib) SetReplicasNum(replicasNum int) {
	self.replicasNum = replicasNum
}

func (self *RedisTrib) GetNodeByName(name string) (node *ClusterNode) {
	for _, node := range self.nodes {
		if strToLower(node.Info().name) == strToLower(name) {
			return node
		}
	}
	return nil
}

// Like get_node_by_name but the specified name can be just the first
// part of the node ID as long as the prefix in unique across the
// cluster.
func (self *RedisTrib) GetNodeByAbbreviatedName(name string) (n *ClusterNode) {
	length := len(name)
	var candidates = []*ClusterNode{}

	name = strings.ToLower(name)
	for _, node := range self.nodes {
		if node.Name()[0:length] == name {
			candidates = append(candidates, node)
		}
	}

	if len(candidates) != 1 {
		return nil
	}

	return candidates[0]
}

// This function returns the master that has the least number of replicas
// in the cluster. If there are multiple masters with the same smaller
// number of replicas, one at random is returned.
func (self *RedisTrib) GetMasterWithLeastReplicas() (node *ClusterNode) {
	mnodes := [](*ClusterNode){}
	for _, node := range self.nodes {
		if node.HasFlag("master") {
			mnodes = append(mnodes, node)
		}
	}

	var j int
	for i, node := range mnodes {
		if i == 0 {
			j = i
			continue
		}

		if len(node.ReplicasNodes()) < len(mnodes[j].ReplicasNodes()) {
			j = i
		}
	}

	return mnodes[j]
}

func (self *RedisTrib) CheckCluster(quiet bool) {
	logrus.Printf(">>> Performing Cluster Check (using node %s).", self.nodes[0].String())

	if !quiet {
		self.ShowNodes()
	}

	self.CheckConfigConsistency()
	self.CheckOpenSlots()
	self.CheckSlotsCoverage()
}

func (self *RedisTrib) ShowClusterInfo() {
	masters := 0
	keys := 0

	for _, node := range self.nodes {
		if node.HasFlag("master") {
			dbsize, err := node.Dbsize()
			if err != nil {
				dbsize = 0
			}
			logrus.Printf("%s (%s...) -> %-5d keys | %d slots | %d slaves.",
				node.String(), node.Name()[0:8], dbsize, len(node.Slots()), len(node.ReplicasNodes()))
			masters += 1
			keys += dbsize
		}
	}

	logrus.Printf("[OK] %d keys in %d masters.", keys, masters)
	kpslot := float64(keys) / 16384.0
	logrus.Printf("%.2f keys per slot on average.", kpslot)
}

func (self *RedisTrib) ShowNodes() {
	for _, n := range self.nodes {
		logrus.Println(n.InfoString())
	}
}

// Redis Cluster config epoch collision resolution code is able to eventually
// set a different epoch to each node after a new cluster is created, but
// it is slow compared to assign a progressive config epoch to each node
// before joining the cluster. However we do just a best-effort try here
// since if we fail is not a problem.
func (self *RedisTrib) AssignConfigEpoch() {
	configEpoch := 1

	for _, node := range self.nodes {
		node.Call("CLUSTER", "set-config-epoch", configEpoch)
		configEpoch += 1
	}
}

func (self *RedisTrib) CheckConfigConsistency() {
	if !self.isConfigConsistent() {
		self.ClusterError("Nodes don't agree about configuration!")
	} else {
		logrus.Printf("[OK] All nodes agree about slots configuration.")
	}
}

func (self *RedisTrib) isConfigConsistent() bool {
	clean := true
	oldSig := ""
	for _, node := range self.nodes {
		if len(oldSig) == 0 {
			oldSig = node.GetConfigSignature()
		} else {
			newSig := node.GetConfigSignature()
			if oldSig != newSig {
				logrus.Errorf("Signatures don't match. Error in Config.")
				logrus.Errorf("Error came up when checking node %s", node.String())
				clean = false
				break
			}
		}
	}
	return clean
}

func (self *RedisTrib) WaitClusterJoin() bool {
	logrus.Printf("Waiting for the cluster to join")

	for {
		if !self.isConfigConsistent() {
			fmt.Printf(".")
			time.Sleep(time.Second * 1)
		} else {
			break
		}
	}

	return true
}

func (self *RedisTrib) CheckOpenSlots() {
	logrus.Printf(">>> Check for open slots...")
	// add check open slots code.
	var openSlots []string

	for _, node := range self.nodes {
		if len(node.Migrating()) > 0 {
			keys := make([]string, len(node.Migrating()))
			for k, _ := range node.Migrating() {
				keys = append(keys, strconv.Itoa(k))
			}
			self.ClusterError(fmt.Sprintf("Node %s has slots in migrating state (%s).",
				node.String(), strings.Join(keys, ",")))
			openSlots = append(openSlots, keys...)
		}
		if len(node.Importing()) > 0 {
			keys := make([]string, len(node.Importing()))
			for k, _ := range node.Importing() {
				keys = append(keys, strconv.Itoa(k))
			}
			self.ClusterError(fmt.Sprintf("Node %s has slots in importing state (%s).",
				node.String(), strings.Join(keys, ",")))
			openSlots = append(openSlots, keys...)
		}
	}
	uniq := Uniq(openSlots)
	if len(uniq) > 0 {
		logrus.Warnf("The following slots are open: %s", strings.Join(uniq, ", "))
	}
	if self.fix {
		for _, slot := range uniq {
			self.FixOpenSlot(slot)
		}
	}
}

// Slot 'slot' was found to be in importing or migrating state in one or
// more nodes. This function fixes this condition by migrating keys where
// it seems more sensible.
func (self *RedisTrib) FixOpenSlot(slot string) {
	logrus.Printf(">>> Fixing open slot %s", slot)
	// TODO: add fix open slot code here

	// Try to obtain the current slot owner, according to the current
	// nodes configuration.
}

// Return the owner of the specified slot
func (self *RedisTrib) GetSlotOwners(slot int) [](*ClusterNode) {
	var owners [](*ClusterNode)
	for _, node := range self.nodes {
		if node.HasFlag("slave") {
			continue
		}
		if slot, ok := node.Slots()[slot]; ok {
			owners = append(owners, node)
		}
	}
	return owners
}

func (self *RedisTrib) CheckSlotsCoverage() {
	logrus.Printf(">>> Check slots coverage...")
	slots := self.CoveredSlots()
	// add check open slots code.
	if len(slots) == ClusterHashSlots {
		logrus.Printf("[OK] All %d slots covered.", ClusterHashSlots)
	} else {
		self.ClusterError(fmt.Sprintf("Not all %d slots are covered by nodes.", ClusterHashSlots))
		// TODO: fix_slots_coverage if @fix
	}
}

// Merge slots of every known node. If the resulting slots are equal
// to ClusterHashSlots, then all slots are served.
func (self *RedisTrib) CoveredSlots() map[int]int {
	slots := make(map[int]int)

	for _, node := range self.nodes {
		for key, value := range node.Slots() {
			slots[key] = value
		}
	}
	return slots
}

func (self *RedisTrib) LoadClusterInfoFromNode(addr string) error {
	node := NewClusterNode(addr)

	if err := node.Connect(true); err != nil {
		return err
	}

	if !node.AssertCluster() {
		logrus.Fatalf("Node %s is not configured as a cluster node.", node.String())
	}
	if err := node.LoadInfo(true); err != nil {
		//logrus.Fatalf("Load info from node %s failed.", node)
		return fmt.Errorf("Load info from node %s failed.", node)
	}
	self.AddNode(node)

	for _, n := range node.Friends() {
		//if n.HasFlag("noaddr") || n.HasFlag("disconnected") || n.HasFlag("fail") {
		//	continue
		//}

		fnode := NewClusterNode(n.String())
		fnode.Connect(false)
		if fnode.R() == nil {
			continue
		}

		fnode.LoadInfo(false)
		self.AddNode(fnode)
	}

	self.PopulateNodesReplicasInfo()
	return nil
}

// This function is called by LoadClusterInfoFromNode in order to
// add additional information to every node as a list of replicas.
func (self *RedisTrib) PopulateNodesReplicasInfo() {
	// Populate the replicas field using the replicate field of slave
	// nodes.
	for _, node := range self.nodes {
		if node.Replicate() != "" {
			master := self.GetNodeByName(node.Replicate())
			if master == nil {
				logrus.Warnf("*** %s claims to be slave of unknown node ID %s.", node.String(), node.Replicate())
			}
			// append master to node.replicate array
			master.AddReplicasNode(node)
		}
	}
}

// get from https://github.com/badboy/redis-trib.go
type InterfaceErrorCombo struct {
	result interface{}
	err    error
}

type EachFunction func(*ClusterNode, interface{}, error, string, []interface{})

func (self *RedisTrib) EachRunCommand(f EachFunction, cmd string, args ...interface{}) ([]*InterfaceErrorCombo, error) {
	nodes := self.nodes

	ies := make([]*InterfaceErrorCombo, len(nodes))

	for i, node := range nodes {
		val, err := node.Call(cmd, args...)
		ie := &InterfaceErrorCombo{val, err}
		ies[i] = ie

		if f != nil {
			f(node, val, err, cmd, args)
		}
	}

	return ies, nil
}

func (self *RedisTrib) EachPrint(cmd string, args ...interface{}) ([]*InterfaceErrorCombo, error) {
	return self.EachRunCommand(
		func(node *ClusterNode, result interface{}, err error, cmd string, args []interface{}) {
			val, _ := redis.String(result, err)

			if len(args) > 0 {
				strArgs := ToStringArray(args)
				logrus.Printf("%s: %s %s \n%s",
					node.String(), cmd, strings.Join(strArgs, " "), strings.Trim(val, " \n"))
			} else {
				logrus.Printf("%s: %s\n%s", node.String(), cmd, strings.Trim(val, " \n"))
			}

			if err != nil {
				logrus.Println(err)
			}
		}, cmd, args...)
}

//  Move slots between source and target nodes using MIGRATE.
//
//  Options:
//  :verbose -- Print a dot for every moved key.
//  :fix     -- We are moving in the context of a fix. Use REPLACE.
//  :cold    -- Move keys without opening slots / reconfiguring the nodes.
//  :update  -- Update nodes.info[:slots] for source/target nodes.
//  :quiet   -- Don't print info messages.
func MoveSlot(src, target ClusterNode, slot string, args []interface{}) {

}
