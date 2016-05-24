package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	//"github.com/codegangsta/cli"
)

var strToLower = strings.ToLower

const (
	ClusterHashSlots          = 16384
	MigrateDefaultTimeout     = 60000
	MigrateDefaultPipeline    = 10
	RebalanceDefaultThreshold = 2
)

type RedisTrib struct {
	nodes   [](*ClusterNode)
	fix     bool
	errors  []error
	timeout int
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

func (self *RedisTrib) GetNodeByName(name string) (node *ClusterNode) {
	for _, node := range self.nodes {
		if strToLower(node.Info().name) == strToLower(name) {
			return node
		}
	}
	return nil
}

func (self *RedisTrib) GetNodeByAbbreviatedName() {
	return
}

func (self *RedisTrib) GetMasterWithLeastReplicas() {
	return
}

func (self *RedisTrib) ShowNodes() {
	for _, n := range self.nodes {
		logrus.Println(n.InfoString())
	}
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

func (self *RedisTrib) CheckConfigConsistency() {
	if !self.isConfigConsistent() {
		self.ClusterError("Nodes don't agree about configuration!")
	} else {
		logrus.Printf("[OK] All nodes agree about slots configuration.")
	}
}

func (self *RedisTrib) isConfigConsistent() bool {
	return true
}

func (self *RedisTrib) CheckOpenSlots() {
	logrus.Printf(">>> Check for open slots...")
	// add check open slots code.
}

func (self *RedisTrib) CheckSlotsCoverage() {
	logrus.Printf(">>> Check slots coverage...")
	slots := self.CoveredSlots()
	// add check open slots code.
	if len(slots) == ClusterHashSlots {
		logrus.Printf("[OK] All %d slots covered.", ClusterHashSlots)
	} else {
		self.ClusterError(fmt.Sprintf("Not all %d slots are covered by nodes.", ClusterHashSlots))
		//fix_slots_coverage if @fix
	}
}

// Merge slots of every known node. If the resulting slots are equal
// to ClusterHashSlots, then all slots are served.
func (self *RedisTrib) CoveredSlots() map[int]int {
	slots := make(map[int]int)
	return slots
}

func (self *RedisTrib) LoadClusterInfoFromNode(addr string) error {
	node := NewClusterNode(addr)

	if err := node.Connect(); err != nil {
		return err
	}

	if !node.AssertCluster() {
		//logrus.Fatalf("Node %s is not configured as a cluster node.", node)
		return fmt.Errorf("Node %s is not configured as a cluster node.", node)
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
		fnode.Connect()
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
		}
	}
}

func (self *RedisTrib) CheckClusterCmd(addr string) error {
	if addr == "" {
		return errors.New("Please check host:port for check command.")
	}

	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	self.CheckCluster(false)
	return nil
}
