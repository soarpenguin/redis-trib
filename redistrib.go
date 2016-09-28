package main

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
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

func (self *RedisTrib) SetFix(fix bool) {
	self.fix = fix
}

func (self *RedisTrib) ClusterError(err string) {
	self.errors = append(self.errors, errors.New(err))
	logrus.Errorf(err)
}

func (self *RedisTrib) Errors() []error {
	return self.errors
}

func (self *RedisTrib) Timeout() int {
	return self.timeout
}

func (self *RedisTrib) SetTimeout(timeout int) {
	self.timeout = timeout
}

func (self *RedisTrib) ReplicasNum() int {
	return self.replicasNum
}

func (self *RedisTrib) SetReplicasNum(replicasNum int) {
	self.replicasNum = replicasNum
}

// Return the node with the specified ID or Nil.
func (self *RedisTrib) GetNodeByName(name string) (node *ClusterNode) {
	for _, node := range self.nodes {
		if strToLower(node.Name()) == strToLower(name) {
			return node
		}
	}
	return nil
}

// Like GetNodeByName but the specified name can be just the first
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
				//logrus.Errorf("Signatures don't match. Error in Config.")
				//logrus.Errorf("Error came up when checking node %s", node.String())
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

// Return the node, among 'nodes' with the greatest number of keys
// in the specified slot.
func (self *RedisTrib) GetNodeWithMostKeysInSlot(nodes []*ClusterNode, slot int) (node *ClusterNode) {
	var best *ClusterNode
	bestNumkeys := 0

	for _, n := range nodes {
		if n.HasFlag("slave") {
			continue
		}
		if numkeys, err := n.ClusterCountKeysInSlot(slot); err == nil {
			if numkeys > bestNumkeys || best == nil {
				best = n
				bestNumkeys = numkeys
			}
		}
	}
	return best
}

// Slot 'slot' was found to be in importing or migrating state in one or
// more nodes. This function fixes this condition by migrating keys where
// it seems more sensible.
func (self *RedisTrib) FixOpenSlot(slot string) {
	logrus.Printf(">>> Fixing open slot %s", slot)

	slotnum, err := strconv.Atoi(slot)
	if err != nil {
		logrus.Warnf("Bad slot num: \"%s\" for FixOpenSlot!", slot)
	}

	// Try to obtain the current slot owner, according to the current
	// nodes configuration.
	var owner *ClusterNode
	owners := self.GetSlotOwners(slotnum)
	if len(owners) == 1 {
		owner = owners[0]
	}

	var migrating [](*ClusterNode)
	var importing [](*ClusterNode)
	for _, node := range self.nodes {
		if node.HasFlag("slave") {
			continue
		}

		if _, ok := node.Migrating()[slotnum]; ok {
			migrating = append(migrating, node)
		} else if _, ok := node.Importing()[slotnum]; ok {
			importing = append(importing, node)
		} else {
			// TODO: fix countkeysinslot
			num, _ := node.ClusterCountKeysInSlot(slotnum)
			if num > 0 && node != owner {
				logrus.Printf("*** Found keys about slot %s in node %s!", slot, node.String())
				importing = append(importing, node)
			}
		}
	}
	logrus.Printf("Set as migrating in: %s", ClusterNodeArray2String(migrating))
	logrus.Printf("Set as importing in: %s", ClusterNodeArray2String(importing))

	// If there is no slot owner, set as owner the slot with the biggest
	// number of keys, among the set of migrating / importing nodes.
	if owner == nil {
		logrus.Printf(">>> Nobody claims ownership, selecting an owner...")
		owner = self.GetNodeWithMostKeysInSlot(self.nodes, slotnum)

		// If we still don't have an owner, we can't fix it.
		if owner == nil {
			logrus.Fatalf("[ERR] Can't select a slot owner. Impossible to fix.")
		}

		// TODO: add fix open slot code here
		// Use ADDSLOTS to assign the slot.
		logrus.Printf("*** Configuring %s as the slot owner", owner.String())
		owner.ClusterSetSlot(slotnum, "stable")
		owner.ClusterAddSlots(slotnum)
		// Make sure this information will propagate. Not strictly needed
		// since there is no past owner, so all the other nodes will accept
		// whatever epoch this node will claim the slot with.
		owner.ClusterBumpepoch()

		// Remove the owner from the list of migrating/importing
		// nodes.
		//migrating.delete(owner)
		//importing.delete(owner)
	}

	// If there are multiple owners of the slot, we need to fix it
	// so that a single node is the owner and all the other nodes
	// are in importing state. Later the fix can be handled by one
	// of the base cases above.
	//
	// Note that this case also covers multiple nodes having the slot
	// in migrating state, since migrating is a valid state only for
	// slot owners.
	if len(owners) > 1 {
		owner = self.GetNodeWithMostKeysInSlot(owners, slotnum)
		for _, node := range owners {
			if node == owner {
				continue
			}

			node.ClusterDelSlots(slotnum)
			//n.r.cluster('setslot',slot,'importing',owner.info[:name])
			//importing.delete(n) # Avoid duplciates
			//importing << n
		}
		owner.ClusterBumpepoch()
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

func (self *RedisTrib) NotCoveredSlots() []int {
	index := 0
	slots := []int{}
	coveredSlots := self.CoveredSlots()

	for index <= ClusterHashSlots {
		if _, ok := coveredSlots[index]; !ok {
			slots = append(slots, index)
		}
	}
	return slots
}

func (self *RedisTrib) CheckSlotsCoverage() {
	logrus.Printf(">>> Check slots coverage...")
	slots := self.CoveredSlots()
	// add check open slots code.
	if len(slots) == ClusterHashSlots {
		logrus.Printf("[OK] All %d slots covered.", ClusterHashSlots)
	} else {
		self.ClusterError(fmt.Sprintf("Not all %d slots are covered by nodes.", ClusterHashSlots))
		if self.fix {
			self.FixSlotsCoverage()
		}
	}
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

func (self *RedisTrib) NodesWithKeysInSlot(slot int) (nodes [](*ClusterNode)) {
	for _, node := range self.nodes {
		if node.HasFlag("slave") {
			continue
		}

		ret, err := node.ClusterGetKeysInSlot(slot, 1)
		if err == nil && len(ret) > 0 {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (self *RedisTrib) FixSlotsCoverage() {
	notCovered := self.NotCoveredSlots()

	logrus.Printf(">>> Fixing slots coverage...")
	logrus.Printf("List of not covered slots: %s", NumArray2String(notCovered))

	// For every slot, take action depending on the actual condition:
	// 1) No node has keys for this slot.
	// 2) A single node has keys for this slot.
	// 3) Multiple nodes have keys for this slot.
	slots := make(map[int][](*ClusterNode))
	for _, slot := range notCovered {
		nodes := self.NodesWithKeysInSlot(slot)
		slots[slot] = append(slots[slot], nodes...)
		var nodesStr string
		for _, node := range nodes {
			nodesStr = nodesStr + node.String() + ","
		}
		logrus.Printf("Slot %d has keys in %d nodes: %s", slot, len(nodes), nodesStr)
	}

	none := []int{}
	single := []int{}
	multi := []int{}
	for index, nodes := range slots {
		if len(nodes) == 0 {
			none = append(none, index)
		} else if len(nodes) == 1 {
			single = append(single, index)
		} else if len(nodes) > 1 {
			multi = append(multi, index)
		}
	}

	// Handle case "1": keys in no node.
	if len(none) > 0 {
		result := NumArray2String(none)
		logrus.Printf("The folowing uncovered slots have no keys across the cluster: %s", result)
		YesOrDie("Fix these slots by covering with a random node?")
		for _, slot := range none {
			node := self.nodes[rand.Intn(len(self.nodes))]
			logrus.Printf(">>> Covering slot %d with %s.", slot, node.String())
			node.ClusterAddSlots(slot)
		}
	}

	// Handle case "2": keys only in one node.
	if len(single) > 0 {
		result := NumArray2String(single)
		logrus.Printf("The folowing uncovered slots have keys in just one node: %s", result)
		YesOrDie("Fix these slots by covering with those nodes?")
		for _, slot := range single {
			node := slots[slot][0]
			logrus.Printf(">>> Covering slot %d with %s", slot, node.String())
			node.ClusterAddSlots(slot)
		}
	}

	// Handle case "3": keys in multiple nodes.
	if len(multi) > 0 {
		result := NumArray2String(multi)
		logrus.Printf("The folowing uncovered slots have keys in multiple nodes: %s", result)
		YesOrDie("Fix these slots by moving keys into a single node?")
		for _, slot := range multi {
			target := self.GetNodeWithMostKeysInSlot(slots[slot], slot)
			if target != nil {
				logrus.Printf(">>> Covering slot %d moving keys to %s", slot, target.String())
				target.ClusterAddSlots(slot)
				target.ClusterSetSlot(slot, "stable")
				nodes := slots[slot]
				for _, src := range nodes {
					if src == target {
						continue
					}

					// TODO:
					// Set the source node in 'importing' state (even if we will
					// actually migrate keys away) in order to avoid receiving
					// redirections for MIGRATE.
					src.ClusterSetSlot(slot, "importing")
					//move_slot(src,target,slot,:dots=>true,:fix=>true,:cold=>true)
					src.ClusterAddSlots(slot)
				}
			}
		}
	}
}

// Return the owner of the specified slot
func (self *RedisTrib) GetSlotOwners(slot int) [](*ClusterNode) {
	var owners [](*ClusterNode)

	for _, node := range self.nodes {
		if node.HasFlag("slave") {
			continue
		}
		if _, ok := node.Slots()[slot]; ok {
			owners = append(owners, node)
		}
	}
	return owners
}

func (self *RedisTrib) LoadClusterInfoFromNode(addr string) error {
	node := NewClusterNode(addr)

	node.Connect(true)
	if !node.AssertCluster() {
		logrus.Fatalf("Node %s is not configured as a cluster node.", node.String())
	}
	if err := node.LoadInfo(true); err != nil {
		return fmt.Errorf("Load info from node %s failed.", node)
	}
	self.AddNode(node)

	for _, n := range node.Friends() {
		if n.HasFlag("noaddr") || n.HasFlag("disconnected") || n.HasFlag("fail") {
			continue
		}

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
			} else {
				// append master to node.replicate array
				master.AddReplicasNode(node)
			}
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

// Option struct for move slot
type MoveOpts struct {
	Dots     bool
	Pipeline int
	Verbose  bool
	Fix      bool
	Cold     bool
	Update   bool
	Quiet    bool
}

//  Move slots between source and target nodes using MIGRATE.
//
//  Options:
//  :verbose -- Print a dot for every moved key.
//  :fix     -- We are moving in the context of a fix. Use REPLACE.
//  :cold    -- Move keys without opening slots / reconfiguring the nodes.
//  :update  -- Update nodes.info[:slots] for source/target nodes.
//  :quiet   -- Don't print info messages.
func (self *RedisTrib) MoveSlot(source *MovedNode, target *ClusterNode, o *MoveOpts) {
	// TODO: add move slot code
	if o.Pipeline <= 0 {
		o.Pipeline = MigrateDefaultPipeline
	}

	// We start marking the slot as importing in the destination node,
	// and the slot as migrating in the target host. Note that the order of
	// the operations is important, as otherwise a client may be redirected
	// to the target node that does not yet know it is importing this slot.
	if !o.Quiet {
		logrus.Printf("Moving slot %d from %s to %s: ", source.Slot, source.Source.InfoString(), target.InfoString())
	}

	if !o.Cold {
		target.ClusterSetSlot(source.Slot, "importing")
		source.Source.ClusterSetSlot(source.Slot, "migrating")
	}

	// Migrate all the keys from source to target using the MIGRATE command
	for {
		keys, err := source.Source.ClusterGetKeysInSlot(source.Slot, o.Pipeline)
		if err != nil {
			if len(keys) == 0 {
				break
			}
		}
		if o.Dots {
			logrus.Printf("%s", strings.Repeat(".", len(keys)))
		}

		//keys = source.r.cluster("getkeysinslot",slot,o[:pipeline])
		//break if keys.length == 0
		//begin
		//    source.r.client.call(["migrate",target.info[:host],target.info[:port],"",0,@timeout,:keys,*keys])
		//rescue => e
		//    if o[:fix] && e.to_s =~ /BUSYKEY/
		//        xputs "*** Target key exists. Replacing it for FIX."
		//        source.r.client.call(["migrate",target.info[:host],target.info[:port],"",0,@timeout,:replace,:keys,*keys])
		//    else
		//        puts ""
		//        xputs "[ERR] Calling MIGRATE: #{e}"
		//        exit 1
		//    end
		//end
		//print "."*keys.length if o[:dots]
		//STDOUT.flush
	}

	//puts if !o[:quiet]
	if !o.Quiet {
		logrus.Printf("\n")
	}

	// Set the new node as the owner of the slot in all the known nodes.
	if !o.Cold {
		for _, n := range self.nodes {
			if n.HasFlag("slave") {
				continue
			}
			//  n.r.cluster("setslot",slot,"node",target.info[:name])
		}
	}

	// Update the node logical config
	if o.Update {
		//source.info[:slots].delete(slot)
		//target.info[:slots][slot] = true
	}
}

// Given a list of source nodes return a "resharding plan"
// with what slots to move in order to move "numslots" slots to another
// instance.
func (self *RedisTrib) ComputeReshardTable(sources ClusterArray, numSlots int) []*MovedNode {
	// defined in clusternode.go
	var moved []*MovedNode
	// Sort from bigger to smaller instance, for two reasons:
	// 1) If we take less slots than instances it is better to start
	//    getting from the biggest instances.
	// 2) We take one slot more from the first instance in the case of not
	//    perfect divisibility. Like we have 3 nodes and need to get 10
	//    slots, we take 4 from the first, and 3 from the rest. So the
	//    biggest is always the first.
	sort.Sort(ClusterArray(sources))

	sourceTotSlots := 0
	for _, node := range sources {
		sourceTotSlots += len(node.Slots())
	}

	for idx, node := range sources {
		n := float64(numSlots) / float64(sourceTotSlots*len(node.Slots()))

		if idx == 0 {
			n = math.Ceil(n)
		} else {
			n = math.Floor(n)
		}

		keys := make([]int, len(node.Slots()))
		i := 0
		for k, _ := range node.Slots() {
			keys[i] = k
			i++
		}
		sort.Ints(keys)

		for i := 0; i < int(n); i++ {
			if len(moved) < numSlots {
				mnode := &MovedNode{
					Source: node,
					Slot:   keys[i],
				}
				moved = append(moved, mnode)
			}
		}
	}
	return moved
}

func (self *RedisTrib) ShowReshardTable(table []*MovedNode) {
	for _, node := range table {
		logrus.Printf("    Moving slot %d from %s", node.Slot, node.Source.Name())
	}
}
