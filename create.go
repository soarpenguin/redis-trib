package main

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

var createCommand = cli.Command{
	Name:      "create",
	Usage:     "create a new redis cluster.",
	ArgsUsage: `<host1:port1 ... hostN:portN>`,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "replicas, r",
			Value: 0,
			Usage: `Slave number for every master created, the default value is none.

    $ redis-trib create <--replicas 1> <host1:port1 ... hostN:portN>`,
		},
	},
	Action: func(context *cli.Context) error {
		if len(context.Args()) < 1 {
			logrus.Fatalf("Must provide at least one \"host:port\" for create command!")
		}

		rt := NewRedisTrib()
		if err := rt.CreateClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) CreateClusterCmd(context *cli.Context) error {
	self.SetReplicasNum(context.Int("replicas"))

	logrus.Printf(">>> Creating cluster")
	for _, addr := range context.Args() {
		if addr == "" {
			continue
		}
		node := NewClusterNode(addr)
		node.Connect(true)
		if !node.AssertCluster() {
			logrus.Fatalf("Node %s is not configured as a cluster node.", node.String())
		}
		node.LoadInfo(false)
		node.AssertEmpty()
		self.AddNode(node)
	}

	self.CheckCreateParameters()
	logrus.Printf(">>> Performing hash slots allocation on %d nodes...", len(self.nodes))
	self.AllocSlots()
	self.ShowNodes()
	YesOrDie("Can I set the above configuration?")
	self.FlushNodesConfig()
	logrus.Printf(">>> Nodes configuration updated")
	logrus.Printf(">>> Assign a different config epoch to each node")
	self.AssignConfigEpoch()
	logrus.Printf(">>> Sending CLUSTER MEET messages to join the cluster")
	self.JoinCluster()

	// Give one second for the join to start, in order to avoid that
	// wait_cluster_join will find all the nodes agree about the config as
	// they are still empty with unassigned slots.
	time.Sleep(time.Second * 1)
	self.WaitClusterJoin()
	self.FlushNodesConfig() // Useful for the replicas
	self.CheckCluster(false)
	return nil
}

func (self *RedisTrib) CheckCreateParameters() bool {
	repOpt := self.ReplicasNum()
	masters := len(self.nodes) / (repOpt + 1)

	if masters < 3 {
		logrus.Fatalf("*** ERROR: Invalid configuration for cluster creation.\n"+
			"\t   *** Redis Cluster requires at least 3 master nodes.\n"+
			"\t   *** This is not possible with %d nodes and %d replicas per node.\n"+
			"\t   *** At least %d nodes are required.", len(self.nodes), repOpt, 3*(repOpt+1))
	}
	return true
}

func (self *RedisTrib) FlushNodesConfig() {
	for _, node := range self.nodes {
		node.FlushNodeConfig()
	}
}

func (self *RedisTrib) JoinCluster() {
	var first *ClusterNode = nil
	var addr string

	for _, node := range self.nodes {
		if first == nil {
			first = node
			addr = fmt.Sprintf("%s:%d", node.Host(), node.Port())
			continue
		}
		node.ClusterAddNode(addr)
	}
}

func (self *RedisTrib) AllocSlots() {
	// TODO:
	var masters [](*ClusterNode)
	nodeNum := len(self.nodes)
	mastersNum := len(self.nodes) / (self.ReplicasNum() + 1)

	// The first step is to split instances by IP. This is useful as
	// we'll try to allocate master nodes in different physical machines
	// (as much as possible) and to allocate slaves of a given master in
	// different physical machines as well.
	//
	// This code assumes just that if the IP is different, than it is more
	// likely that the instance is running in a different physical host
	// or at least a different virtual machine.
	var ips map[string][](*ClusterNode)
	ips = make(map[string][](*ClusterNode))
	for _, node := range self.nodes {
		ips[node.Name()] = append(ips[node.Name()], node)
	}

	// Select master instances
	logrus.Printf("Using %d masters:", mastersNum)
	var interleaved [](*ClusterNode)
	stop := false
	for !stop {
		// Take one node from each IP until we run out of nodes
		// across every IP.
		for ip, nodes := range ips {
			if len(nodes) == 0 {
				// if this IP has no remaining nodes, check for termination
				if len(interleaved) == nodeNum {
					// stop when 'interleaved' has accumulated all nodes
					stop = true
					continue
				}
			} else {
				// else, move one node from this IP to 'interleaved'
				interleaved = append(interleaved, nodes[0])
				ips[ip] = nodes[1:]
			}
		}
	}

	masters = interleaved[:mastersNum]
	interleaved = interleaved[mastersNum:]
	nodeNum -= mastersNum

	for _, node := range masters {
		logrus.Printf("  -> %s", node.String())
	}

	// Alloc slots on masters
	slotsPerNode := float64(ClusterHashSlots) / float64(mastersNum)
	first := 0
	cursor := 0.0
	for index, node := range masters {
		last := Round(cursor + slotsPerNode - 1)
		if last > ClusterHashSlots || index == len(masters)-1 {
			last = ClusterHashSlots - 1
		}

		if last < first {
			last = first
		}

		node.AddSlots(first, last)
		first = last + 1
		cursor += slotsPerNode
	}
	// Select N replicas for every master.
	// We try to split the replicas among all the IPs with spare nodes
	// trying to avoid the host where the master is running, if possible.
	//
	// Note we loop two times.  The first loop assigns the requested
	// number of replicas to each master.  The second loop assigns any
	// remaining instances as extra replicas to masters.  Some masters
	// may end up with more than their requested number of replicas, but
	// all nodes will be used.
	assignVerbose := false
	assignedReplicas := 0
	var slave *ClusterNode
	var node *ClusterNode
	types := []string{"required", "unused"}

	for _, assign := range types {
		for _, m := range masters {
			assignedReplicas = 0
			for assignedReplicas < self.ReplicasNum() {
				if nodeNum == 0 {
					break
				}
				if assignVerbose {
					if assign == "required" {
						logrus.Printf("Requesting total of %d replicas (%d replicas assigned so far with %d total remaining).",
							self.ReplicasNum(), assignedReplicas, nodeNum)
					} else if assign == "unused" {
						logrus.Printf("Assigning extra instance to replication role too (%d remaining).", nodeNum)
					}
				}

				// Return the first node not matching our current master
				index := getNodeFromSlice(m, interleaved)
				if index != -1 {
					node = interleaved[index]
				} else {
					node = nil
				}

				// If we found a node, use it as a best-first match.
				// Otherwise, we didn't find a node on a different IP, so we
				// go ahead and use a same-IP replica.
				if node != nil {
					slave = node
					interleaved = append(interleaved[:index], interleaved[index+1:]...)
				} else {
					slave, interleaved = interleaved[0], interleaved[1:]
				}
				slave.SetReplicate(m.Name())
				nodeNum -= 1
				assignedReplicas += 1
				logrus.Printf("Adding replica %s to %s", slave.String(), m.String())

				// If we are in the "assign extra nodes" loop,
				// we want to assign one extra replica to each
				// master before repeating masters.
				// This break lets us assign extra replicas to masters
				// in a round-robin way.
				if assign == "unused" {
					break
				}
			}
		}
	}
	return
}

func getNodeFromSlice(m *ClusterNode, nodes [](*ClusterNode)) (index int) {
	if len(nodes) < 1 {
		return -1
	}

	for i, node := range nodes {
		if m.Host() != node.Host() {
			return i
		}
	}

	return -1
}
