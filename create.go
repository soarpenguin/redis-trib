// +build linux

package main

import (
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
			Name: "replicas, r",
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
			//logrus.Errorf("%p", err)
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
		node.AssertCluster()
		node.LoadInfo(false)
		//node.AssertEmpty()
		self.AddNode(node)
	}

	self.CheckCreateParameters(self.ReplicasNum())
	logrus.Printf(">>> Performing hash slots allocation on %d nodes...", len(self.nodes))
	self.AllocSlots(self.ReplicasNum())
	self.ShowNodes()
	YesOrDie("Can I set the above configuration?")
	// flush_nodes_config
	logrus.Printf(">>> Nodes configuration updated")
	logrus.Printf(">>> Assign a different config epoch to each node")
	self.AssignConfigEpoch()
	logrus.Printf(">>> Sending CLUSTER MEET messages to join the cluster")
	// join_cluster

	// Give one second for the join to start, in order to avoid that
	// wait_cluster_join will find all the nodes agree about the config as
	// they are still empty with unassigned slots.
	time.Sleep(time.Second * 1)
	self.WaitClusterJoin()
	//flush_nodes_config # Useful for the replicas
	self.CheckCluster(false)
	return nil
}

func (self *RedisTrib) CheckCreateParameters(repOpt int) bool {
	masters := len(self.nodes) / (repOpt + 1)

	if masters < 3 {
		logrus.Fatalf("*** ERROR: Invalid configuration for cluster creation.\n"+
			"\t   *** Redis Cluster requires at least 3 master nodes.\n"+
			"\t   *** This is not possible with %d nodes and %d replicas per node.\n"+
			"\t   *** At least %d nodes are required.", len(self.nodes), repOpt, 3*(repOpt+1))
	}
	return true
}

func (self *RedisTrib) AllocSlots(repOpt int) {
	// nodeNum := len(self.nodes)
	// mastersNum := len(self.nodes) / (repOpt + 1)

	// The first step is to split instances by IP. This is useful as
	// we'll try to allocate master nodes in different physical machines
	// (as much as possible) and to allocate slaves of a given master in
	// different physical machines as well.
	//
	// This code assumes just that if the IP is different, than it is more
	// likely that the instance is running in a different physical host
	// or at least a different virtual machine.

	// code for alloc slots
	return
}
