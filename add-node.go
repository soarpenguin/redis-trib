package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

//  add-node        new_host:new_port existing_host:existing_port
//                  --slave
//                  --master-id <arg>
var addNodeCommand = cli.Command{
	Name:      "add-node",
	Aliases:   []string{"add"},
	Usage:     "add a new redis node to existed cluster.",
	ArgsUsage: `new_host:new_port existing_host:existing_port`,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name: "slave",
			Usage: `Slave flag for node join a existed cluster.

    $ redis-trib add-node <--slave> new_host:new_port existing_host:existing_port`,
		},
		cli.StringFlag{
			Name:  "master-id",
			Value: "",
			Usage: `Master id for slave node to meet.

    $ redis-trib add-node <--slave --master-id arg> new_host:new_port existing_host:existing_port`,
		},
	},
	Action: func(context *cli.Context) error {
		if len(context.Args()) < 2 {
			logrus.Fatalf("Must provide \"new_host:new_port existing_host:existing_port\" for add-node command!")
		}

		rt := NewRedisTrib()
		if err := rt.AddNodeClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) AddNodeClusterCmd(context *cli.Context) error {
	var newaddr string
	var addr string
	var masterID string
	var master *ClusterNode

	if newaddr = context.Args().Get(0); newaddr == "" {
		logrus.Fatalf("Please check new_host:new_port for add-node command!")
	} else if addr = context.Args().Get(1); addr == "" {
		logrus.Fatalf("Please check existing_host:existing_port for add-node command!")
	}

	logrus.Printf(">>> Adding node %s to cluster %s", newaddr, addr)
	// Check the existing cluster
	// Load cluster information
	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}
	self.CheckCluster(false)

	// If --master-id was specified, try to resolve it now so that we
	// abort before starting with the node configuration.
	if context.Bool("slave") {
		masterID = context.String("master-id")
		if masterID != "" {
			master = self.GetNodeByName(masterID)
			if master == nil {
				logrus.Errorf("No such master ID %s", masterID)
			}
		} else {
			master = self.GetMasterWithLeastReplicas()
			if master == nil {
				logrus.Errorf("Can't selected a master node!")
			} else {
				logrus.Printf("Automatically selected master %s", master.String())
			}
		}
	}

	// Add the new node
	newNode := NewClusterNode(newaddr)
	newNode.Connect(true)
	if !newNode.AssertCluster() { // quit if not in cluster mode
		logrus.Fatalf("Node %s is not configured as a cluster node.", newNode.String())
	}

	if err := newNode.LoadInfo(false); err != nil {
		logrus.Fatalf("Load new node %s info failed: %s!", newaddr, err.Error())
	}
	newNode.AssertEmpty()
	self.AddNode(newNode)

	// Send CLUSTER FORGET to all the nodes but the node to remove
	logrus.Printf(">>> Send CLUSTER MEET to node %s to make it join the cluster", newNode.String())
	if _, err := newNode.ClusterAddNode(addr); err != nil {
		logrus.Fatalf("Add new node %s failed: %s!", newaddr, err.Error())
	}

	// Additional configuration is needed if the node is added as
	// a slave.
	if context.Bool("slave") {
		self.WaitClusterJoin()
		if master != nil {
			logrus.Printf(">>> Configure node as replica of %s.", master.String())
			newNode.ClusterReplicateWithNodeID(master.Name())
		} else {
			logrus.Fatalf("Master node is nil, can't get master info.")
		}
	}
	logrus.Printf("[OK] New node added correctly.")
	return nil
}
