// +build linux

package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

var addNodeCommand = cli.Command{
	Name:      "add-node",
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

    $ redis-trib add-node <--master-id arg> new_host:new_port existing_host:existing_port`,
		},
	},
	Action: func(context *cli.Context) error {

		if len(context.Args()) < 2 {
			logrus.Fatalf("Must provide \"new_host:new_port existing_host:existing_port\" for add-node command!")
		}

		rt := NewRedisTrib()
		if err := rt.AddNodeClusterCmd(context); err != nil {
			//logrus.Errorf("%p", err)
			return err
		}
		return nil
	},
}

func (self *RedisTrib) AddNodeClusterCmd(context *cli.Context) error {
	var newaddr string
	var addr string

	if newaddr = context.Args().Get(0); newaddr == "" {
		logrus.Fatalf("Please check new_host:new_port for add-node command!")
	}
	if addr = context.Args().Get(1); addr == "" {
		logrus.Fatalf("Please check existing_host:existing_port for add-node command!")
	}

	logrus.Printf(">>> Adding node %s to cluster %s", newaddr, addr)
	// Load cluster information
	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}
	self.CheckCluster(false)

	// Add the new node
	new := NewClusterNode(newaddr)
	new.Connect(true)
	new.AssertCluster()
	new.LoadInfo(false)
	new.AssertEmpty()
	//first := self.nodes[0]
	self.AddNode(new)

	// Send CLUSTER FORGET to all the nodes but the node to remove
	logrus.Printf(">>> Send CLUSTER MEET to node %s to make it join the cluster", new.String())
	//new.R().Call("")
	logrus.Printf("[OK] New node added correctly.")
	return nil
}
