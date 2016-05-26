// +build linux

package main

import (
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
	logrus.Printf(">>> Creating cluster")

	for _, addr := range context.Args() {
		if addr == "" {
			continue
		}
		node := NewClusterNode(addr)
		node.Connect(true)
		node.AssertCluster()
		node.LoadInfo(false)
		node.AssertEmpty()
		self.AddNode(node)
	}
	self.CheckCreateParameters()

	logrus.Printf(">>> Performing hash slots allocation on %d nodes...", len(self.nodes))
	return nil
}

func (self *RedisTrib) CheckCreateParameters() bool {
	return true
}
