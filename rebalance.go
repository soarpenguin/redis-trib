// +build linux

package main

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

//  rebalance       host:port
//                  --weight <arg>
//                  --auto-weights
//                  --use-empty-masters
//                  --timeout <arg>
//                  --simulate
//                  --pipeline <arg>
//                  --threshold <arg>

var rebalanceCommand = cli.Command{
	Name:      "rebalance",
	Usage:     "rebalance a redis cluster.",
	ArgsUsage: `host:port`,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "weight",
			Usage: "Specifies per redis weight.",
		},
		cli.BoolFlag{
			Name:  "auto-weights",
			Usage: `Auto-weights flag for rebalance cluster.`,
		},
		cli.BoolFlag{
			Name:  "use-empty-masters",
			Usage: `Use empty mastes flag for rebalance cluster.`,
		},
		cli.IntFlag{
			Name:  "timeout",
			Usage: `Timeout for rebalance redis cluster.`,
		},
		cli.BoolFlag{
			Name:  "simulate",
			Usage: `Simulate flag for rebalance cluster.`,
		},
		cli.StringFlag{
			Name:  "pipeline",
			Value: "",
			Usage: `Pipeline for rebalance redis cluster.`,
		},
		cli.StringFlag{
			Name:  "replicas, r",
			Value: "",
			Usage: `Slave number for every master created, the default value is none.`,
		},
		cli.IntFlag{
			Name:  "threshold",
			Usage: `Threshold for rebalance redis cluster.`,
		},
	},
	Action: func(context *cli.Context) error {
		if len(context.Args()) < 1 {
			logrus.Fatalf("Must provide at least \"host:port\" for rebalance command!")
		}

		rt := NewRedisTrib()
		if err := rt.RebalanceClusterCmd(context); err != nil {
			//logrus.Errorf("%p", err)
			return err
		}
		return nil
	},
}

func (self *RedisTrib) RebalanceClusterCmd(context *cli.Context) error {
	var addr string
	if addr = context.Args().Get(0); addr == "" {
		return errors.New("Please check host:port for rebalance command.")
	}

	// Load nodes info before parsing options, otherwise we can't
	// handle --weight.
	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	// Check cluster, only proceed if it looks sane.
	self.CheckCluster(true)
	if len(self.Errors()) > 0 {
		logrus.Fatalf("*** Please fix your cluster problem before rebalancing.")
	}
	return nil
}
