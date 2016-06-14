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

	// Options parsing
	//weights := make(map[string]int)
	useEmpty := context.Bool("use-empty-masters")

	// Assign a weight to each node, and compute the total cluster weight.
	//totalWeight := 0
	nodesInvolved := 0
	for _, node := range self.nodes {
		if node.HasFlag("master") {
			if !useEmpty && len(node.Slots()) == 0 {
				continue
			}
			//n.info[:w] = weights[n.info[:name]] ? weights[n.info[:name]] : 1
			//total_weight += n.info[:w]
			nodesInvolved += 1
		}
	}

	// Check cluster, only proceed if it looks sane.
	self.CheckCluster(true)
	if len(self.Errors()) > 0 {
		logrus.Fatalf("*** Please fix your cluster problem before rebalancing.")
	}

	// Calculate the slots balance for each node. It's the number of
	// slots the node should lose (if positive) or gain (if negative)
	// in order to be balanced.
	threshold := context.Int("threshold")
	thresholdReached := false
	for _, node := range self.nodes {
		if node.HasFlag("master") {

		}
	}
	if !thresholdReached {
		logrus.Printf("*** No rebalancing needed! All nodes are within the %f threshold.", threshold)
		return nil
	}

	return nil
}
