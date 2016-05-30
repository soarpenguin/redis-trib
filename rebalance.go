// +build linux

package main

import (
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
		return nil
	},
}
