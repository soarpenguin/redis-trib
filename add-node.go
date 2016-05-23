// +build linux

package main

import (
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
		return nil
	},
}
