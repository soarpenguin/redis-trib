// +build linux

package main

import (
	"github.com/codegangsta/cli"
)

// del-node        host:port node_id

var delNodeCommand = cli.Command{
	Name:      "del-node",
	Usage:     "del a redis node from existed cluster.",
	ArgsUsage: `host:port node_id`,
	Action: func(context *cli.Context) error {
		return nil
	},
}
