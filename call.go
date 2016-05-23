// +build linux

package main

import (
	"github.com/codegangsta/cli"
)

// call            host:port command arg arg .. arg
var callCommand = cli.Command{
	Name:      "call",
	Usage:     "run command in redis cluster.",
	ArgsUsage: `host:port command arg arg .. arg`,
	Action: func(context *cli.Context) error {
		return nil
	},
}
