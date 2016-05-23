// +build linux

package main

import (
	"github.com/codegangsta/cli"
)

var fixCommand = cli.Command{
	Name:      "fix",
	Usage:     "fix the redis cluster.",
	ArgsUsage: `host:port`,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "timeout, t",
			Usage: `timeout for fix the redis cluster.`,
		},
	},
	Action: func(context *cli.Context) error {
		return nil
	},
}
