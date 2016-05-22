// +build linux

package main

import (
	"github.com/codegangsta/cli"
)

var createCommand = cli.Command{
	Name:      "create",
	Usage:     "create a new redis cluster.",
	ArgsUsage: `<host1:port1 ... hostN:portN>`,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "replicas, r",
			Value: "",
			Usage: `slave for every master created, the default value is none.

    $ redis-trib create <--replicas 1> <host1:port1 ... hostN:portN>`,
		},
	},
	Action: func(context *cli.Context) error {
		return nil
	},
}
