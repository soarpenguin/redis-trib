// +build linux

package main

import (
	"github.com/codegangsta/cli"
)

var checkCommand = cli.Command{
	Name:      "check",
	Usage:     "check the redis cluster.",
	ArgsUsage: `host:port`,
	Action: func(context *cli.Context) error {
		return nil
	},
}
