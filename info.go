// +build linux

package main

import (
	"github.com/codegangsta/cli"
)

var infoCommand = cli.Command{
	Name:      "info",
	Usage:     "display the info of redis cluster.",
	ArgsUsage: `host:port`,
	Action: func(context *cli.Context) error {
		return nil
	},
}
