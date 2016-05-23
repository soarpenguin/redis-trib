// +build linux

package main

import (
	"github.com/codegangsta/cli"
)

// set-timeout     host:port milliseconds
var setTimeoutCommand = cli.Command{
	Name:      "set-timeout",
	Usage:     "set timeout configure for redis cluster.",
	ArgsUsage: `host:port milliseconds`,
	Action: func(context *cli.Context) error {
		return nil
	},
}
