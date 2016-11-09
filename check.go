package main

import (
	"errors"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

// check            host:port
var checkCommand = cli.Command{
	Name:        "check",
	Usage:       "check the redis cluster.",
	ArgsUsage:   `host:port`,
	Description: `The check command check for redis cluster.`,
	Action: func(context *cli.Context) error {
		if context.NArg() != 1 {
			fmt.Printf("Incorrect Usage.\n\n")
			cli.ShowCommandHelp(context, "check")
			logrus.Fatalf("Must provide host:port for check command!")
		}

		rt := NewRedisTrib()
		if err := rt.CheckClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) CheckClusterCmd(context *cli.Context) error {
	var addr string

	if addr = context.Args().Get(0); addr == "" {
		return errors.New("Please check host:port for check command!")
	}

	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	self.CheckCluster(false)
	return nil
}
