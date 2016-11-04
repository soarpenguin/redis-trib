package main

import (
	"errors"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

// info            host:port
var infoCommand = cli.Command{
	Name:        "info",
	Usage:       "display the info of redis cluster.",
	ArgsUsage:   `host:port`,
	Description: `The info command get infomation from redis cluster.`,
	Action: func(context *cli.Context) error {
		if context.NArg() < 1 {
			fmt.Printf("Incorrect Usage.\n\n")
			cli.ShowCommandHelp(context, "info")
			logrus.Fatalf("Must provide host:port for info command!")
		}

		rt := NewRedisTrib()
		if err := rt.InfoClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) InfoClusterCmd(context *cli.Context) error {
	var addr string

	if addr = context.Args().Get(0); addr == "" {
		return errors.New("Please check host:port for info command!")
	}

	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	self.ShowClusterInfo()
	return nil
}
