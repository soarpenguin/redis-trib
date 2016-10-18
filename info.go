package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

// info            host:port
var infoCommand = cli.Command{
	Name:      "info",
	Usage:     "display the info of redis cluster.",
	ArgsUsage: `host:port`,
	Action: func(context *cli.Context) error {

		rt := NewRedisTrib()
		if err := rt.InfoClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) InfoClusterCmd(context *cli.Context) error {
	var addr string

	if len(context.Args()) < 1 {
		logrus.Fatalf("Must provide host:port for info command!")
	} else if addr = context.Args().Get(0); addr == "" {
		logrus.Fatalf("Please check host:port for info command!")
	}

	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	self.ShowClusterInfo()
	return nil
}
