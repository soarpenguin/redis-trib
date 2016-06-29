package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

var checkCommand = cli.Command{
	Name:      "check",
	Usage:     "check the redis cluster.",
	ArgsUsage: `host:port`,
	Action: func(context *cli.Context) error {
		rt := NewRedisTrib()
		if err := rt.CheckClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) CheckClusterCmd(context *cli.Context) error {
	var addr string

	if len(context.Args()) < 1 {
		logrus.Fatalf("Must provide host:port for check command!")
	} else if addr = context.Args().Get(0); addr == "" {
		logrus.Fatalf("Please check host:port for check command!")
	}

	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	self.CheckCluster(false)
	return nil
}
