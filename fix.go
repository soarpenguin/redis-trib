package main

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

var fixCommand = cli.Command{
	Name:      "fix",
	Usage:     "fix the redis cluster.",
	ArgsUsage: `host:port`,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "timeout, t",
			Value: MigrateDefaultTimeout,
			Usage: `timeout for fix the redis cluster.`,
		},
	},
	Action: func(context *cli.Context) error {
		if len(context.Args()) < 1 {
			logrus.Fatalf("Must provide at least \"host:port\" for fix command!")
		}

		rt := NewRedisTrib()
		if err := rt.FixClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) FixClusterCmd(context *cli.Context) error {
	var addr string
	if addr = context.Args().Get(0); addr == "" {
		return errors.New("Please check host:port for fix command.")
	}

	self.SetFix(true)
	timeout := context.Int("timeout")
	self.SetTimeout(timeout)
	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	self.CheckCluster(false)
	return nil
}
