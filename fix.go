package main

import (
	"errors"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

// fix            host:port
//                  --timeout <arg>
var fixCommand = cli.Command{
	Name:        "fix",
	Usage:       "fix the redis cluster.",
	ArgsUsage:   `host:port`,
	Description: `The fix command for fix the redis cluster.`,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "timeout, t",
			Value: MigrateDefaultTimeout,
			Usage: `timeout for fix the redis cluster.`,
		},
	},
	Action: func(context *cli.Context) error {
		if context.NArg() != 1 {
			fmt.Printf("Incorrect Usage.\n\n")
			cli.ShowCommandHelp(context, "fix")
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
		return errors.New("please check host:port for fix command")
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
