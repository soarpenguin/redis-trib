// +build linux

package main

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

//  reshard         host:port
//                  --from <arg>
//                  --to <arg>
//                  --slots <arg>
//                  --yes
//                  --timeout <arg>
//                  --pipeline <arg>

var reshardCommand = cli.Command{
	Name:      "reshard",
	Usage:     "reshard the redis cluster.",
	ArgsUsage: `host:port`,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "from",
			Usage: `Start slot number for reshard redis cluster.`,
		},
		cli.StringFlag{
			Name:  "to",
			Usage: `Dest slot number for reshard redis cluster.`,
		},
		cli.StringFlag{
			Name:  "slots",
			Usage: `Slots for reshard redis cluster.`,
		},
		cli.BoolFlag{
			Name:  "yes",
			Usage: `Auto agree the config for reshard.`,
		},
		cli.IntFlag{
			Name:  "timeout",
			Usage: `Timeout for reshard the redis cluster.`,
		},
		cli.StringFlag{
			Name:  "pipeline",
			Value: "",
			Usage: `Pipeline for reshard redis cluster.`,
		},
	},
	Action: func(context *cli.Context) error {
		if len(context.Args()) < 1 {
			logrus.Fatalf("Must provide at least \"host:port\" for reshard command!")
		}

		rt := NewRedisTrib()
		if err := rt.ReshardClusterCmd(context); err != nil {
			//logrus.Errorf("%p", err)
			return err
		}
		return nil
	},
}

func (self *RedisTrib) ReshardClusterCmd(context *cli.Context) error {
	var addr string
	if addr = context.Args().Get(0); addr == "" {
		return errors.New("Please check host:port for reshard command.")
	}

	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	self.CheckCluster(false)

	if len(self.Errors()) > 0 {
		logrus.Fatalf("*** Please fix your cluster problem before resharding.")
	}
	return nil
}
