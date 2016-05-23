// +build linux

package main

import (
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
		return nil
	},
}
