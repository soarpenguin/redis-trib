// +build linux

package main

import (
	"github.com/codegangsta/cli"
)

// import          host:port
//                  --from <arg>
//                  --copy
//                  --replace
var importCommand = cli.Command{
	Name:      "import",
	Usage:     "import operation for redis cluster.",
	ArgsUsage: `host:port`,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "from",
			Usage: `Start slot redis cluster.`,
		},
		cli.BoolFlag{
			Name:  "copy",
			Usage: `Copy flag for import operation.`,
		},
		cli.BoolFlag{
			Name:  "replace",
			Usage: `Replace flag for import operation.`,
		},
	},
	Action: func(context *cli.Context) error {
		return nil
	},
}
