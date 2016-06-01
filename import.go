// +build linux

package main

import (
	"errors"

	"github.com/Sirupsen/logrus"
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
		if len(context.Args()) < 1 {
			logrus.Fatalf("Must provide \"host:port\" for import command!")
		}

		rt := NewRedisTrib()
		if err := rt.ImportClusterCmd(context); err != nil {
			//logrus.Errorf("%p", err)
			return err
		}
		return nil
	},
}

func (self *RedisTrib) ImportClusterCmd(context *cli.Context) error {
	var addr string
	var source string

	if source = context.String("from"); source == "" {
		logrus.Fatalf("Option \"--from\" is required for import command!")
	}

	if addr = context.Args().Get(0); addr == "" {
		return errors.New("Please check host:port for import command.")
	}

	logrus.Printf(">>> Importing data from %s to cluster %s", source, addr)

	// Load nodes info before parsing options, otherwise we can't
	// handle --weight.
	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	// Check cluster, only proceed if it looks sane.
	self.CheckCluster(false)

	// Connect to the source node.
	logrus.Printf(">>> Connecting to the source Redis instance")
	srcNode := NewClusterNode(source)

	if srcNode.AssertCluster() {
		logrus.Errorf("The source node should not be a cluster node.")
	}
	dbsize, _ := srcNode.Dbsize()
	logrus.Printf("*** Importing %d keys from DB 0", dbsize)

	// Build a slot -> node map
	//slots := make(map[int]*ClusterNode)
	return nil
}
