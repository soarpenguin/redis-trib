package main

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/garyburd/redigo/redis"
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

	useCopy := context.Bool("copy")
	useReplace := context.Bool("replace")

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
	slots := make(map[int]*ClusterNode)
	for _, node := range self.nodes {
		for key, _ := range node.Slots() {
			slots[key] = node
		}
	}

	// Use SCAN to iterate over the keys, migrating to the
	// right node as needed.
	var keys []string
	cursor := 0
	for {
		// we scan with our iter offset, starting at 0
		if arr, err := redis.MultiBulk(srcNode.R().Do("SCAN", cursor)); err != nil {
			logrus.Errorf("Do scan in import cmd failed: %s", err.Error())
		} else {
			// now we get the iter and the keys from the multi-bulk reply
			cursor, _ = redis.Int(arr[0], nil)
			keys, _ = redis.Strings(arr[1], nil)
		}
		// check if we need to stop...
		if cursor == 0 {
			break
		}

		var cmd []interface{}
		for _, key := range keys {
			slot := Key2Slot(key)
			target := slots[int(slot)]
			logrus.Printf("Migrating %s to %s - OK", key, target.String())

			cmd = append(cmd, target.Host(), target.Port(), key, 0, MigrateDefaultTimeout)

			if useCopy {
				cmd = append(cmd, useCopy)
			}

			if useReplace {
				cmd = append(cmd, useReplace)
			}

			if _, err := srcNode.Call("migrate", cmd...); err != nil {
				logrus.Printf("Migrating %s to %s - %s", key, target.String(), err.Error())
			} else {
				logrus.Printf("Migrating %s to %s - OK", key, target.String())
			}
		}
	}
	return nil
}
