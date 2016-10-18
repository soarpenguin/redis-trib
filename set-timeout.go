package main

import (
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

// set-timeout     host:port milliseconds
var setTimeoutCommand = cli.Command{
	Name:      "set-timeout",
	Usage:     "set timeout configure for redis cluster.",
	ArgsUsage: `host:port milliseconds`,
	Action: func(context *cli.Context) error {
		if len(context.Args()) < 2 {
			logrus.Fatalf("Must provide \"host:port milliseconds\" for set-timeout command!")
		}

		rt := NewRedisTrib()
		if err := rt.SetTimeoutClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) SetTimeoutClusterCmd(context *cli.Context) error {
	var addr string
	if addr = context.Args().Get(0); addr == "" {
		logrus.Fatalf("Please check host:port for info command!")
	}

	timeout := context.Args().Get(1)
	millisec, err := strconv.ParseInt(timeout, 0, 32)
	if err != nil {
		logrus.Fatalf("Please check the timeout format is number: %s", err.Error())
	} else if millisec < 100 {
		logrus.Fatalf("Setting a node timeout of less than 100 milliseconds is a bad idea.")
	}

	// Load cluster information
	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}
	okCount := 0
	errCount := 0

	// Send CLUSTER FORGET to all the nodes but the node to remove
	logrus.Printf(">>> Reconfiguring node timeout in every cluster node...")

	for _, node := range self.nodes {
		if _, err := node.Call("CONFIG", "set", "cluster-node-timeout", millisec); err != nil {
			logrus.Errorf("ERR setting node-timeot in set operation for %s: %s", node.String(), err.Error())
			errCount += 1
		} else {
			if _, err := node.Call("CONFIG", "rewrite"); err != nil {
				logrus.Errorf("ERR setting node-timeot in rewrite operation for %s: %s", node.String(), err.Error())
				errCount += 1
			} else {
				logrus.Printf("*** New timeout set for %s", node.String())
				okCount += 1
			}
		}
	}

	logrus.Printf(">>> New node timeout set. %d OK, %d ERR.", okCount, errCount)
	return nil
}
