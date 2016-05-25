// +build linux

package main

import (
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

// call            host:port command arg arg .. arg
var callCommand = cli.Command{
	Name:      "call",
	Usage:     "run command in redis cluster.",
	ArgsUsage: `host:port command arg arg .. arg`,
	Action: func(context *cli.Context) error {
		rt := NewRedisTrib()
		if err := rt.CallClusterCmd(context); err != nil {
			//logrus.Errorf("%p", err)
			return err
		}
		return nil
	},
}

func (self *RedisTrib) CallClusterCmd(context *cli.Context) error {
	var addr string

	if len(context.Args()) < 2 {
		logrus.Fatalf("Must provide \"host:port command\" for call command!")
	} else if addr = context.Args().Get(0); addr == "" {
		logrus.Fatalf("Please check host:port for call command!")
	}

	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	cmd := context.Args().Get(1)
	args := strings.Join(context.Args()[2:], " ")
	logrus.Printf(">>> Calling %s %s", cmd, args)
	for _, node := range self.nodes {
		if node == nil {
			continue
		}
		//res, err := node.CallCmd(cmd, args)
		//if err != nil {
		//	logrus.Printf("%s: %v", node.String(), res)
		//} else {
		//	logrus.Printf("%s: %s", node.String(), err.Error())
		//}
		//return err
	}
	return nil
}
