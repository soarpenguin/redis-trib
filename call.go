package main

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

// call            host:port command arg arg .. arg
var callCommand = cli.Command{
	Name:        "call",
	Usage:       "run command in redis cluster.",
	ArgsUsage:   `host:port command arg arg .. arg`,
	Description: `The call command for call cmd in every redis cluster node.`,
	Action: func(context *cli.Context) error {
		if context.NArg() < 2 {
			fmt.Printf("Incorrect Usage.\n\n")
			cli.ShowCommandHelp(context, "call")
			logrus.Fatalf("Must provide \"host:port command\" for call command!")
		}

		rt := NewRedisTrib()
		if err := rt.CallClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) CallClusterCmd(context *cli.Context) error {
	var addr string

	if addr = context.Args().Get(0); addr == "" {
		logrus.Fatalf("Please check host:port for call command!")
	}

	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	cmd := strings.ToUpper(context.Args().Get(1))
	cmdArgs := ToInterfaceArray(context.Args()[2:])

	logrus.Printf(">>> Calling %s %s", cmd, cmdArgs)
	_, err := self.EachRunCommandAndPrint(cmd, cmdArgs...)
	if err != nil {
		logrus.Errorf("Command failed: %s", err)
		return err
	}

	return nil
}
