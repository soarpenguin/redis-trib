package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

// version will be the hash that the binary was built from
// and will be populated by the Makefile
var version = ""

// gitCommit will be the hash that the binary was built from
// and will be populated by the Makefile
var gitCommit = ""

const (
	usage = `Redis Cluster command line utility.

For check, fix, reshard, del-node, set-timeout you can specify the host and port
of any working node in the cluster.`
)

var mainFlags = []cli.Flag{
	cli.BoolFlag{
		Name:  "debug",
		Usage: "enable debug output for logging",
	},
	cli.BoolFlag{
		Name:  "verbose",
		Usage: "verbose global flag for output.",
	},
	cli.StringFlag{
		Name:  "log",
		Value: "",
		Usage: "set the log file path where internal debug information is written",
	},
	cli.StringFlag{
		Name:  "log-format",
		Value: "text",
		Usage: "set the format used by logs ('text' (default), or 'json')",
	},
}

var mainCommands = []cli.Command{
	addNodeCommand,
	callCommand,
	checkCommand,
	createCommand,
	delNodeCommand,
	fixCommand,
	importCommand,
	infoCommand,
	rebalanceCommand,
	reshardCommand,
	setTimeoutCommand,
}

func main() {
	app := cli.NewApp()
	app.Name = "redis-trib"
	app.Usage = usage
	v := []string{
		version,
	}
	if gitCommit != "" {
		v = append(v, fmt.Sprintf("commit: %s", gitCommit))
	}
	app.Version = strings.Join(v, "\n")
	app.Flags = mainFlags
	app.Author = "soarpenguin"
	app.Email = "soarpenguin@gmail.com"
	app.EnableBashCompletion = true
	app.Commands = mainCommands
	app.Before = func(context *cli.Context) error {
		if context.GlobalBool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		if path := context.GlobalString("log"); path != "" {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_SYNC, 0666)
			if err != nil {
				return err
			}
			logrus.SetOutput(f)
		}
		switch context.GlobalString("log-format") {
		case "text":
			// retain logrus's default.
		case "json":
			logrus.SetFormatter(new(logrus.JSONFormatter))
		default:
			logrus.Fatalf("unknown log-format %q", context.GlobalString("log-format"))
		}
		return nil
	}
	if err := app.Run(os.Args); err != nil {
		fatal(err)
	}
}
