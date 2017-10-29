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
	// name holds the name of this program
	name  = "redis-trib"
	usage = `Redis Cluster command line utility.

For check, fix, reshard, del-node, set-timeout you can specify the host and port
of any working node in the cluster.`
)

// runtimeFlags is the list of supported global command-line flags
var runtimeFlags = []cli.Flag{
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

// runtimeBeforeSubcommands is the function to run before command-line
// parsing occurs.
var runtimeBeforeSubcommands = beforeSubcommands

// runtimeCommandNotFound is the function to handle an invalid sub-command.
var runtimeCommandNotFound = commandNotFound

// runtimeCommands is all sub-command
var runtimeCommands = []cli.Command{
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

func beforeSubcommands(context *cli.Context) error {
	if context.GlobalBool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if context.GlobalBool("verbose") {
		os.Setenv("ENV_MODE_VERBOSE", "true")
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

// function called when an invalid command is specified which causes the
// runtime to error.
func commandNotFound(c *cli.Context, command string) {
	err := fmt.Errorf("invalid command %q", command)
	fatal(err)
}

// makeVersionString returns a multi-line string describing the runtime version.
func makeVersionString() string {
	v := []string{
		version,
	}
	if gitCommit != "" {
		v = append(v, fmt.Sprintf("commit: %s", gitCommit))
	}

	return strings.Join(v, "\n")
}

func main() {
	app := cli.NewApp()

	app.Name = name
	app.Writer = os.Stdout
	app.Usage = usage
	app.Version = makeVersionString()
	app.Flags = runtimeFlags
	app.Author = "soarpenguin"
	app.Email = "soarpenguin@gmail.com"
	app.EnableBashCompletion = true
	app.CommandNotFound = runtimeCommandNotFound
	app.Before = runtimeBeforeSubcommands
	app.Commands = runtimeCommands

	if err := app.Run(os.Args); err != nil {
		fatal(err)
	}
}
