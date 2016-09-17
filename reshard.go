package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

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
		cli.IntFlag{
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

	if context.Int("timeout") > 0 {
		self.SetTimeout(context.Int("timeout"))
	}

	// Get number of slots
	var numSlots int
	if context.Int("slots") != 0 {
		numSlots = context.Int("slots")
	} else {
		numSlots = 0
		reader := bufio.NewReader(os.Stdin)
		for {
			if numSlots <= 0 || numSlots > ClusterHashSlots {
				fmt.Printf("How many slots do you want to move (from 1 to %d)? ", ClusterHashSlots)
				text, _ := reader.ReadString('\n')
				num, err := strconv.ParseInt(strings.TrimSpace(text), 10, 0)
				if err != nil {
					continue
				}
				numSlots = int(num)
			} else {
				break
			}
		}
	}

	// Get the target instance
	var target *ClusterNode
	if context.String("to") != "" {
		target = self.GetNodeByName(context.String("to"))

		if target == nil || target.HasFlag("slave") {
			logrus.Fatalf("*** The specified node is not known or not a master, please retry.")
		}
	} else {
		target = nil
		reader := bufio.NewReader(os.Stdin)

		for {
			if target != nil {
				break
			}
			fmt.Printf("What is the receiving node ID? ")
			text, _ := reader.ReadString('\n')
			target = self.GetNodeByName(strings.TrimSpace(text))

			if target == nil || target.HasFlag("slave") {
				logrus.Printf("*** The specified node is not known or not a master, please retry.")
				target = nil
			}
		}
	}

	// Get the source instances
	var sources []interface{}
	from := strings.TrimSpace(context.String("from"))
	if from != "" {
		srcArray := strings.Split(from, ",")

		for _, nodeID := range srcArray {
			nodeID = strings.TrimSpace(nodeID)
			if nodeID == "all" {
				sources = sources[:0]
				sources = append(sources, "all")
				break
			} else {
				node := self.GetNodeByName(nodeID)
				if node == nil || node.HasFlag("slave") {
					logrus.Fatalf("*** The specified node is not known or not a master, please retry.")
				}
				sources = append(sources, node)
			}
		}
	} else {
		logrus.Printf("Please enter all the source node IDs.\n" +
			"\t    Type 'all' to use all the nodes as source nodes for the hash slots.\n" +
			"\t    Type 'done' once you entered all the source nodes IDs.")

		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Printf("Source node #%d:", len(sources)+1)
			text, _ := reader.ReadString('\n')
			text = strings.TrimSpace(text)
			src := self.GetNodeByName(text)

			if text == "done" {
				break
			} else if text == "all" {
				sources = sources[:0]
				sources = append(sources, "all")
				break
			} else if src == nil || src.HasFlag("slave") {
				logrus.Warningf("*** The specified node is not known or not a master, please retry.")
			} else if src.Name() == target.Name() {
				logrus.Warningf("*** It is not possible to use the target node as source node.")
			} else {
				sources = append(sources, src)
			}
		}
	}

	if len(sources) <= 0 {
		logrus.Fatalf("*** No source nodes given, operation aborted")
	}

	if len(sources) == 1 {
		first := sources[0]

		// Handle soures == all.
		str, found := first.(string)
		if found && str == "all" {
			sources = sources[:0]

			for _, node := range self.nodes {
				if node.Name() == target.Name() || node.HasFlag("slave") {
					continue
				}
				sources = append(sources, node)
			}
		}
	}

	// Check if the destination node is the same of any source nodes.
	for _, node := range sources {
		if node != nil {
			if cnode, ok := node.(ClusterNode); ok {
				if cnode.Name() == target.Name() {
					logrus.Fatalf("*** Target node is also listed among the source nodes!")
				}
			}
		}
	}

	logrus.Printf("Ready to move %d slots.", numSlots)
	logrus.Printf("  Source nodes:")
	var srcs ClusterArray
	for _, node := range sources {
		if cnode, ok := node.(ClusterNode); ok {
			fmt.Printf("\t%s", cnode.InfoString())
			srcs = append(srcs, cnode)
		}
	}
	logrus.Printf("  Destination node: %s", target.InfoString())

	// TODO: ComputeReshardTable
	reshardTable := self.ComputeReshardTable(srcs, numSlots)
	logrus.Printf("  Resharding plan:")
	self.ShowReshardTable(reshardTable)

	if !context.Bool("yes") {
		fmt.Printf("Do you want to proceed with the proposed reshard plan (yes/no)? ")
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')

		if !strings.EqualFold(strings.TrimSpace(text), "yes") {
			logrus.Fatalf("*** Aborting...")
		}
	}

	pipeline := MigrateDefaultPipeline
	if context.String("pipeline") != "" {
		pnum, err := strconv.Atoi(context.String("pipeline"))
		if err == nil {
			pipeline = pnum
		}
	}
	opts := &MoveOpts{
		Dots:     true,
		Pipeline: pipeline,
	}
	// TODO: Move slots
	for _, e := range reshardTable {
		// move slot
		self.MoveSlot(e, target, opts)
	}

	return nil
}
