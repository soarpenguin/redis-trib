package main

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

//  rebalance       host:port
//                  --weight <arg>
//                  --auto-weights
//                  --use-empty-masters
//                  --timeout <arg>
//                  --simulate
//                  --pipeline <arg>
//                  --threshold <arg>

var rebalanceCommand = cli.Command{
	Name:      "rebalance",
	Usage:     "rebalance a redis cluster.",
	ArgsUsage: `host:port`,
	Flags: []cli.Flag{
		cli.StringSliceFlag{
			Name:  "weight",
			Value: &cli.StringSlice{},
			Usage: "Specifies per redis node weight, muti times allowed.",
		},
		cli.BoolFlag{
			Name:  "auto-weights",
			Usage: `Auto-weights flag for rebalance cluster.`,
		},
		cli.BoolFlag{
			Name:  "use-empty-masters",
			Usage: `Use empty mastes flag for rebalance cluster.`,
		},
		cli.IntFlag{
			Name:  "timeout",
			Usage: `Timeout for rebalance redis cluster.`,
		},
		cli.BoolFlag{
			Name:  "simulate",
			Usage: `Simulate flag for rebalance cluster.`,
		},
		cli.IntFlag{
			Name:  "pipeline",
			Value: MigrateDefaultPipeline,
			Usage: `Pipeline for rebalance redis cluster.`,
		},
		cli.IntFlag{
			Name:  "threshold",
			Value: RebalanceDefaultThreshold,
			Usage: `Threshold for rebalance redis cluster.`,
		},
	},
	Action: func(context *cli.Context) error {
		if len(context.Args()) < 1 {
			logrus.Fatalf("Must provide at least \"host:port\" for rebalance command!")
		}
		rt := NewRedisTrib()
		if err := rt.RebalanceClusterCmd(context); err != nil {
			return err
		}
		return nil
	},
}

func (self *RedisTrib) RebalanceClusterCmd(context *cli.Context) error {
	var addr string
	if addr = context.Args().Get(0); addr == "" {
		return errors.New("Please check host:port for rebalance command.")
	}

	// Load nodes info before parsing options, otherwise we can't
	// handle --weight.
	if err := self.LoadClusterInfoFromNode(addr); err != nil {
		return err
	}

	// Options parsing
	//threshold := context.Int("threshold")
	//autoweights := context.Bool("auto-weights")
	weights := make(map[string]int)
	if context.String("weight") != "" {
		ws := context.StringSlice("weight")
		for _, e := range ws {
			if e != "" && strings.Contains(e, "=") {
				s := strings.Split(e, "=")
				node := self.GetNodeByAbbreviatedName(s[0])
				if node == nil || !node.HasFlag("master") {
					logrus.Fatalf("*** No such master node %s", s[0])
				}

				if w, err := strconv.Atoi(s[1]); err != nil {
					logrus.Fatalf("Invalid weight num for rebalance: %s=%v", s[0], s[1])
				} else {
					weights[node.Name()] = w
				}
			}
		}
	}
	useEmpty := context.Bool("use-empty-masters")

	// Assign a weight to each node, and compute the total cluster weight.
	totalWeight := 0
	nodesInvolved := 0
	for _, node := range self.nodes {
		if node.HasFlag("master") {
			if !useEmpty && len(node.Slots()) == 0 {
				continue
			}
			if w, ok := weights[node.Name()]; ok {
				node.SetWeight(w)
			} else {
				node.SetWeight(1)
			}

			totalWeight += node.Weight()
			nodesInvolved += 1
		}
	}

	// Check cluster, only proceed if it looks sane.
	self.CheckCluster(true)
	if len(self.Errors()) > 0 {
		logrus.Fatalf("*** Please fix your cluster problem before rebalancing.")
	}

	// Calculate the slots balance for each node. It's the number of
	// slots the node should lose (if positive) or gain (if negative)
	// in order to be balanced.
	threshold := context.Int("threshold")
	thresholdReached := false
	for _, node := range self.nodes {
		if node.HasFlag("master") {
			if node.Weight() == 0 {
				continue
			}
			expected := int((float64(ClusterHashSlots) / float64(totalWeight)) * float64(node.Weight()))
			node.SetBalance(len(node.Slots()) - expected)
			// Compute the percentage of difference between the
			// expected number of slots and the real one, to see
			// if it's over the threshold specified by the user.
			overThreshold := false

			if threshold > 0 {
				if len(node.Slots()) > 0 {
					errPerc := math.Abs(float64(100 - (100.0*expected)/len(node.Slots())))
					if int(errPerc) > threshold {
						overThreshold = true
					}
				} else if expected > 0 {
					overThreshold = true
				}
			}

			if overThreshold {
				thresholdReached = true
			}
		}
	}
	if !thresholdReached {
		logrus.Printf("*** No rebalancing needed! All nodes are within the %f threshold.", threshold)
		return nil
	}

	// Only consider nodes we want to change
	var sn [](*ClusterNode)
	for _, node := range self.nodes {
		if node.HasFlag("master") && node.Weight() != 0 {
			sn = append(sn, node)
		}
	}

	// Because of rounding, it is possible that the balance of all nodes
	// summed does not give 0. Make sure that nodes that have to provide
	// slots are always matched by nodes receiving slots.
	// TODO: add Calculate code.
	//total_balance = sn.map{|x| x.info[:balance]}.reduce{|a,b| a+b}
	//while total_balance > 0
	//    sn.each{|n|
	//        if n.info[:balance] < 0 && total_balance > 0
	//            n.info[:balance] -= 1
	//            total_balance -= 1
	//        end
	//    }
	//end

	// Sort nodes by their slots balance.
	//sn = sn.sort{|a,b|
	//    a.info[:balance] <=> b.info[:balance]
	//}

	logrus.Printf(">>> Rebalancing across %d nodes. Total weight = %d", nodesInvolved, totalWeight)

	// TODO:
	if context.GlobalBool("verbose") {

	}

	// Now we have at the start of the 'sn' array nodes that should get
	// slots, at the end nodes that must give slots.
	// We take two indexes, one at the start, and one at the end,
	// incrementing or decrementing the indexes accordingly til we
	// find nodes that need to get/provide slots.
	// TODO:
	// dstIdx := 0
	// srcIdx := len(sn) - 1

	//for dstIdx < srcIdx {

	//}

	return nil
}
