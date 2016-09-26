package main

import (
	"errors"
	"math"
	"sort"
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
	var sn BalanceArray
	for _, node := range self.nodes {
		if node.HasFlag("master") && node.Weight() != 0 {
			sn = append(sn, node)
		}
	}

	// Because of rounding, it is possible that the balance of all nodes
	// summed does not give 0. Make sure that nodes that have to provide
	// slots are always matched by nodes receiving slots.
	//total_balance = sn.map{|x| x.info[:balance]}.reduce{|a,b| a+b}
	totalBalance := 0
	for _, node := range sn {
		totalBalance += node.Balance()
	}

	for totalBalance > 0 {
		for _, node := range sn {
			if node.Balance() < 0 && totalBalance > 0 {
				b := node.Balance() - 1
				node.SetBalance(b)
				totalBalance -= 1
			}
		}
	}

	// TODO:
	// Sort nodes by their slots balance.
	sort.Sort(BalanceArray(sn))

	logrus.Printf(">>> Rebalancing across %d nodes. Total weight = %d", nodesInvolved, totalWeight)

	if context.GlobalBool("verbose") {
		for _, node := range sn {
			logrus.Printf("%s balance is %d slots", node.String(), node.Balance())
		}
	}

	// Now we have at the start of the 'sn' array nodes that should get
	// slots, at the end nodes that must give slots.
	// We take two indexes, one at the start, and one at the end,
	// incrementing or decrementing the indexes accordingly til we
	// find nodes that need to get/provide slots.
	// TODO: check the logic of code
	dstIdx := 0
	srcIdx := len(sn) - 1

	for dstIdx < srcIdx {
		dst := sn[dstIdx]
		src := sn[srcIdx]

		var numSlots float64
		if math.Abs(float64(dst.Balance())) < math.Abs(float64(src.Balance())) {
			numSlots = math.Abs(float64(dst.Balance()))
		} else {
			numSlots = math.Abs(float64(src.Balance()))
		}

		if numSlots > 0 {
			logrus.Printf("Moving %d slots from %s to %s", numSlots, src.String(), dst.String())

			// Actaully move the slots.
			// TODO: add move slot code.
			srcs := ClusterArray{*src}
			reshardTable := self.ComputeReshardTable(srcs, int(numSlots))
			if len(reshardTable) != int(numSlots) {
				logrus.Fatalf("*** Assertio failed: Reshard table != number of slots")
			}

			if context.Bool("simulate") {
				logrus.Printf("%s", strings.Repeat("#", len(reshardTable)))
			} else {
				//opts := &MoveOpts{
				//	Quiet:    true,
				//	Dots:     false,
				//	Update:   true,
				//	Pipeline: context.Int("pipeline"),
				//}
				for _, _ = range reshardTable {
					//self.MoveSlot(e, target, opts)

					//     move_slot(e[:source],dst,e[:slot],
					//         :quiet=>true,
					//         :dots=>false,
					//         :update=>true,
					//         :pipeline=>opt['pipeline'])
					//     print "#"
					//     STDOUT.flush
				}
			}
		}

		// Update nodes balance.
		dst.SetBalance(dst.Balance() + int(numSlots))
		src.SetBalance(src.Balance() - int(numSlots))
		if dst.Balance() == 0 {
			dstIdx += 1
		}
		if src.Balance() == 0 {
			srcIdx -= 1
		}
	}

	return nil
}

///////////////////////////////////////////////////////////
// some useful struct contains cluster node.
type BalanceArray []*ClusterNode

func (b BalanceArray) Len() int {
	return len(b)
}

func (b BalanceArray) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b BalanceArray) Less(i, j int) bool {
	return b[i].Balance() < b[j].Balance()
}
