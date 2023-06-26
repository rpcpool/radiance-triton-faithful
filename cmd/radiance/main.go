// Gossip interacts with Solana gossip networks
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
	"go.firedancer.io/radiance/cmd/radiance/blockstore"
	"go.firedancer.io/radiance/cmd/radiance/car"
	"go.firedancer.io/radiance/cmd/radiance/gossip"
	"go.firedancer.io/radiance/cmd/radiance/replay"
	"go.firedancer.io/radiance/pkg/versioninfo"
	"k8s.io/klog/v2"

	// Load in instruction pretty-printing
	_ "github.com/gagliardetto/solana-go/programs/system"
	_ "github.com/gagliardetto/solana-go/programs/vote"
)

var cmd = cobra.Command{
	Use:   "radiance",
	Short: "Solana Go playground",
}

func init() {
	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	cmd.PersistentFlags().AddGoFlagSet(klogFlags)

	cmd.AddCommand(
		&blockstore.Cmd,
		&car.Cmd,
		&gossip.Cmd,
		&replay.Cmd,
		&versionCmd,
	)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cobra.CheckErr(cmd.ExecuteContext(ctx))
}

var (
	GitCommit string
	GitTag    string
)

func isAnyOf(s string, anyOf ...string) bool {
	for _, v := range anyOf {
		if s == v {
			return true
		}
	}
	return false
}

var versionCmd = cobra.Command{
	Use:   "version",
	Short: "Print the version number of Radiance",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Radiance.\n")
		fmt.Printf("Tag/Branch: %s\n", GitTag)
		fmt.Printf("Commit: %s\n", GitCommit)

		if info, ok := versioninfo.GetBuildSettings(); ok {
			fmt.Printf("More info:\n")
			for _, setting := range info {
				fmt.Printf("  %s: %s\n", setting.Key, setting.Value)
			}
		}
	},
}
