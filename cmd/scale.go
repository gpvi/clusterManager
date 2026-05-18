package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"redisClusterManager/model"
)

var additionalShards int

var scaleCmd = &cobra.Command{
	Use:   "scale",
	Short: "scale  cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		if clusterName == "" {
			return fmt.Errorf("please input clustername first")
		}
		ctx := context.Background()
		err := model.ScaleClusterAction(ctx, additionalShards, clusterName)
		if err != nil {
			fmt.Printf("ScaleCluster() error = %v\n", err)
		}
		return nil
	},
}

func init() {
	scaleCmd.Flags().IntVarP(&additionalShards, "shards", "s", 1, "Number of shards to add")
	scaleCmd.Flags().IntVar(&additionalShards, "shaderNum", 1, "Deprecated alias for --shards")
	_ = scaleCmd.Flags().MarkDeprecated("shaderNum", "use --shards instead")
	scaleCmd.Flags().StringVarP(&clusterName, "clusterName", "n", "", "Name of the cluster")
	RootCmd.AddCommand(scaleCmd)
}
