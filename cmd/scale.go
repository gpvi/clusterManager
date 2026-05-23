package cmd

import (
	"context"
	"fmt"
	"redisClusterManager/model"

	"github.com/spf13/cobra"
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
		err := model.ScaleClusterAction(ctx, appConfig, additionalShards, clusterName)
		if err != nil {
			return fmt.Errorf("ScaleCluster() error: %w", err)
		}
		return nil
	},
}

func init() {
	scaleCmd.Flags().IntVarP(&additionalShards, "shards", "s", 1, "Number of shards to add")
	scaleCmd.Flags().StringVarP(&clusterName, "clusterName", "n", "", "Name of the cluster")
	RootCmd.AddCommand(scaleCmd)
}
