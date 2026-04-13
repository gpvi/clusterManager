package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"redisStudy/model"
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
		ctx, err := model.CreatePodmanConnection(ctx)
		if err != nil {
			e := fmt.Errorf("%v", err)
			println(e)

		}
		err = model.ScaleClusterAction(ctx, additionalShards, clusterName)
		if err != nil {
			e := fmt.Errorf("ScaleCluster() error = %v", err)
			println(e)
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
