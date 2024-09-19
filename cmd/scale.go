package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"redisStudy/model"
)

var addNum int

var scaleCmd = &cobra.Command{
	Use:   "scale",
	Short: "scale  cluster",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		ctx, err := model.CreatePodmanConnection(ctx)
		if err != nil {
			e := fmt.Errorf("%v", err)
			println(e)

		}
		err = model.ScaleClusterAction(ctx, addNum, clusterName)
		if err != nil {
			e := fmt.Errorf("ScaleCluster() error = %v", err)
			println(e)
		}
	},
}

func init() {
	scaleCmd.Flags().IntVarP(&addNum, "shaderNum", "n", 1, "Number of nodes in the cluster")
	scaleCmd.Flags().StringVarP(&clusterName, "clusterName", "c", "myCluster", "Name of the cluster")
	RootCmd.AddCommand(scaleCmd)
}
