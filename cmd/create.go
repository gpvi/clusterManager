package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"redisStudy/model"
)

var shaderNum int
var replica int
var clusterName string
var inputPort uint16
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "create a Redis cluster",
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		ctx := context.Background()
		ctxPodman, err := model.CreatePodmanConnection(ctx)
		if err != nil {
			fmt.Printf("CreatePodmanConnection error: %v", err)
			return
		}
		err = model.CreateClusterAction(ctxPodman, shaderNum, replica, clusterName)
		if err != nil {
			log.Printf("CreateClusterAction Error: %v", err)
		}
	},
}

func init() {
	createCmd.Flags().IntVarP(&shaderNum, "shaderNum", "s", 3, "Number of nodes in the cluster")
	createCmd.Flags().IntVarP(&replica, "replica", "r", 2, "Number of nodes in the cluster")
	createCmd.Flags().StringVarP(&clusterName, "clusterName", "n", "cluster", "Name of the cluster")
	createCmd.Flags().Uint16VarP(&model.RedisContainerPort, "port", "p", 6379, "Port of the Redis container")
	RootCmd.AddCommand(createCmd)
}
