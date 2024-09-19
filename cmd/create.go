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
	createCmd.Flags().IntVarP(&shaderNum, "shaderNum", "n", 3, "Number of nodes in the cluster")
	createCmd.Flags().IntVarP(&replica, "replica", "r", 2, "Number of nodes in the cluster")
	createCmd.Flags().StringVarP(&clusterName, "clusterName", "c", "myCluster", "Name of the cluster")
	RootCmd.AddCommand(createCmd)
}
