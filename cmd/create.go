package cmd

import (
	"context"
	"fmt"
	"log"
	"redisStudy/model"

	"github.com/spf13/cobra"
)

var shardCount int
var nodesPerShard int
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
		err = model.CreateClusterAction(ctxPodman, shardCount, nodesPerShard, clusterName)
		if err != nil {
			log.Printf("CreateClusterAction Error: %v", err)
		}
	},
}

func init() {
	createCmd.Flags().IntVarP(&shardCount, "shards", "s", 3, "Number of shards to create")
	createCmd.Flags().IntVarP(&nodesPerShard, "nodes-per-shard", "r", 2, "Number of Redis nodes in each shard, including the master")
	createCmd.Flags().StringVarP(&clusterName, "clusterName", "n", "cluster", "Name of the cluster")
	createCmd.Flags().Uint16VarP(&model.RedisContainerPort, "port", "p", 6379, "Port of the Redis container")
	createCmd.Flags().IntVar(&shardCount, "shaderNum", 3, "Deprecated alias for --shards")
	_ = createCmd.Flags().MarkDeprecated("shaderNum", "use --shards instead")
	createCmd.Flags().IntVar(&nodesPerShard, "replica", 2, "Deprecated alias for --nodes-per-shard")
	_ = createCmd.Flags().MarkDeprecated("replica", "use --nodes-per-shard instead")
	RootCmd.AddCommand(createCmd)
}
