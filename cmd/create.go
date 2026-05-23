package cmd

import (
	"context"
	"log"
	"redisClusterManager/model"

	"github.com/spf13/cobra"
)

var shardCount int
var nodesPerShard int
var clusterName string
var redisPort uint16

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "create a Redis cluster",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		if appConfig == nil {
			log.Fatal("config not initialized")
		}
		if redisPort != 0 {
			appConfig.RedisContainerPort = redisPort
		}
		err := model.CreateClusterAction(ctx, appConfig, shardCount, nodesPerShard, clusterName)
		if err != nil {
			log.Printf("CreateClusterAction Error: %v", err)
		}
	},
}

func init() {
	createCmd.Flags().IntVarP(&shardCount, "shards", "s", 3, "Number of shards to create")
	createCmd.Flags().IntVarP(&nodesPerShard, "nodes-per-shard", "r", 2, "Number of Redis nodes in each shard, including the master")
	createCmd.Flags().StringVarP(&clusterName, "clusterName", "n", "cluster", "Name of the cluster")
	createCmd.Flags().Uint16VarP(&redisPort, "port", "p", 6379, "Port of the Redis container")
	RootCmd.AddCommand(createCmd)
}
