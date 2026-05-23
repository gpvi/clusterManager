package main

import (
	"log"
	"redisClusterManager/cluster/cmd"
	"redisClusterManager/cluster/model"
)

func main() {
	cfg, err := model.InitConfig()
	if err != nil {
		log.Fatalf("failed to initialize config: %v", err)
	}
	cmd.SetConfig(cfg)
	if err := cmd.RootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
