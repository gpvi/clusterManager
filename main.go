package main

import (
	"log"
	"redisClusterManager/cluster/cmd"
	"redisClusterManager/cluster/config"
)

func main() {
	cfg, err := config.InitConfig()
	if err != nil {
		log.Fatalf("failed to initialize config: %v", err)
	}
	cmd.SetConfig(cfg)
	if err := cmd.RootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
