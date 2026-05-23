package main

import (
	"log"
	"redisClusterManager/cmd"
	"redisClusterManager/model"
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
