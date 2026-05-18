package main

import (
	"log"
	"os"

	"redisClusterManager/agent"
	"redisClusterManager/model"
)

func main() {
	cfg, err := model.InitConfig()
	if err != nil {
		log.Fatalf("failed to initialize config: %v", err)
	}

	clientset, restCfg, err := model.NewK8sClientset(cfg.KubeConfigPath)
	if err != nil {
		log.Fatalf("failed to create k8s clientset: %v", err)
	}

	server := agent.NewServer(clientset, restCfg, cfg)

	port := os.Getenv("TOOL_SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	log.Fatal(server.Start(":" + port))
}
