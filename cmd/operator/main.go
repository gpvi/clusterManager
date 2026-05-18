package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"redisClusterManager/controller"
	"redisClusterManager/model"
)

func main() {
	kubeConfig := os.Getenv("KUBECONFIG")
	clientset, restCfg, err := model.NewK8sClientset(kubeConfig)
	if err != nil {
		log.Fatalf("failed to create Kubernetes clientset: %v", err)
	}

	namespace := os.Getenv("KUBE_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	ctrl := controller.NewController(clientset, restCfg, namespace)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Fatal(ctrl.Run(ctx))
}
