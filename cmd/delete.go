package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"redisStudy/model"
)

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete cluster",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		ctxPodman, err := model.CreatePodmanConnection(ctx)
		if err != nil {
			e := fmt.Errorf("create Podman ctx error:%v", err)
			println(e)
		}
		model.DeleteAllContainers(ctxPodman)
	},
}

func init() {
	RootCmd.AddCommand(deleteCmd)
}
