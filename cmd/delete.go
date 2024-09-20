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
	RunE: func(cmd *cobra.Command, args []string) error {
		if clusterName == "" {
			return fmt.Errorf("please input clustername first")
		}
		ctx := context.Background()
		ctxPodman, err := model.CreatePodmanConnection(ctx)
		if err != nil {
			e := fmt.Errorf("create Podman ctx error:%v", err)
			println(e)
		}
		model.DeleteAllContainers(ctxPodman, clusterName)
		return nil
	},
}

func init() {
	deleteCmd.Flags().StringVarP(&clusterName, "clusterName", "n", "", "Name of the cluster")
	RootCmd.AddCommand(deleteCmd)
}
