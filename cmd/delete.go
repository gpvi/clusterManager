package cmd

import (
	"context"
	"fmt"
	"redisStudy/model"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// deleteCmd 删除集群
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "删除指定的集群",
	Long: `此命令用于删除指定的集群，并清除与集群相关的所有容器。 
	示例用法:
	cluster delete --clusterName <cluster-name> 
	删除指定名称的集群及其所有相关容器`,
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

// 自定义 Help 函数
func DeleteHelpFunc(cmd *cobra.Command, args []string) {
	fmt.Println("自定义帮助信息:")
	fmt.Println("该命令用于删除指定名称的集群及其所有相关容器。")
	fmt.Println()
	fmt.Println("示例用法:")
	fmt.Printf("  %s --clusterName <cluster-name>\n", cmd.Use)
	fmt.Println()
	fmt.Println("可用标志:")
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		fmt.Printf("  --%s\n", flag.Name)
	})
}

func init() {
	// 设置自定义帮助函数
	deleteCmd.SetHelpFunc(DeleteHelpFunc)

	// 添加 --clusterName 标志
	deleteCmd.Flags().StringVarP(&clusterName, "clusterName", "n", "", "指定集群的名称")

	// 将 deleteCmd 添加到 RootCmd
	RootCmd.AddCommand(deleteCmd)
}
