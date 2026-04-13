package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var RootCmd = &cobra.Command{
	Use:   "cluster",
	Short: "命令行控制 cluster 的创建销毁与扩容",
	Long: `Cluster CLI 工具用于管理和控制集群的创建、销毁与扩容。
			用法示例:
			cluster create  # 创建新的集群
			cluster delete  # 删除集群
			cluster scale   # 扩容集群
`,
}

// 自定义 Help 函数
func customHelpFunc(cmd *cobra.Command, args []string) {
	// 打印自定义帮助信息
	fmt.Println("自定义帮助信息:")
	fmt.Println()
	fmt.Println(cmd.Long)

	// 打印可用子命令
	fmt.Println("\n可用子命令:")
	for _, c := range cmd.Commands() {
		fmt.Printf("  %s\n", c.Use)
	}

	// 打印可用标志
	fmt.Println("\n可用标志:")
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		fmt.Printf("  --%s\n", flag.Name)
	})
}

func init() {
	// 设置自定义的 Help 函数
	RootCmd.SetHelpFunc(customHelpFunc)
}
