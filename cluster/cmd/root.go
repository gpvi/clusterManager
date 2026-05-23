package cmd

import (
	"fmt"
	"redisClusterManager/cluster/config"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var appConfig *config.RuntimeConfig

func SetConfig(cfg *config.RuntimeConfig) {
	appConfig = cfg
}

var backend string
var containerdSocket string
var dbPath string
var clusterName string
var shardCount int
var nodesPerShard int
var redisPort uint16
var additionalShards int

var RootCmd = &cobra.Command{
	Use:   "cluster",
	Short: "命令行控制 cluster 的创建销毁与扩容",
	Long: `Cluster CLI 工具用于管理和控制集群的创建、销毁与扩容。
				用法示例:
				cluster create  # 创建新的集群
				cluster delete  # 删除集群
				cluster scale   # 扩容集群
`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if appConfig != nil && backend != "" {
			appConfig.Backend = backend
		}
		if appConfig != nil && containerdSocket != "" {
			appConfig.ContainerdSocket = containerdSocket
		}
		if appConfig != nil && dbPath != "" {
			appConfig.DBPath = dbPath
		}
		return nil
	},
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
	RootCmd.PersistentFlags().StringVarP(&backend, "backend", "b", "podman", "Backend to use: podman or containerd")
	RootCmd.PersistentFlags().StringVar(&containerdSocket, "containerd-socket", "", "Containerd socket path")
	RootCmd.PersistentFlags().StringVar(&dbPath, "db", "", "SQLite database path for state persistence (default: runtime/cluster.db)")
}
