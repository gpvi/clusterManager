package cmd

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use:   "cluster",
	Short: "命令行控制cluster的创建销毁与扩容",
}

func init() {
	RootCmd.AddCommand(deleteCmd)
	RootCmd.AddCommand(createCmd)
	RootCmd.AddCommand(scaleCmd)
}
