package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"redisClusterManager/cache"
	"redisClusterManager/model"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	cachePort   int
	cacheGossip int
	cacheAPI    bool
	cacheSeeds  []string
)

var cacheCmd = &cobra.Command{
	Use:   "cache",
	Short: "启动一个分布式缓存节点 (GeeCache)",
	Long: `启动一个基于 GeeCache 的分布式内存缓存节点，
通过 gRPC 进行节点间通信，使用 SWIM gossip 协议进行集群发现。

示例:
  cluster cache --port=8001 --gossip=9001
  cluster cache --port=8002 --gossip=9002 --api --seeds=10.0.1.1:8001`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := buildCacheConfig()
		fmt.Printf("Starting cache node on gRPC :%d, gossip :%d\n", cfg.Port, cfg.Gossip)

		ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		return cache.Run(ctx, cfg)
	},
}

func buildCacheConfig() *cache.Config {
	cfg := &cache.Config{
		Host:      "0.0.0.0",
		Port:      cachePort,
		Gossip:    cacheGossip,
		IsAPI:     cacheAPI,
		SeedAddrs: cacheSeeds,
		GroupName: appConfig.CacheGroupName(),
		MaxBytes:  appConfig.CacheMaxBytes(),
	}
	// Use config file values as defaults when CLI flags are not set.
	if cfg.Port == 0 {
		cfg.Port = appConfig.CachePort()
	}
	if cfg.Gossip == 0 {
		cfg.Gossip = appConfig.CacheGossipPort()
	}
	if cfg.IsAPI && cfg.APIAddr == "" {
		cfg.APIAddr = appConfig.CacheAPIAddr()
	}
	return cfg
}

func init() {
	RootCmd.AddCommand(cacheCmd)
	cacheCmd.Flags().IntVarP(&cachePort, "port", "p", 0, "缓存 gRPC 端口 (默认: 8001)")
	cacheCmd.Flags().IntVarP(&cacheGossip, "gossip", "g", 0, "Gossip 协议端口 (默认: 9001)")
	cacheCmd.Flags().BoolVar(&cacheAPI, "api", false, "启用 HTTP API 网关")
	cacheCmd.Flags().StringSliceVar(&cacheSeeds, "seeds", nil, "种子节点地址列表 (逗号分隔)")

	// Wire cache command to appConfig if available.
	cobra.OnInitialize(func() {
		if appConfig == nil {
			cfg, err := model.InitConfig()
			if err != nil {
				fmt.Fprintf(os.Stderr, "init config: %v\n", err)
				os.Exit(1)
			}
			appConfig = cfg
		}
	})
}
