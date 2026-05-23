package cache

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"redisClusterManager/cache/conf"
)

// Config holds all configuration for a cache node.
type Config struct {
	// Node identity
	Host   string
	Port   int
	Gossip int
	IsAPI  bool

	// Cache group settings
	GroupName string
	MaxBytes  int64

	// Frontend API address (e.g. "http://host:port")
	APIAddr string

	// SeedAddrs is the list of seed node addresses for gossip discovery.
	// Takes precedence over servers when non-empty (CLI path).
	SeedAddrs []string

	// Backend data store (key → value)
	DB map[string]string

	// Cluster topology (all known servers, used internally for seed discovery
	// when SeedAddrs is empty — the YAML config path).
	servers []conf.Server
}

// LoadConfig loads configuration from file and environment variables.
// It replaces the original InitBefore() and returns a Config instead of
// setting package-level globals.
func LoadConfig() (*Config, error) {
	configPath := os.Getenv("GEECACHE_CONFIG")
	if configPath == "" {
		configPath = "config.yaml"
	}

	configData := conf.NewConfigData(configPath)
	if err := configData.Load(); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	if err := configData.GetConfig().Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	cfg := configData.GetConfig()
	apiAddr := cfg.FrontServer

	port := pickPort(configData)
	srv, ok := cfg.SelfServer(port)
	if !ok {
		return nil, fmt.Errorf("no server config for port %d", port)
	}

	log.Printf("[init] running as %s (api=%v, gossip=%d)",
		srv.CacheAddr(), srv.API, srv.Gossip)

	return &Config{
		Host:      srv.IP,
		Port:      srv.Port,
		Gossip:    srv.Gossip,
		IsAPI:     srv.API,
		APIAddr:   apiAddr,
		GroupName: "scores",
		MaxBytes:  cfg.Cache.MaxBytes,
		DB: map[string]string{
			"Tom":  "630",
			"Jack": "589",
			"Sam":  "567",
		},
		servers: cfg.OnlineServers,
	}, nil
}

// pickPort selects the port from the PORT env var, the first server config,
// or a default of 8001.
func pickPort(configData *conf.ConfigData) int {
	if env := os.Getenv("PORT"); env != "" {
		p, err := strconv.Atoi(env)
		if err == nil {
			return p
		}
	}
	cfg := configData.GetConfig()
	if len(cfg.OnlineServers) > 0 {
		return cfg.OnlineServers[0].Port
	}
	return 8001
}
