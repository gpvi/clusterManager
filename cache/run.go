package cache

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Run starts the cache node with the given configuration and blocks until
// shutdown is triggered (SIGINT/SIGTERM) or a fatal gRPC error occurs.
// On graceful shutdown it returns nil; on gRPC failure it returns the error.
func Run(ctx context.Context, cfg *Config) error {
	cacheAddr := addrFromHostPort(cfg.Host, cfg.Port)

	log.Printf("=== GeeCache Node ===")
	log.Printf("cache (gRPC): %s", cacheAddr)
	log.Printf("gossip:       %s:%d", cfg.Host, cfg.Gossip)

	// Create memberlist-based peer registry.
	seeds := cfg.SeedAddrs
	if len(seeds) == 0 {
		seeds = seedAddrs(cfg.servers, cfg.Port)
	}
	registry, err := createRegistry(cfg.Host, cfg.Port, cfg.Gossip, seeds)
	if err != nil {
		return fmt.Errorf("create registry: %w", err)
	}

	log.Printf("connected to %d peers", registry.PeerCount())

	// Create cache group and wire it to the registry.
	gee, err := createGroup(cfg)
	if err != nil {
		return fmt.Errorf("create group: %w", err)
	}
	if err := gee.RegisterPeers(&RegistryPicker{Registry: registry}); err != nil {
		return fmt.Errorf("register peers: %w", err)
	}
	gee.SetRegistry(registry)

	// Enable hot key detection and replication.
	hotTracker := createHotKeyTracker(registry, cacheAddr)
	gee.SetHotKeyTracker(hotTracker)

	// Enable DB load rate limiting to prevent thundering herd on node changes.
	gee.SetDBLimiter(NewDBLoaderLimiter())

	// Start gRPC server for peer-to-peer cache communication.
	grpcSrv, serveErr, err := startGrpcServer(cacheAddr)
	if err != nil {
		return err
	}

	// Start HTTP health-check server.
	healthPort := cfg.Port + 100
	healthAddr := addrFromHostPort(cfg.Host, healthPort)
	httpSrv := startCacheHTTPServer(healthAddr)

	// Start API gateway if this node has api=true.
	var apiSrv *http.Server
	if cfg.IsAPI {
		apiAddr := cfg.APIAddr
		if apiAddr == "" {
			apiAddr = addrFromHostPort(cfg.Host, 9999)
		}
		// strip "http://" prefix
		if len(apiAddr) > 7 && apiAddr[:7] == "http://" {
			apiAddr = apiAddr[7:]
		}
		apiSrv = startAPIServer(apiAddr, gee)
	}

	// Wait for context cancellation or fatal gRPC error.
	var grpcErr error
	select {
	case <-ctx.Done():
		log.Printf("context cancelled, shutting down...")
	case err := <-serveErr:
		if err != nil {
			log.Printf("gRPC server fatal error: %v, shutting down...", err)
			grpcErr = err
		}
	}

	// Graceful shutdown with timeout.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcSrv.Stop()
	httpSrv.Shutdown(shutdownCtx)
	if apiSrv != nil {
		apiSrv.Shutdown(shutdownCtx)
	}
	hotTracker.Stop()
	registry.Stop()

	log.Println("shutdown complete")
	return grpcErr
}
