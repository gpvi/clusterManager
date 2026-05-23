package cache

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"redisClusterManager/cache/conf"
	"redisClusterManager/cache/peer"
)

// createGroup creates a cache group backed by the in-memory DB.
// The group name is taken from cfg.GroupName (not hardcoded).
func createGroup(cfg *Config) (*Group, error) {
	getter := GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := cfg.DB[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		})
	return NewGroup(cfg.GroupName, cfg.MaxBytes, getter)
}

// createRegistry creates a memberlist-based peer registry.
// Peers communicate via gRPC on the cache port.
func createRegistry(bindAddr string, cachePort, gossipPort int, seeds []string) (*peer.Registry, error) {
	cfg := peer.RegistryConfig{
		BindAddr:       bindAddr,
		CachePort:      cachePort,
		GossipBindPort: gossipPort,
		SeedPeers:      seeds,
		LogOutput:      os.Stderr,
		TLS:            tlsConfigFromEnv(),
	}
	return peer.NewRegistry(cfg)
}

// startGrpcServer starts the gRPC server for peer-to-peer cache communication.
func startGrpcServer(addr string) (*peer.GrpcServer, <-chan error, error) {
	srv := peer.NewGrpcServer(addr, &CacheServiceAdapter{})
	serveErr, err := srv.Start(tlsConfigFromEnv())
	if err != nil {
		return nil, nil, fmt.Errorf("gRPC server: %w", err)
	}
	log.Printf("gRPC server running at %s", addr)
	return srv, serveErr, nil
}

// tlsConfigFromEnv reads TLS configuration from environment variables.
func tlsConfigFromEnv() peer.TLSConfig {
	mode := peer.TLSInsecure
	if os.Getenv("GEECACHE_TLS") == "1" || os.Getenv("GEECACHE_TLS") == "server" {
		mode = peer.TLSServerOnly
	}
	if os.Getenv("GEECACHE_TLS") == "mutual" {
		mode = peer.TLSMutual
	}
	return peer.TLSConfig{
		Mode:     mode,
		CertFile: os.Getenv("GEECACHE_TLS_CERT"),
		KeyFile:  os.Getenv("GEECACHE_TLS_KEY"),
		CAFile:   os.Getenv("GEECACHE_TLS_CA"),
	}
}

// startCacheHTTPServer starts a minimal HTTP server for health checks and debugging.
func startCacheHTTPServer(addr string) *http.Server {
	server := NewHTTPServer(addr)
	mux := http.NewServeMux()
	mux.Handle("/", server)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("HTTP health server running at %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server: %v", err)
		}
	}()
	return srv
}

// startAPIServer starts the frontend API gateway.
func startAPIServer(apiAddr string, gee *Group) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		view, err := gee.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(view.ByteSlice())
	})

	srv := &http.Server{
		Addr:    apiAddr,
		Handler: mux,
	}

	go func() {
		log.Printf("API gateway running at %s", apiAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("API server: %v", err)
		}
	}()
	return srv
}

// createHotKeyTracker creates a hot key detector with sensible defaults.
func createHotKeyTracker(registry *peer.Registry, localAddr string) *HotKeyTracker {
	cfg := DefaultHotKeyConfig()
	return NewHotKeyTracker(cfg, registry, localAddr)
}

// seedAddrs builds the memberlist seed list for this node.
func seedAddrs(servers []conf.Server, excludePort int) []string {
	var seeds []string
	for _, s := range servers {
		if s.Port != excludePort {
			seeds = append(seeds, s.SeedAddr())
		}
	}
	return seeds
}

// addrFromHostPort returns "host:port".
func addrFromHostPort(host string, port int) string {
	return host + ":" + strconv.Itoa(port)
}
