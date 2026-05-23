// Package peer provides memberlist-based peer discovery and gRPC transport.
package peer

import (
	"redisClusterManager/cache/consistenthash"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

const defaultReplicas = 50

// Registry manages the peer set using hashicorp/memberlist for membership,
// failure detection, and health propagation. Each peer is assigned a GrpcGetter
// for cache data transport.
type Registry struct {
	list        *memberlist.Memberlist
	localAddr   string // "host:port" (without http://)
	apiPort     int    // the HTTP API port, for memberlist metadata
	hashRing    *consistenthash.Map
	grpcGetters map[string]*GrpcGetter // addr → gRPC client
	eventsCh    chan memberlist.NodeEvent
	tlsCfg      TLSConfig
	mu          sync.RWMutex
	running     bool
}

// RegistryConfig configures a peer Registry.
type RegistryConfig struct {
	// BindAddr is the IP/interface to bind to.
	BindAddr string
	// CachePort is the port this node's gRPC server listens on.
	CachePort int
	// APIPort is the port for the HTTP API gateway (stored in memberlist meta).
	APIPort int
	// GossipBindPort is the port memberlist binds to for gossip.
	GossipBindPort int
	// SeedPeers is the initial list of memberlist addresses to join.
	SeedPeers []string
	// LogOutput controls memberlist logging; nil discards.
	LogOutput io.Writer
	// TLS configures transport security for gRPC connections.
	TLS TLSConfig
}

// NewRegistry creates a Registry and starts the memberlist gossip agent.
func NewRegistry(cfg RegistryConfig) (*Registry, error) {
	localCacheAddr := net.JoinHostPort(cfg.BindAddr, strconv.Itoa(cfg.CachePort))

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = localCacheAddr
	mlCfg.BindAddr = cfg.BindAddr
	mlCfg.BindPort = cfg.GossipBindPort
	if cfg.LogOutput != nil {
		mlCfg.Logger = log.New(cfg.LogOutput, "[memberlist] ", log.LstdFlags)
	}

	eventsCh := make(chan memberlist.NodeEvent, 256)
	mlCfg.Events = &memberlist.ChannelEventDelegate{Ch: eventsCh}

	list, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("create memberlist: %w", err)
	}

	r := &Registry{
		list:        list,
		localAddr:   localCacheAddr,
		apiPort:     cfg.APIPort,
		hashRing:    consistenthash.New(defaultReplicas, nil),
		grpcGetters: make(map[string]*GrpcGetter),
		eventsCh:    eventsCh,
		tlsCfg:      cfg.TLS,
		running:     true,
	}

	// Add self to ring so PickPeer can exclude local node.
	r.hashRing.Add(localCacheAddr)

	if len(cfg.SeedPeers) > 0 {
		joined, err := list.Join(cfg.SeedPeers)
		if err != nil {
			log.Printf("[registry] join cluster: %v (reached %d peers)", err, joined)
		} else {
			log.Printf("[registry] joined cluster via seeds %v, reached %d peers", cfg.SeedPeers, joined)
		}
	}

	go r.processEvents()
	return r, nil
}

// Stop leaves the memberlist cluster gracefully and cleans up resources.
func (r *Registry) Stop() error {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()

	// Signal the event loop to exit and wait for it.
	close(r.eventsCh)

	// Close all gRPC connections.
	for _, g := range r.grpcGetters {
		g.Close()
	}
	return r.list.Leave(3 * time.Second)
}

// LocalAddr returns this node's cache gRPC address (host:port).
func (r *Registry) LocalAddr() string {
	return r.localAddr
}

// PickPeer selects the peer responsible for key using consistent hashing.
// Returns (nil, false) when the key belongs to the local node or no peer exists.
func (r *Registry) PickPeer(key string) (PeerGetter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	addr := r.hashRing.Get(key)
	if addr == "" || addr == r.localAddr {
		return nil, false
	}
	getter, ok := r.grpcGetters[addr]
	if !ok {
		return nil, false
	}
	// Return the GrpcGetter directly — it implements PeerGetter.
	return getter, true
}

// PickPeerWithFallback returns up to n candidate peers for key.
func (r *Registry) PickPeerWithFallback(key string, n int) []PeerGetter {
	r.mu.RLock()
	defer r.mu.RUnlock()

	candidates := r.hashRing.GetN(key, n)
	result := make([]PeerGetter, 0, len(candidates))
	for _, addr := range candidates {
		if addr == r.localAddr {
			continue
		}
		if getter, ok := r.grpcGetters[addr]; ok {
			result = append(result, getter)
		}
	}
	return result
}

// All returns all remote peers (excluding self).
func (r *Registry) All() []PeerGetter {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]PeerGetter, 0, len(r.grpcGetters))
	for addr, getter := range r.grpcGetters {
		if addr != r.localAddr {
			result = append(result, getter)
		}
	}
	return result
}

// PeerCount returns the number of known members including self.
func (r *Registry) PeerCount() int {
	return r.list.NumMembers()
}

// --- memberlist event handlers ---

func (r *Registry) processEvents() {
	for event := range r.eventsCh {
		switch event.Event {
		case memberlist.NodeJoin:
			r.handleJoin(event.Node)
		case memberlist.NodeLeave:
			r.handleLeave(event.Node)
		case memberlist.NodeUpdate:
			// Metadata changed; no-op unless we store weights.
		}
	}
}

func (r *Registry) handleJoin(node *memberlist.Node) {
	cacheAddr := node.Name // "host:port"
	log.Printf("[registry] peer joined: %s", cacheAddr)
	r.addToRing(cacheAddr)
}

func (r *Registry) handleLeave(node *memberlist.Node) {
	cacheAddr := node.Name
	log.Printf("[registry] peer left: %s", cacheAddr)
	r.removeFromRing(cacheAddr)
}

func (r *Registry) addToRing(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.grpcGetters[addr]; exists {
		return
	}

	getter, err := NewGrpcGetter(addr, r.tlsCfg)
	if err != nil {
		log.Printf("[registry] failed to create gRPC client for %s: %v", addr, err)
		return
	}

	r.hashRing.Add(addr)
	r.grpcGetters[addr] = getter
	log.Printf("[registry] added to ring: %s (ring size: %d)", addr, len(r.grpcGetters))
}

func (r *Registry) removeFromRing(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if getter, ok := r.grpcGetters[addr]; ok {
		getter.Close()
	}
	r.hashRing.Remove(addr)
	delete(r.grpcGetters, addr)
	log.Printf("[registry] removed from ring: %s (ring size: %d)", addr, len(r.grpcGetters))
}
