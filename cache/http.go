package cache

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	pb "redisClusterManager/proto"
	"redisClusterManager/cache/peer"
)

const defaultBasePath = "/cache/"

// ========== HTTP Server (health + backward compat) ==========

// HTTPServer serves health checks and legacy HTTP cache requests.
// Primary cache transport is gRPC; this is kept for debugging and health checks.
type HTTPServer struct {
	self     string
	basePath string
}

// NewHTTPServer creates an HTTP server for this node.
func NewHTTPServer(selfAddr string) *HTTPServer {
	return &HTTPServer{
		self:     selfAddr,
		basePath: defaultBasePath,
	}
}

func (s *HTTPServer) Log(format string, v ...interface{}) {
	log.Printf("[server %s] %s", s.self, fmt.Sprintf(format, v...))
}

// ServeHTTP routes incoming requests.
func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/_health":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))

	case r.URL.Path == "/_info":
		fmt.Fprintf(w, "node: %s\nprotocol: gRPC+HTTP\n", s.self)

	default:
		http.Error(w, "cache requests use gRPC; use grpcurl or a gRPC client", http.StatusGone)
	}
}

// ========== CacheService Adapter ==========

// CacheServiceAdapter bridges peer.CacheService to the global Group registry.
type CacheServiceAdapter struct{}

// Ensure we implement the interface.
var _ peer.CacheService = (*CacheServiceAdapter)(nil)

func (a *CacheServiceAdapter) Get(groupName, key string) ([]byte, error) {
	g := GetGroup(groupName)
	if g == nil {
		return nil, fmt.Errorf("no such group: %s", groupName)
	}
	view, err := g.Get(key)
	if err != nil {
		return nil, err
	}
	return view.ByteSlice(), nil
}

func (a *CacheServiceAdapter) Delete(groupName, key string) {
	g := GetGroup(groupName)
	if g != nil {
		g.DeleteLocal(key)
	}
}

func (a *CacheServiceAdapter) PushReplica(groupName, key string, value []byte) {
	g := GetGroup(groupName)
	if g != nil {
		g.StoreReplica(key, ByteView{cloneBytes(value)})
	}
}

// ========== RegistryPicker (PeerPicker adapter) ==========

// RegistryPicker adapts a peer.Registry to the PeerPicker interface.
type RegistryPicker struct {
	Registry *peer.Registry
}

func (p *RegistryPicker) PickPeer(key string) (PeerGetter, bool) {
	pg, ok := p.Registry.PickPeer(key)
	if !ok {
		return nil, false
	}
	return &peerGetterAdapter{getter: pg}, true
}

// ========== Internal adapters ==========

type peerGetterAdapter struct {
	getter peer.PeerGetter
}

func (a *peerGetterAdapter) Get(in *pb.Request, out *pb.Response) error {
	resp, err := a.getter.Get(in)
	if err != nil {
		return err
	}
	out.Value = resp.Value
	return nil
}

// ========== Invalidation Broadcast ==========

// BroadcastInvalidation sends an invalidation to all peers via gRPC.
// Uses a bounded semaphore to prevent goroutine storms.
func BroadcastInvalidation(registry *peer.Registry, groupName, key string) {
	for _, pg := range registry.All() {
		replicationSem <- struct{}{}
		go func(p peer.PeerGetter) {
			defer func() { <-replicationSem }()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			if pp, ok := p.(peer.Invalidator); ok {
				if err := pp.Invalidate(ctx, groupName, key); err != nil {
					log.Printf("[Invalidate] broadcast to %s: %v", p.Addr(), err)
				}
			}
		}(pg)
	}
}
