package peer

import (
	pb "redisClusterManager/cluster/proto"
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

// GrpcGetter implements PeerGetter over gRPC.
// Each instance holds a gRPC connection to a single peer.
type GrpcGetter struct {
	addr   string // e.g. "localhost:8001"
	conn   *grpc.ClientConn
	client pb.GroupCacheClient
	mu     sync.RWMutex
}

// NewGrpcGetter creates a gRPC client for a peer at the given address.
// The address is host:port (without http:// prefix).
func NewGrpcGetter(addr string, tlsCfg TLSConfig) (*GrpcGetter, error) {
	creds, err := ClientCredentials(tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("tls client credentials: %w", err)
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(`{
			"loadBalancingPolicy": "round_robin",
			"healthCheckConfig": {"serviceName": "geecachepb.GroupCache"}
		}`),
		// Keepalive — send pings every 10s to detect dead peers quickly.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	return &GrpcGetter{
		addr:   addr,
		conn:   conn,
		client: pb.NewGroupCacheClient(conn),
	}, nil
}

// Addr returns the peer's address.
func (g *GrpcGetter) Addr() string {
	return g.addr
}

// Get fetches a cache value from the peer via gRPC.
func (g *GrpcGetter) Get(req *pb.Request) (*pb.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := g.client.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpc get %s: %w", g.addr, err)
	}
	return resp, nil
}

// PushReplica sends a hot key replica to this peer for local storage.
func (g *GrpcGetter) PushReplica(ctx context.Context, group, key string, value []byte) error {
	req := &pb.PushRequest{Group: group, Key: key, Value: value}
	_, err := g.client.Push(ctx, req)
	if err != nil {
		return fmt.Errorf("push replica %s: %w", g.addr, err)
	}
	return nil
}

// Invalidate tells the peer to remove a cached entry via gRPC.
func (g *GrpcGetter) Invalidate(ctx context.Context, group, key string) error {
	req := &pb.Request{Group: group, Key: key}
	_, err := g.client.Invalidate(ctx, req)
	if err != nil {
		return fmt.Errorf("grpc invalidate %s: %w", g.addr, err)
	}
	return nil
}

// Healthy returns true if the gRPC connection is in a healthy state.
func (g *GrpcGetter) Healthy() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	state := g.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Close closes the underlying gRPC connection.
func (g *GrpcGetter) Close() error {
	return g.conn.Close()
}
