package peer

import (
	"context"

	pb "redisClusterManager/proto"
)

// PeerPicker selects a peer for a given key.
type PeerPicker interface {
	PickPeer(key string) (PeerGetter, bool)
}

// PeerGetter fetches data from a remote peer.
type PeerGetter interface {
	// Get retrieves the cache value from the peer.
	Get(req *pb.Request) (*pb.Response, error)
	// Addr returns the peer's address.
	Addr() string
}

// ReplicaPusher is implemented by peers that can store replicated hot keys.
type ReplicaPusher interface {
	PushReplica(ctx context.Context, group, key string, value []byte) error
}

// Invalidator is implemented by peers that support cache invalidation.
type Invalidator interface {
	Invalidate(ctx context.Context, group, key string) error
}
