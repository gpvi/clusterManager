package peer

import (
	pb "redisClusterManager/proto"
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
	"strings"
	"time"
)

// shared HTTP client with connection pooling and timeouts.
var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 3 * time.Second,
}

// HTTPGetter implements PeerGetter over HTTP.
type HTTPGetter struct {
	baseURL string // e.g. "http://localhost:8001/cache/"
}

// NewHTTPGetter creates an HTTPGetter for a peer at the given base URL.
// baseURL should be the full cache path prefix, e.g. "http://localhost:8001/cache/".
func NewHTTPGetter(baseURL string) *HTTPGetter {
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	return &HTTPGetter{baseURL: baseURL}
}

// Addr returns the peer's base URL.
func (h *HTTPGetter) Addr() string {
	return h.baseURL
}

// Get fetches a cache value from the remote peer using protobuf over HTTP.
func (h *HTTPGetter) Get(req *pb.Request) (*pb.Response, error) {
	u := fmt.Sprintf("%s%s/%s",
		strings.TrimSuffix(h.baseURL, "/"),
		req.GetGroup(),
		req.GetKey(),
	)

	resp, err := httpClient.Get(u)
	if err != nil {
		return nil, fmt.Errorf("peer get %s: %w", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	out := &pb.Response{}
	if err := proto.Unmarshal(body, out); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return out, nil
}

// Invalidate sends a cache invalidation request to the peer.
// Uses query parameters to avoid requiring protoc-generated InvalidateRequest.
func (h *HTTPGetter) Invalidate(ctx context.Context, group, key string) error {
	u := fmt.Sprintf("%s_invalidate?group=%s&key=%s",
		strings.TrimSuffix(h.baseURL, "/"),
		group, key,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalidate returned %d", resp.StatusCode)
	}
	return nil
}

// Ping checks if the peer is reachable via its cache HTTP endpoint.
func (h *HTTPGetter) Ping(ctx context.Context) error {
	u := h.baseURL + "_health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned %d", resp.StatusCode)
	}
	return nil
}
