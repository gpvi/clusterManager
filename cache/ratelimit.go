package cache

import (
	"context"
	"golang.org/x/time/rate"
	"time"
)

// DBLoaderLimiter rate-limits calls to getLocally() to prevent thundering-herd
// DB storms when nodes join or fail.
type DBLoaderLimiter struct {
	limiter *rate.Limiter
}

// Default DB load limit: 200 req/s with burst of 50.
// A 50-node cluster with 2% key redistribution at 100K active keys
// = 2000 misses. 200 req/s disperses them over 10s, keeping DB safe.
func NewDBLoaderLimiter() *DBLoaderLimiter {
	return &DBLoaderLimiter{
		limiter: rate.NewLimiter(200, 50),
	}
}

// NewDBLoaderLimiterWithLimit creates a limiter with custom rate.
func NewDBLoaderLimiterWithLimit(rps int, burst int) *DBLoaderLimiter {
	return &DBLoaderLimiter{
		limiter: rate.NewLimiter(rate.Limit(rps), burst),
	}
}

// Wait blocks until a DB load is permitted or the context expires.
func (l *DBLoaderLimiter) Wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	return l.limiter.Wait(ctx)
}

// Allow returns true if a DB load can proceed immediately.
func (l *DBLoaderLimiter) Allow() bool {
	if l == nil {
		return true
	}
	return l.limiter.Allow()
}

// Default 3-second timeout for rate-limited DB loads.
var dbLoadTimeout = 3 * time.Second
