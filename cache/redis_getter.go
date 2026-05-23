package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisGetter implements Getter by fetching from a Redis instance.
// It connects to the Redis cluster managed by clusterManager.
type RedisGetter struct {
	client *redis.Client
	addr   string
}

// NewRedisGetter creates a Getter backed by a Redis connection.
func NewRedisGetter(addr, password string, db int) (*RedisGetter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis connect %s: %w", addr, err)
	}
	log.Printf("[RedisGetter] connected to %s", addr)
	return &RedisGetter{client: client, addr: addr}, nil
}

// NewRedisGetterFromClient creates a Getter from an existing redis.Client.
func NewRedisGetterFromClient(client *redis.Client) *RedisGetter {
	return &RedisGetter{client: client}
}

// Get fetches a key from Redis.
func (g *RedisGetter) Get(key string) ([]byte, error) {
	if g.client == nil {
		return nil, errors.New("redis client is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	val, err := g.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("key %q not found in redis", key)
	}
	return val, err
}

// Addr returns the Redis server address.
func (g *RedisGetter) Addr() string {
	return g.addr
}

// Close releases the Redis connection.
func (g *RedisGetter) Close() error {
	if g.client != nil {
		return g.client.Close()
	}
	return nil
}
