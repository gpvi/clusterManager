package utils

import (
	"fmt"
	"github.com/go-redis/redis"
	"os"
)

type RedisClient struct {
	client *redis.Client
}

// NewRedisClient creates a Redis client using config values or defaults.
func NewRedisClient(addr, password string, db int) *RedisClient {
	if addr == "" {
		addr = os.Getenv("REDIS_ADDR")
	}
	if addr == "" {
		addr = "localhost:6379"
	}
	if password == "" {
		password = os.Getenv("REDIS_PASSWORD")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisClient{client: client}
}

// GetKey retrieves a key from Redis.
func (c *RedisClient) GetKey(key string) (string, error) {
	val, err := c.client.Get(key).Result()
	if err != nil {
		return "", fmt.Errorf("redis get %q: %w", key, err)
	}
	return val, nil
}

// SetKey stores a key-value pair in Redis.
func (c *RedisClient) SetKey(key string, value string) error {
	return c.client.Set(key, value, 0).Err()
}

// Close closes the Redis client connection pool.
func (c *RedisClient) Close() error {
	return c.client.Close()
}
