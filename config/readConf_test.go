package config

import (
	"redisStudy/utils"
	"testing"
)

const totalSlots = 16384

var True = true

// 指定本地的配置文件路径
var redisHostConfigPath = "/Users/zhuoqun.niu/Desktop/redis/config"

// 容器redis配置路径
var redisConfigPath = "/data/redis/config"

// 宿主机redis配置路径
var redisHostDataPath = "/Users/zhuoqun.niu/Desktop/redis/data"

// 容器路径
var redisConfigDataPath = "/data/redis/data"

// 创建后的配置文件名
var ConfigSaveFileName = "redis_cluster_config.json"

type Config struct {
	RedisHostConfigPath string
	RedisConfigPath     string
	RedisHostDataPath   string
	RedisConfigDataPath string
	ConfigSaveFileName  string
	Port                int
}

func NewConfig() *Config {
	return &Config{
		RedisHostConfigPath: "",
		RedisConfigPath:     "",
		RedisHostDataPath:   "",
		RedisConfigDataPath: "",
		ConfigSaveFileName:  "",
		Port:                6379,
	}
}
func (c *Config) PrintConfig() {
	println(c.RedisConfigPath)
	println(c.RedisHostConfigPath)
	println(c.RedisHostDataPath)
	println(c.RedisConfigDataPath)
	println(c.ConfigSaveFileName)
	println(c.Port)
}
func TestConfig(t *testing.T) {
	config := NewConfig()
	err := utils.ReadFromYAMLFile("./conf.yaml", config)
	if err != nil {
		println(err)
	}
	config.PrintConfig()
}
