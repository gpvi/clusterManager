package config

import (
	"os"
	"testing"

	"gopkg.in/yaml.v2"
)

//const totalSlots = 16384
//
//var True = true
//
//// 指定本地的配置文件路径
//var redisHostConfigPath = "/Users/zhuoqun.niu/Desktop/redis/config"
//
//// 容器redis配置路径
//var redisConfigPath = "/data/redis/config"
//
//// 宿主机redis配置路径
//var redisHostDataPath = "/Users/zhuoqun.niu/Desktop/redis/data"
//
//// 容器路径
//var redisConfigDataPath = "/data/redis/data"
//
//// 创建后的配置文件名
//var ConfigSaveFileName = "redis_cluster_config.json"

// Config holds the YAML file structure
type Config struct {
	RedisHostConfigPath string `yaml:"redis_host_config_path"`
	RedisConfigPath     string `yaml:"redis_config_path"`
	RedisHostDataPath   string `yaml:"redis_host_data_path"`
	RedisConfigDataPath string `yaml:"redis_config_data_path"`
	ConfigSaveFileName  string `yaml:"configs_save_file_name"`
	ImageName           string `yaml:"image_name"`
}

// ReadYAML reads the YAML file and returns a Config struct
func ReadYAML(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
func NewConfig() *Config {
	return &Config{
		RedisHostConfigPath: "",
		RedisConfigPath:     "",
		RedisHostDataPath:   "",
		RedisConfigDataPath: "",
		ConfigSaveFileName:  "",
		ImageName:           "",
	}
}
func (c *Config) PrintConfig() {
	println(c.RedisConfigPath)
	println(c.RedisHostConfigPath)
	println(c.RedisHostDataPath)
	println(c.RedisConfigDataPath)
	println(c.ConfigSaveFileName)
	println(c.ImageName)
}
func TestConfig(t *testing.T) {
	config := NewConfig()
	config, err := ReadYAML("./conf.yaml")
	if err != nil {
		println(err)
	}
	config.PrintConfig()
	// 检查地址是否可访问
	if _, err := os.Stat(config.RedisHostConfigPath); os.IsNotExist(err) {
		println("redis host config path not exist %v", err)
	}
	if _, err := os.Stat(config.RedisHostDataPath); os.IsNotExist(err) {
		println("redis host data path not exist %v", err)
	}
}
