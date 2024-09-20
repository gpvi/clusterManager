package model

import (
	"fmt"
	"path"
	"path/filepath"
	"redisStudy/utils"
	"runtime"
)

const totalSlots = 16384

var True = true

// RedisHostConfigPath 指定本地的配置文件路径
// var RedisHostConfigPath = "/Users/zhuoqun.niu/Desktop/redis/config"
var RedisHostConfigPath string

// RedisConfigPath  容器redis配置路径
// var RedisConfigPath = "/data/redis/config"
var RedisConfigPath string

// RedisHostDataPath 宿主机redis配置路径
// var RedisHostDataPath = "/Users/zhuoqun.niu/Desktop/redis/data"
var RedisHostDataPath string

// RedisConfigDataPath 容器路径
// var RedisConfigDataPath = "/data/redis/data"
var RedisConfigDataPath string

// ConfigSaveFileName 创建后的配置文件名
// var ConfigSaveFileName = "redis_cluster_config.json"
var ConfigSaveFileName string

var RedisContainerPort uint16

var imageName string

type Config struct {
	RedisHostConfigPath string `yaml:"redis_host_config_path"`
	RedisConfigPath     string `yaml:"redis_config_path"`
	RedisHostDataPath   string `yaml:"redis_host_data_path"`
	RedisConfigDataPath string `yaml:"redis_config_data_path"`
	ConfigSaveFileName  string `yaml:"configs_save_file_name"`
	ImageName           string `yaml:"image_name"`
}

func (c *Config) ReadConfig() error {

	_, filename, _, _ := runtime.Caller(0)
	root := path.Dir(path.Dir(filename))
	filePath := filepath.Join(root, "config", "conf.yaml")
	err := utils.ReadFromYAMLFile(filePath, c)
	if err != nil {
		return err
	}

	if c.RedisConfigPath == "" {
		c.RedisConfigPath = "/data/redis/config"
	} else {
		RedisConfigPath = c.RedisConfigPath
	}

	if c.RedisConfigDataPath == "" {
		c.RedisConfigDataPath = "/data/redis/data"
	} else {
		RedisConfigDataPath = c.RedisConfigDataPath
	}

	if c.RedisHostConfigPath == "" {
		return fmt.Errorf("redis host config path is empty")
	} else {
		RedisHostConfigPath = c.RedisHostConfigPath
	}

	if c.RedisHostDataPath == "" {
		return fmt.Errorf("redis host data path is empty")
	} else {
		RedisHostDataPath = c.RedisHostDataPath
	}

	if c.ConfigSaveFileName == "" {
		c.ConfigSaveFileName = "redis_cluster_config.json"
	} else {
		ConfigSaveFileName = c.ConfigSaveFileName
	}
	if c.ImageName == "" {
		return fmt.Errorf("image name is empty")
	} else {
		imageName = c.ImageName
	}
	return nil
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
	fmt.Printf("RedsiHostConfig:%s\n", c.RedisHostConfigPath)
	fmt.Printf("RedsiConfigPath:%s\n", c.RedisConfigPath)
	fmt.Printf("RedsiHostDataPath:%s\n", c.RedisHostDataPath)
	fmt.Printf("RedsiConfigDataPath:%s\n", c.RedisConfigDataPath)
	fmt.Printf("ConfigSaveFileName:%s\n", c.ConfigSaveFileName)
	fmt.Println("imageName:", imageName)
}

func InitConfig() error {
	config := NewConfig()
	err := config.ReadConfig()
	if err != nil {
		return fmt.Errorf("read config error: %v", err)
	}
	config.PrintConfig()
	return nil
}
