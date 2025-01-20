package model

import (
	"fmt"
	"path"
	"path/filepath"
	"redisStudy/utils"
	"runtime"
	"strings"
)

const TotalSlots int = 16384

var True = true

// 全局变量，用于存储配置数据
var (
	RedisHostConfigPath string
	RedisConfigPath     string
	RedisHostDataPath   string
	RedisConfigDataPath string
	ConfigSaveFileName  string
	imageName           string
	RedisContainerPort  uint16 = 6379
)

// Config 结构体用于映射 YAML 配置文件
type Config struct {
	UserName string `yaml:"user_name"`
	Paths    struct {
		RedisHostConfigPath string `yaml:"redis_host_config_path"`
		RedisConfigPath     string `yaml:"redis_config_path"`
		RedisHostDataPath   string `yaml:"redis_host_data_path"`
		RedisConfigDataPath string `yaml:"redis_config_data_path"`
	} `yaml:"paths"`
	Configs struct {
		SaveFileName string `yaml:"save_file_name"`
		ImageName    string `yaml:"image_name"`
	} `yaml:"configs"`
}

// ReadConfig 读取 YAML 配置文件并填充配置项
func (c *Config) ReadConfig() error {
	// 获取项目根目录路径
	_, filename, _, _ := runtime.Caller(0)
	root := path.Dir(path.Dir(filename))
	filePath := filepath.Join(root, "config", "conf.yaml")

	// 调用工具函数读取 YAML 文件
	err := utils.ReadFromYAMLFile(filePath, c)
	if err != nil {
		return fmt.Errorf("failed to read YAML file: %w", err)
	}

	// 动态替换路径中的占位符 `${user_name}`
	replacePlaceholders(c)

	// 验证配置是否完整
	if c.Paths.RedisHostConfigPath == "" {
		return fmt.Errorf("redis host config path is empty")
	}
	RedisHostConfigPath = c.Paths.RedisHostConfigPath

	if c.Paths.RedisHostDataPath == "" {
		return fmt.Errorf("redis host data path is empty")
	}
	RedisHostDataPath = c.Paths.RedisHostDataPath

	RedisConfigPath = c.Paths.RedisConfigPath
	if RedisConfigPath == "" {
		RedisConfigPath = "/data/redis/config"
	}

	RedisConfigDataPath = c.Paths.RedisConfigDataPath
	if RedisConfigDataPath == "" {
		RedisConfigDataPath = "/data/redis/data"
	}

	ConfigSaveFileName = c.Configs.SaveFileName
	if ConfigSaveFileName == "" {
		ConfigSaveFileName = "redis_cluster_config.json"
	}

	imageName = c.Configs.ImageName
	if imageName == "" {
		return fmt.Errorf("image name is empty")
	}

	return nil
}

// replacePlaceholders 替换路径中的占位符 `${user_name}`
func replacePlaceholders(c *Config) {
	placeholders := map[string]string{
		"${user_name}": c.UserName,
	}

	c.Paths.RedisHostConfigPath = replacePlaceholder(c.Paths.RedisHostConfigPath, placeholders)
	c.Paths.RedisHostDataPath = replacePlaceholder(c.Paths.RedisHostDataPath, placeholders)
}

// replacePlaceholder 替换单个字符串中的占位符
func replacePlaceholder(input string, placeholders map[string]string) string {
	for placeholder, value := range placeholders {
		input = strings.ReplaceAll(input, placeholder, value)
	}
	return input
}

// NewConfig 创建一个新的配置对象
func NewConfig() *Config {
	return &Config{}
}

// PrintConfig 打印当前配置
func (c *Config) PrintConfig() {
	fmt.Printf("UserName: %s\n", c.UserName)
	fmt.Printf("RedisHostConfigPath: %s\n", c.Paths.RedisHostConfigPath)
	fmt.Printf("RedisConfigPath: %s\n", c.Paths.RedisConfigPath)
	fmt.Printf("RedisHostDataPath: %s\n", c.Paths.RedisHostDataPath)
	fmt.Printf("RedisConfigDataPath: %s\n", c.Paths.RedisConfigDataPath)
	fmt.Printf("ConfigSaveFileName: %s\n", c.Configs.SaveFileName)
	fmt.Printf("ImageName: %s\n", c.Configs.ImageName)
}

// InitConfig 初始化配置，读取 YAML 文件并设置全局变量
func InitConfig() error {
	config := NewConfig()
	err := config.ReadConfig()
	if err != nil {
		return fmt.Errorf("failed to initialize config: %w", err)
	}
	config.PrintConfig()
	return nil
}
