package model

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"redisStudy/utils"
	"runtime"
	"strconv"
)

const TotalSlots int = 16384

var True = true

// 全局变量，用于存储配置数据
var (
	ProjectRoot         string
	RedisHostConfigPath string
	RedisConfigPath     string
	RedisHostDataPath   string
	RedisConfigDataPath string
	RuntimeStateDir     string
	ContainerInfoFile   string
	ConfigSaveFileName  string
	PodmanEndpoint      string
	PodmanNetworkName   string
	imageName           string
	RedisContainerPort  uint16 = 6379
)

// Config 结构体用于映射 YAML 配置文件
type Config struct {
	Paths struct {
		RedisHostConfigPath string `yaml:"redis_host_config_path"`
		RedisConfigPath     string `yaml:"redis_config_path"`
		RedisHostDataPath   string `yaml:"redis_host_data_path"`
		RedisConfigDataPath string `yaml:"redis_config_data_path"`
		RuntimeStateDir     string `yaml:"runtime_state_dir"`
	} `yaml:"paths"`
	Podman struct {
		Endpoint    string `yaml:"endpoint"`
		NetworkName string `yaml:"network_name"`
	} `yaml:"podman"`
	Configs struct {
		SaveFileName      string `yaml:"save_file_name"`
		ContainerInfoFile string `yaml:"container_info_file_name"`
		ImageName         string `yaml:"image_name"`
		RedisPort         uint16 `yaml:"redis_port"`
	} `yaml:"configs"`
}

// ReadConfig 读取 YAML 配置文件并填充配置项
func (c *Config) ReadConfig() error {
	// 获取项目根目录路径
	_, filename, _, _ := runtime.Caller(0)
	root := path.Dir(path.Dir(filename))
	ProjectRoot = root
	filePath := filepath.Join(root, "config", "conf.yaml")

	// 调用工具函数读取 YAML 文件
	err := utils.ReadFromYAMLFile(filePath, c)
	if err != nil {
		return fmt.Errorf("failed to read YAML file: %w", err)
	}

	// 1. 优先从环境变量读取覆盖
	c.loadFromEnv()

	// 2. 处理相对路径，将其转换为绝对路径
	c.resolvePaths(root)

	// 验证并设置全局变量
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

	RuntimeStateDir = c.Paths.RuntimeStateDir
	if RuntimeStateDir == "" {
		RuntimeStateDir = filepath.Join(root, "runtime")
	}

	configFileName := c.Configs.SaveFileName
	if configFileName == "" {
		configFileName = "run_time_config.yaml"
	}
	ConfigSaveFileName = resolveStateFilePath(RuntimeStateDir, configFileName)

	containerInfoFile := c.Configs.ContainerInfoFile
	if containerInfoFile == "" {
		containerInfoFile = "containers.json"
	}
	ContainerInfoFile = containerInfoFile

	imageName = c.Configs.ImageName
	if imageName == "" {
		return fmt.Errorf("image name is empty")
	}

	if c.Configs.RedisPort != 0 && RedisContainerPort == 6379 {
		RedisContainerPort = c.Configs.RedisPort
	}

	PodmanEndpoint = c.Podman.Endpoint
	if PodmanEndpoint == "" {
		PodmanEndpoint = defaultPodmanEndpoint()
	}

	PodmanNetworkName = c.Podman.NetworkName
	if PodmanNetworkName == "" {
		PodmanNetworkName = "podman"
	}

	return nil
}

// loadFromEnv 从环境变量读取配置并覆盖当前配置
func (c *Config) loadFromEnv() {
	if v := os.Getenv("REDIS_HOST_CONFIG_PATH"); v != "" {
		c.Paths.RedisHostConfigPath = v
	}
	if v := os.Getenv("REDIS_CONFIG_PATH"); v != "" {
		c.Paths.RedisConfigPath = v
	}
	if v := os.Getenv("REDIS_HOST_DATA_PATH"); v != "" {
		c.Paths.RedisHostDataPath = v
	}
	if v := os.Getenv("REDIS_CONFIG_DATA_PATH"); v != "" {
		c.Paths.RedisConfigDataPath = v
	}
	if v := os.Getenv("CLUSTER_SAVE_FILE_NAME"); v != "" {
		c.Configs.SaveFileName = v
	}
	if v := os.Getenv("CLUSTER_CONTAINER_INFO_FILE"); v != "" {
		c.Configs.ContainerInfoFile = v
	}
	if v := os.Getenv("CLUSTER_IMAGE_NAME"); v != "" {
		c.Configs.ImageName = v
	}
	if v := os.Getenv("CLUSTER_STATE_DIR"); v != "" {
		c.Paths.RuntimeStateDir = v
	}
	if v := os.Getenv("PODMAN_ENDPOINT"); v != "" {
		c.Podman.Endpoint = v
	} else if v := os.Getenv("CONTAINER_HOST"); v != "" {
		c.Podman.Endpoint = v
	}
	if v := os.Getenv("PODMAN_NETWORK_NAME"); v != "" {
		c.Podman.NetworkName = v
	}
	if v := os.Getenv("CLUSTER_REDIS_PORT"); v != "" {
		if port, err := strconv.ParseUint(v, 10, 16); err == nil {
			c.Configs.RedisPort = uint16(port)
		}
	}
}

// resolvePaths 将所有相对路径转换为基于项目根目录的绝对路径
func (c *Config) resolvePaths(root string) {
	c.Paths.RedisHostConfigPath = resolveAbsPath(root, c.Paths.RedisHostConfigPath)
	c.Paths.RedisHostDataPath = resolveAbsPath(root, c.Paths.RedisHostDataPath)
	c.Paths.RuntimeStateDir = resolveAbsPath(root, c.Paths.RuntimeStateDir)
}

// resolveAbsPath 如果是相对路径，则返回相对于 root 的绝对路径
func resolveAbsPath(root, p string) string {
	if filepath.IsAbs(p) || p == "" {
		return p
	}
	return filepath.Join(root, p)
}

func resolveStateFilePath(root, name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return filepath.Join(root, name)
}

func ClusterStateDir(clusterName string) string {
	return filepath.Join(RuntimeStateDir, clusterName)
}

func ClusterRuntimeConfigPath(clusterName string) string {
	return resolveStateFilePath(ClusterStateDir(clusterName), ConfigSaveFileName)
}

func ClusterContainerInfoPath(clusterName string) string {
	return resolveStateFilePath(ClusterStateDir(clusterName), ContainerInfoFile)
}

func defaultPodmanEndpoint() string {
	if v := os.Getenv("CONTAINER_HOST"); v != "" {
		return v
	}

	switch runtime.GOOS {
	case "windows":
		return "npipe://\\\\.\\pipe\\podman-machine-default"
	case "darwin":
		home, err := os.UserHomeDir()
		if err == nil && home != "" {
			return "unix://" + filepath.ToSlash(filepath.Join(home, ".local", "share", "containers", "podman", "machine", "podman.sock"))
		}
	case "linux":
		if xdgRuntimeDir := os.Getenv("XDG_RUNTIME_DIR"); xdgRuntimeDir != "" {
			return "unix://" + filepath.ToSlash(filepath.Join(xdgRuntimeDir, "podman", "podman.sock"))
		}
		home, err := os.UserHomeDir()
		if err == nil && home != "" {
			return "unix://" + filepath.ToSlash(filepath.Join(home, ".local", "share", "containers", "podman", "podman.sock"))
		}
	}

	return ""
}

// NewConfig 创建一个新的配置对象
func NewConfig() *Config {
	return &Config{}
}

// PrintConfig 打印当前配置
func (c *Config) PrintConfig() {
	fmt.Printf("ProjectRoot: %s\n", ProjectRoot)
	fmt.Printf("RedisHostConfigPath: %s\n", RedisHostConfigPath)
	fmt.Printf("RedisConfigPath: %s\n", RedisConfigPath)
	fmt.Printf("RedisHostDataPath: %s\n", RedisHostDataPath)
	fmt.Printf("RedisConfigDataPath: %s\n", RedisConfigDataPath)
	fmt.Printf("RuntimeStateDir: %s\n", RuntimeStateDir)
	fmt.Printf("ConfigSaveFileName: %s\n", ConfigSaveFileName)
	fmt.Printf("ContainerInfoFile: %s\n", ContainerInfoFile)
	fmt.Printf("PodmanEndpoint: %s\n", PodmanEndpoint)
	fmt.Printf("PodmanNetworkName: %s\n", PodmanNetworkName)
	fmt.Printf("RedisContainerPort: %d\n", RedisContainerPort)
	fmt.Printf("ImageName: %s\n", imageName)
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
