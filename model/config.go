package model

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"redisClusterManager/utils"
	"runtime"
	"strconv"
)

const TotalSlots int = 16384

var True = true

var (
	ProjectRoot         string
	RedisHostConfigPath string
	RedisConfigPath     string
	RedisHostDataPath   string
	RedisConfigDataPath string
	RuntimeStateDir     string
	ContainerInfoFile   string
	ConfigSaveFileName  string
	KubeConfigPath      string
	KubeNamespace       string
	BaseNodePort        int
	imageName           string
	RedisContainerPort  uint16 = 6379
)

type Config struct {
	Paths struct {
		RedisHostConfigPath string `yaml:"redis_host_config_path"`
		RedisConfigPath     string `yaml:"redis_config_path"`
		RedisHostDataPath   string `yaml:"redis_host_data_path"`
		RedisConfigDataPath string `yaml:"redis_config_data_path"`
		RuntimeStateDir     string `yaml:"runtime_state_dir"`
	} `yaml:"paths"`
	Kubernetes struct {
		KubeConfigPath string `yaml:"kube_config_path"`
		Namespace      string `yaml:"namespace"`
		BaseNodePort   int    `yaml:"base_node_port"`
	} `yaml:"kubernetes"`
	Configs struct {
		SaveFileName      string `yaml:"save_file_name"`
		ContainerInfoFile string `yaml:"container_info_file_name"`
		ImageName         string `yaml:"image_name"`
		RedisPort         uint16 `yaml:"redis_port"`
	} `yaml:"configs"`
}

func (c *Config) ReadConfig() error {
	_, filename, _, _ := runtime.Caller(0)
	root := path.Dir(path.Dir(filename))
	ProjectRoot = root
	filePath := filepath.Join(root, "config", "conf.yaml")

	err := utils.ReadFromYAMLFile(filePath, c)
	if err != nil {
		return fmt.Errorf("failed to read YAML file: %w", err)
	}

	c.loadFromEnv()
	c.resolvePaths(root)

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

	KubeConfigPath = c.Kubernetes.KubeConfigPath
	if KubeConfigPath == "" {
		KubeConfigPath = defaultKubeConfig()
	}

	KubeNamespace = c.Kubernetes.Namespace
	if KubeNamespace == "" {
		KubeNamespace = "default"
	}

	BaseNodePort = c.Kubernetes.BaseNodePort
	if BaseNodePort == 0 {
		BaseNodePort = 30000
	}

	return nil
}

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
	if v := os.Getenv("KUBECONFIG"); v != "" {
		c.Kubernetes.KubeConfigPath = v
	}
	if v := os.Getenv("KUBE_NAMESPACE"); v != "" {
		c.Kubernetes.Namespace = v
	}
	if v := os.Getenv("BASE_NODE_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			c.Kubernetes.BaseNodePort = port
		}
	}
	if v := os.Getenv("CLUSTER_REDIS_PORT"); v != "" {
		if port, err := strconv.ParseUint(v, 10, 16); err == nil {
			c.Configs.RedisPort = uint16(port)
		}
	}
}

func (c *Config) resolvePaths(root string) {
	c.Paths.RedisHostConfigPath = resolveContainerHostPath(root, c.Paths.RedisHostConfigPath)
	c.Paths.RedisHostDataPath = resolveContainerHostPath(root, c.Paths.RedisHostDataPath)
	c.Paths.RuntimeStateDir = resolveLocalAbsPath(root, c.Paths.RuntimeStateDir)
}

func resolveContainerHostPath(root, p string) string {
	if isUnixStyleAbsPath(p) {
		return p
	}
	return resolveLocalAbsPath(root, p)
}

func resolveLocalAbsPath(root, p string) string {
	if filepath.IsAbs(p) || p == "" {
		return p
	}
	return filepath.Join(root, p)
}

func isUnixStyleAbsPath(p string) bool {
	return len(p) > 0 && p[0] == '/'
}

func resolveStateFilePath(root, name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return filepath.Join(root, name)
}

func defaultKubeConfig() string {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".kube", "config")
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

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) PrintConfig() {
	fmt.Printf("ProjectRoot: %s\n", ProjectRoot)
	fmt.Printf("RedisHostConfigPath: %s\n", RedisHostConfigPath)
	fmt.Printf("RedisConfigPath: %s\n", RedisConfigPath)
	fmt.Printf("RedisHostDataPath: %s\n", RedisHostDataPath)
	fmt.Printf("RedisConfigDataPath: %s\n", RedisConfigDataPath)
	fmt.Printf("RuntimeStateDir: %s\n", RuntimeStateDir)
	fmt.Printf("ConfigSaveFileName: %s\n", ConfigSaveFileName)
	fmt.Printf("ContainerInfoFile: %s\n", ContainerInfoFile)
	fmt.Printf("KubeConfigPath: %s\n", KubeConfigPath)
	fmt.Printf("KubeNamespace: %s\n", KubeNamespace)
	fmt.Printf("BaseNodePort: %d\n", BaseNodePort)
	fmt.Printf("RedisContainerPort: %d\n", RedisContainerPort)
	fmt.Printf("ImageName: %s\n", imageName)
}

func InitConfig() error {
	config := NewConfig()
	err := config.ReadConfig()
	if err != nil {
		return fmt.Errorf("failed to initialize config: %w", err)
	}
	config.PrintConfig()
	return nil
}
