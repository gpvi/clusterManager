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

type RuntimeConfig struct {
	ProjectRoot         string
	RedisHostConfigPath string
	RedisConfigPath     string
	RedisHostDataPath   string
	RedisConfigDataPath string
	RuntimeStateDir     string
	ContainerInfoFile   string
	ConfigSaveFileName  string
	Backend             string
	DBPath              string
	ContainerdSocket    string
	KubeConfigPath      string
	KubeNamespace       string
	BaseNodePort        int
	ImageName           string
	RedisContainerPort  uint16
}

type Config struct {
	Backend          string `yaml:"backend"`
	DBPath           string `yaml:"db_path"`
	ContainerdSocket string `yaml:"containerd_socket"`
	Paths            struct {
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

func (c *Config) ReadConfig() (*RuntimeConfig, error) {
	_, filename, _, _ := runtime.Caller(0)
	root := path.Dir(path.Dir(filename))
	cfg := &RuntimeConfig{ProjectRoot: root}
	filePath := filepath.Join(root, "config", "conf.yaml")

	err := utils.ReadFromYAMLFile(filePath, c)
	if err != nil {
		return nil, fmt.Errorf("failed to read YAML file: %w", err)
	}

	c.loadFromEnv()
	c.resolvePaths(root)

	if c.Paths.RedisHostConfigPath == "" {
		return nil, fmt.Errorf("redis host config path is empty")
	}
	cfg.RedisHostConfigPath = c.Paths.RedisHostConfigPath

	if c.Paths.RedisHostDataPath == "" {
		return nil, fmt.Errorf("redis host data path is empty")
	}
	cfg.RedisHostDataPath = c.Paths.RedisHostDataPath

	cfg.RedisConfigPath = c.Paths.RedisConfigPath
	if cfg.RedisConfigPath == "" {
		cfg.RedisConfigPath = "/data/redis/config"
	}

	cfg.RedisConfigDataPath = c.Paths.RedisConfigDataPath
	if cfg.RedisConfigDataPath == "" {
		cfg.RedisConfigDataPath = "/data/redis/data"
	}

	cfg.RuntimeStateDir = c.Paths.RuntimeStateDir
	if cfg.RuntimeStateDir == "" {
		cfg.RuntimeStateDir = filepath.Join(root, "runtime")
	}

	configFileName := c.Configs.SaveFileName
	if configFileName == "" {
		configFileName = "run_time_config.yaml"
	}
	cfg.ConfigSaveFileName = resolveStateFilePath(cfg.RuntimeStateDir, configFileName)

	containerInfoFile := c.Configs.ContainerInfoFile
	if containerInfoFile == "" {
		containerInfoFile = "containers.json"
	}
	cfg.ContainerInfoFile = containerInfoFile

	cfg.ImageName = c.Configs.ImageName
	if cfg.ImageName == "" {
		return nil, fmt.Errorf("image name is empty")
	}

	cfg.RedisContainerPort = 6379
	if c.Configs.RedisPort != 0 {
		cfg.RedisContainerPort = c.Configs.RedisPort
	}

	cfg.Backend = c.Backend
	if cfg.Backend == "" {
		cfg.Backend = "k8s"
	}

	cfg.ContainerdSocket = c.ContainerdSocket
	if cfg.ContainerdSocket == "" {
		cfg.ContainerdSocket = defaultContainerdSocket()
	}

	cfg.DBPath = c.DBPath
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(cfg.RuntimeStateDir, "cluster.db")
	}

	cfg.KubeConfigPath = c.Kubernetes.KubeConfigPath
	if cfg.KubeConfigPath == "" {
		cfg.KubeConfigPath = defaultKubeConfig()
	}

	cfg.KubeNamespace = c.Kubernetes.Namespace
	if cfg.KubeNamespace == "" {
		cfg.KubeNamespace = "default"
	}

	cfg.BaseNodePort = c.Kubernetes.BaseNodePort
	if cfg.BaseNodePort == 0 {
		cfg.BaseNodePort = 30000
	}

	return cfg, nil
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
	if v := os.Getenv("CLUSTER_BACKEND"); v != "" {
		c.Backend = v
	}
	if v := os.Getenv("CLUSTER_CONTAINERD_SOCKET"); v != "" {
		c.ContainerdSocket = v
	}
	if v := os.Getenv("CLUSTER_DB_PATH"); v != "" {
		c.DBPath = v
	}
}

func (c *Config) resolvePaths(root string) {
	c.Paths.RedisHostConfigPath = resolveContainerHostPath(root, c.Paths.RedisHostConfigPath)
	c.Paths.RedisHostDataPath = resolveContainerHostPath(root, c.Paths.RedisHostDataPath)
	c.Paths.RuntimeStateDir = resolveLocalAbsPath(root, c.Paths.RuntimeStateDir)
}

func (cfg *RuntimeConfig) ClusterStateDir(clusterName string) string {
	return filepath.Join(cfg.RuntimeStateDir, clusterName)
}

func (cfg *RuntimeConfig) ClusterRuntimeConfigPath(clusterName string) string {
	return resolveStateFilePath(cfg.ClusterStateDir(clusterName), cfg.ConfigSaveFileName)
}

func (cfg *RuntimeConfig) ClusterContainerInfoPath(clusterName string) string {
	return resolveStateFilePath(cfg.ClusterStateDir(clusterName), cfg.ContainerInfoFile)
}

func (cfg *RuntimeConfig) PrintConfig() {
	fmt.Printf("ProjectRoot: %s\n", cfg.ProjectRoot)
	fmt.Printf("Backend: %s\n", cfg.Backend)
	fmt.Printf("RedisHostConfigPath: %s\n", cfg.RedisHostConfigPath)
	fmt.Printf("RedisConfigPath: %s\n", cfg.RedisConfigPath)
	fmt.Printf("RedisHostDataPath: %s\n", cfg.RedisHostDataPath)
	fmt.Printf("RedisConfigDataPath: %s\n", cfg.RedisConfigDataPath)
	fmt.Printf("RuntimeStateDir: %s\n", cfg.RuntimeStateDir)
	fmt.Printf("ConfigSaveFileName: %s\n", cfg.ConfigSaveFileName)
	fmt.Printf("ContainerInfoFile: %s\n", cfg.ContainerInfoFile)
	fmt.Printf("ContainerdSocket: %s\n", cfg.ContainerdSocket)
	fmt.Printf("KubeConfigPath: %s\n", cfg.KubeConfigPath)
	fmt.Printf("KubeNamespace: %s\n", cfg.KubeNamespace)
	fmt.Printf("BaseNodePort: %d\n", cfg.BaseNodePort)
	fmt.Printf("RedisContainerPort: %d\n", cfg.RedisContainerPort)
	fmt.Printf("ImageName: %s\n", cfg.ImageName)
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

func defaultContainerdSocket() string {
	if runtime.GOOS == "windows" {
		return `\\.\pipe\containerd-containerd`
	}
	return "/run/containerd/containerd.sock"
}

func NewConfig() *Config {
	return &Config{}
}

func InitConfig() (*RuntimeConfig, error) {
	config := NewConfig()
	cfg, err := config.ReadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize config: %w", err)
	}
	cfg.PrintConfig()
	return cfg, nil
}
