package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"redisClusterManager/cluster/model"
	"redisClusterManager/cluster/utils"
	"runtime"
	"strconv"
	"strings"
)

type RuntimeConfig struct {
	ProjectRoot         string
	RedisHostConfigPath string
	RedisConfigPath     string
	RedisHostDataPath   string
	RedisConfigDataPath string
	RuntimeStateDir     string
	ConfigSaveFileName  string
	Backend             string
	DBPath              string
	ContainerdSocket    string
	ImageName           string
	RedisContainerPort  uint16
	Cache               *CacheConfig
	DNS                 *DNSConfig
	// CacheInvalidator is set by the cache subsystem when running in-process.
	// If non-nil, it is registered with each ClusterManager created by action functions.
	CacheInvalidator model.CacheInvalidator
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
	Configs struct {
		SaveFileName      string `yaml:"save_file_name"`
		ImageName         string `yaml:"image_name"`
		RedisPort         uint16 `yaml:"redis_port"`
	} `yaml:"configs"`
	Cache CacheConfig `yaml:"cache"`
	DNS   DNSConfig   `yaml:"dns"`
}

// CacheConfig holds configuration for the embedded cache subsystem (GeeCache).
type CacheConfig struct {
	Enabled          bool     `yaml:"enabled"`
	MaxBytes         int64    `yaml:"max_bytes"`
	TTL              int      `yaml:"ttl"`
	HotKeyThreshold  int      `yaml:"hot_key_threshold"`
	HotReplicas      int      `yaml:"hot_replicas"`
	DBRateLimit      int      `yaml:"db_rate_limit"`
	ServerIP         string   `yaml:"server_ip"`
	ServerPort       int      `yaml:"server_port"`
	GossipPort       int      `yaml:"gossip_port"`
	APIGateway       bool     `yaml:"api_gateway"`
	CacheGroupName   string   `yaml:"cache_group_name"`
	Seeds            []string `yaml:"seeds"`
	TLSMode          string   `yaml:"tls_mode"`
	TLSCertFile      string   `yaml:"tls_cert_file"`
	TLSKeyFile       string   `yaml:"tls_key_file"`
	TLSCAFile        string   `yaml:"tls_ca_file"`
}

// DNSConfig holds configuration for DNS-based node discovery.
type DNSConfig struct {
	Enabled        bool   `yaml:"enabled"`
	Domain         string `yaml:"domain"`
	NamingTemplate string `yaml:"naming_template"`
}

// RedisClusterConfig holds the per-cluster configuration persisted alongside runtime state.
type RedisClusterConfig struct {
	NodesPerShard int    `yaml:"nodes_per_shard"`
	Port          uint16 `yaml:"port"`
}

func (c RedisClusterConfig) EffectiveNodesPerShard() int {
	return c.NodesPerShard
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
		cfg.Backend = "podman"
	}

	cfg.ContainerdSocket = c.ContainerdSocket
	if cfg.ContainerdSocket == "" {
		cfg.ContainerdSocket = defaultContainerdSocket()
	}

	cfg.DBPath = c.DBPath
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(cfg.RuntimeStateDir, "cluster.db")
	}

	c.resolveCacheDefaults()
	c.resolveDNSDefaults()
	cfg.Cache = &c.Cache
	cfg.DNS = &c.DNS

	return cfg, nil
}

func (c *Config) resolveDNSDefaults() {
	if c.DNS.NamingTemplate == "" {
		c.DNS.NamingTemplate = "{{.ClusterName}}-redis-{{.Index}}.{{.ClusterName}}-svc.{{.Namespace}}"
	}
}

func (c *Config) resolveCacheDefaults() {
	if c.Cache.MaxBytes == 0 {
		c.Cache.MaxBytes = 1 << 30 // 1GB
	}
	if c.Cache.TTL == 0 {
		c.Cache.TTL = 3600
	}
	if c.Cache.HotKeyThreshold == 0 {
		c.Cache.HotKeyThreshold = 100
	}
	if c.Cache.HotReplicas == 0 {
		c.Cache.HotReplicas = 2
	}
	if c.Cache.DBRateLimit == 0 {
		c.Cache.DBRateLimit = 200
	}
	if c.Cache.ServerPort == 0 {
		c.Cache.ServerPort = 8001
	}
	if c.Cache.GossipPort == 0 {
		c.Cache.GossipPort = 9001
	}
	if c.Cache.CacheGroupName == "" {
		c.Cache.CacheGroupName = "default"
	}
	if c.Cache.TLSMode == "" {
		c.Cache.TLSMode = "insecure"
	}
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
	if v := os.Getenv("CLUSTER_IMAGE_NAME"); v != "" {
		c.Configs.ImageName = v
	}
	if v := os.Getenv("CLUSTER_STATE_DIR"); v != "" {
		c.Paths.RuntimeStateDir = v
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
	// DNS env var overrides.
	if v := os.Getenv("CLUSTER_DNS_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			c.DNS.Enabled = b
		}
	}
	if v := os.Getenv("CLUSTER_DNS_DOMAIN"); v != "" {
		c.DNS.Domain = v
	}
	if v := os.Getenv("CLUSTER_DNS_NAMING_TEMPLATE"); v != "" {
		c.DNS.NamingTemplate = v
	}
	// Cache subsystem overrides.
	if v := os.Getenv("CACHE_MAX_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			c.Cache.MaxBytes = n
		}
	}
	if v := os.Getenv("CACHE_TTL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Cache.TTL = n
		}
	}
	if v := os.Getenv("CACHE_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Cache.ServerPort = n
		}
	}
	if v := os.Getenv("CACHE_GOSSIP_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Cache.GossipPort = n
		}
	}
	if v := os.Getenv("CACHE_TLS_MODE"); v != "" {
		c.Cache.TLSMode = v
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

func (cfg *RuntimeConfig) PrintConfig() {
	fmt.Printf("ProjectRoot: %s\n", cfg.ProjectRoot)
	fmt.Printf("Backend: %s\n", cfg.Backend)
	fmt.Printf("RedisHostConfigPath: %s\n", cfg.RedisHostConfigPath)
	fmt.Printf("RedisConfigPath: %s\n", cfg.RedisConfigPath)
	fmt.Printf("RedisHostDataPath: %s\n", cfg.RedisHostDataPath)
	fmt.Printf("RedisConfigDataPath: %s\n", cfg.RedisConfigDataPath)
	fmt.Printf("RuntimeStateDir: %s\n", cfg.RuntimeStateDir)
	fmt.Printf("ConfigSaveFileName: %s\n", cfg.ConfigSaveFileName)
	fmt.Printf("ContainerdSocket: %s\n", cfg.ContainerdSocket)
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

// Cache accessor methods on RuntimeConfig.
func (cfg *RuntimeConfig) CacheGroupName() string {
	if cfg.Cache == nil {
		return "default"
	}
	return cfg.Cache.CacheGroupName
}
func (cfg *RuntimeConfig) CacheMaxBytes() int64 {
	if cfg.Cache == nil {
		return 1 << 30
	}
	return cfg.Cache.MaxBytes
}
func (cfg *RuntimeConfig) CacheTTL() int {
	if cfg.Cache == nil {
		return 3600
	}
	return cfg.Cache.TTL
}
func (cfg *RuntimeConfig) CacheHotKeyThreshold() int {
	if cfg.Cache == nil {
		return 100
	}
	return cfg.Cache.HotKeyThreshold
}
func (cfg *RuntimeConfig) CacheHotReplicas() int {
	if cfg.Cache == nil {
		return 2
	}
	return cfg.Cache.HotReplicas
}
func (cfg *RuntimeConfig) CacheDBRateLimit() int {
	if cfg.Cache == nil {
		return 200
	}
	return cfg.Cache.DBRateLimit
}
func (cfg *RuntimeConfig) CachePort() int {
	if cfg.Cache == nil {
		return 8001
	}
	return cfg.Cache.ServerPort
}
func (cfg *RuntimeConfig) CacheGossipPort() int {
	if cfg.Cache == nil {
		return 9001
	}
	return cfg.Cache.GossipPort
}
func (cfg *RuntimeConfig) CacheSeeds() []string {
	if cfg.Cache == nil {
		return nil
	}
	return cfg.Cache.Seeds
}
func (cfg *RuntimeConfig) CacheTLSMode() string {
	if cfg.Cache == nil {
		return "insecure"
	}
	return cfg.Cache.TLSMode
}
func (cfg *RuntimeConfig) CacheAPIAddr() string {
	if cfg.Cache == nil || cfg.Cache.ServerIP == "" {
		return ":9999"
	}
	return fmt.Sprintf("%s:9999", cfg.Cache.ServerIP)
}

// DNS accessor methods on RuntimeConfig.
func (cfg *RuntimeConfig) DNSEnabled() bool {
	if cfg.DNS == nil {
		return false
	}
	return cfg.DNS.Enabled
}
func (cfg *RuntimeConfig) DNSDomain() string {
	if cfg.DNS == nil || cfg.DNS.Domain == "" {
		return "svc.cluster.local"
	}
	return cfg.DNS.Domain
}
func (cfg *RuntimeConfig) DNSNamingTemplate() string {
	if cfg.DNS == nil {
		return ""
	}
	return cfg.DNS.NamingTemplate
}

// BuildHostname expands the DNS naming template for a given cluster node.
// Returns empty string when DNS is disabled. Falls back to a simple
// "{clusterName}-redis-{index}" pattern when the template is empty.
func (cfg *RuntimeConfig) BuildHostname(clusterName string, index int) string {
	if cfg.DNS == nil || !cfg.DNS.Enabled {
		return ""
	}
	// Fallback if template is empty or invalid: use simple pattern
	if cfg.DNS.NamingTemplate == "" {
		return fmt.Sprintf("%s-redis-%d", clusterName, index)
	}
	// Simple string replacement for known placeholders
	name := cfg.DNS.NamingTemplate
	name = strings.ReplaceAll(name, "{{.ClusterName}}", clusterName)
	name = strings.ReplaceAll(name, "{{.Index}}", strconv.Itoa(index))
	name = strings.ReplaceAll(name, "{{.Namespace}}", "default")
	return name
}
