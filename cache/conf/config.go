package conf

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Server configures a single cache node.
type Server struct {
	Port    int    `yaml:"port"`
	API     bool   `yaml:"api"`
	IP      string `yaml:"ip"`
	Gossip  int    `yaml:"gossip"` // memberlist gossip port
}

// SeedPeer returns the memberlist join address for this server.
func (s Server) SeedAddr() string {
	return fmt.Sprintf("%s:%d", s.IP, s.Gossip)
}

// CacheAddr returns the HTTP cache address for this server.
func (s Server) CacheAddr() string {
	return fmt.Sprintf("http://%s:%d", s.IP, s.Port)
}

// Config represents the full application configuration.
type Config struct {
	FrontServer string   `yaml:"front-server"`
	Http        struct {
		DefaultReplicas int    `yaml:"defaultReplicas"`
		DefaultBasePath string `yaml:"defaultBasePath"`
	} `yaml:"http"`
	API           string   `yaml:"api"`
	OnlineServers []Server `yaml:"online-servers"`
	Redis         struct {
		Addr string `yaml:"addr"`
		Port int    `yaml:"port"`
		User string `yaml:"user"`
		Pwd  string `yaml:"pwd"`
	} `yaml:"redis"`
	MySQL struct {
		Password string `yaml:"password"`
		User     string `yaml:"user"`
		Database string `yaml:"database"`
	} `yaml:"mysql"`
	Cache struct {
		MaxBytes int64 `yaml:"max-bytes"` // maximum cache size in bytes
	} `yaml:"cache"`
}

// LoadConfig reads and parses the YAML config file.
func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	var cfg Config
	if err := yaml.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	// Apply defaults.
	for i := range cfg.OnlineServers {
		if cfg.OnlineServers[i].IP == "" {
			cfg.OnlineServers[i].IP = "localhost"
		}
		if cfg.OnlineServers[i].Gossip == 0 {
			cfg.OnlineServers[i].Gossip = cfg.OnlineServers[i].Port + 1000
		}
	}

	return &cfg, nil
}

// SeedAddrs returns all gossip addresses for cluster bootstrapping,
// excluding the node at excludePort.
func (c *Config) SeedAddrs(excludePort int) []string {
	var seeds []string
	for _, s := range c.OnlineServers {
		if s.Port != excludePort {
			seeds = append(seeds, s.SeedAddr())
		}
	}
	return seeds
}

// SelfServer returns the Server config matching the given port.
func (c *Config) SelfServer(port int) (Server, bool) {
	for _, s := range c.OnlineServers {
		if s.Port == port {
			return s, true
		}
	}
	return Server{}, false
}

// Validate checks the configuration for correctness.
func (c *Config) Validate() error {
	if c.FrontServer == "" {
		return fmt.Errorf("front-server is required")
	}
	if len(c.OnlineServers) == 0 {
		return fmt.Errorf("at least one online-server is required")
	}
	for i, s := range c.OnlineServers {
		if s.Port <= 0 || s.Port > 65535 {
			return fmt.Errorf("online-servers[%d]: invalid port %d", i, s.Port)
		}
		if s.IP == "" {
			return fmt.Errorf("online-servers[%d]: ip is required", i)
		}
		if s.Gossip <= 0 || s.Gossip > 65535 {
			return fmt.Errorf("online-servers[%d]: invalid gossip port %d", i, s.Gossip)
		}
	}
	if c.Cache.MaxBytes <= 0 {
		c.Cache.MaxBytes = 256 << 20 // default 256MB
	}
	return nil
}

// APIServer returns the server that has api:true, if any.
func (c *Config) APIServer() (Server, bool) {
	for _, s := range c.OnlineServers {
		if s.API {
			return s, true
		}
	}
	return Server{}, false
}
