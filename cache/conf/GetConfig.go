package conf

// ConfigData is a lazily loaded config accessor.
type ConfigData struct {
	path   string
	config *Config
}

// NewConfigData creates a config accessor. Load must be called before use.
func NewConfigData(path string) *ConfigData {
	return &ConfigData{path: path}
}

// Load reads and caches the configuration.
func (c *ConfigData) Load() error {
	cfg, err := LoadConfig(c.path)
	if err != nil {
		return err
	}
	c.config = cfg
	return nil
}

func (c *ConfigData) GetFrontServer() string  { return c.config.FrontServer }
func (c *ConfigData) GetApi() string           { return c.config.API }
func (c *ConfigData) GetOnlineServers() []Server { return c.config.OnlineServers }
func (c *ConfigData) GetConfig() *Config       { return c.config }
