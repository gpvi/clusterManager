package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	"gopkg.in/yaml.v3"
)

// Config holds the YAML structure
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
		ImageName         string `yaml:"image_name"`
		RedisPort         uint16 `yaml:"redis_port"`
	} `yaml:"configs"`
}

// ReadYAML reads and parses the YAML file into a Config struct
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

// PrintConfig prints the entire configuration for debugging
func (c *Config) PrintConfig() {
	fmt.Println("Paths:")
	fmt.Println("  RedisHostConfigPath:", c.Paths.RedisHostConfigPath)
	fmt.Println("  RedisConfigPath:", c.Paths.RedisConfigPath)
	fmt.Println("  RedisHostDataPath:", c.Paths.RedisHostDataPath)
	fmt.Println("  RedisConfigDataPath:", c.Paths.RedisConfigDataPath)
	fmt.Println("  RuntimeStateDir:", c.Paths.RuntimeStateDir)
	fmt.Println("Podman:")
	fmt.Println("  Endpoint:", c.Podman.Endpoint)
	fmt.Println("  NetworkName:", c.Podman.NetworkName)
	fmt.Println("Configs:")
	fmt.Println("  SaveFileName:", c.Configs.SaveFileName)
	fmt.Println("  ImageName:", c.Configs.ImageName)
	fmt.Println("  RedisPort:", c.Configs.RedisPort)
}

// TestConfig demonstrates reading and using the YAML configuration
func TestConfig(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	root := path.Dir(path.Dir(filename))
	configPath := filepath.Join(root, "config", "conf.yaml")

	config, err := ReadYAML(configPath)
	if err != nil {
		t.Fatalf("Error reading YAML file: %v", err)
	}

	config.PrintConfig()

	if config.Configs.ImageName == "" {
		t.Fatalf("image name should not be empty")
	}
	if config.Configs.SaveFileName == "" {
		t.Fatalf("save file name should not be empty")
	}
	if _, err := os.Stat(filepath.Join(root, config.Paths.RedisHostConfigPath)); os.IsNotExist(err) {
		t.Fatalf("Redis host config path does not exist: %s", config.Paths.RedisHostConfigPath)
	}
}
