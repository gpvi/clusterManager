package config

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

// Config holds the YAML structure
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

	// Replace placeholders with actual values
	replaceVariables(&config)

	return &config, nil
}

// replaceVariables replaces placeholders in the Config struct
func replaceVariables(config *Config) {
	placeholders := map[string]string{
		"${user_name}": config.UserName,
	}

	config.Paths.RedisHostConfigPath = replacePlaceholders(config.Paths.RedisHostConfigPath, placeholders)
	config.Paths.RedisHostDataPath = replacePlaceholders(config.Paths.RedisHostDataPath, placeholders)
}

// replacePlaceholders replaces placeholders in a string
func replacePlaceholders(input string, placeholders map[string]string) string {
	for placeholder, value := range placeholders {
		input = strings.ReplaceAll(input, placeholder, value)
	}
	return input
}

// PrintConfig prints the entire configuration for debugging
func (c *Config) PrintConfig() {
	fmt.Println("UserName:", c.UserName)
	fmt.Println("Paths:")
	fmt.Println("  RedisHostConfigPath:", c.Paths.RedisHostConfigPath)
	fmt.Println("  RedisConfigPath:", c.Paths.RedisConfigPath)
	fmt.Println("  RedisHostDataPath:", c.Paths.RedisHostDataPath)
	fmt.Println("  RedisConfigDataPath:", c.Paths.RedisConfigDataPath)
	fmt.Println("Configs:")
	fmt.Println("  SaveFileName:", c.Configs.SaveFileName)
	fmt.Println("  ImageName:", c.Configs.ImageName)
}

// TestConfig demonstrates reading and using the YAML configuration
func TestConfig(t *testing.T) {
	config, err := ReadYAML("./conf.yaml")
	if err != nil {
		fmt.Println("Error reading YAML file:", err)
		return
	}

	config.PrintConfig()

	// Check if paths exist
	if _, err := os.Stat(config.Paths.RedisHostConfigPath); os.IsNotExist(err) {
		fmt.Printf("Redis host config path does not exist: %s\n", config.Paths.RedisHostConfigPath)
	}
	if _, err := os.Stat(config.Paths.RedisHostDataPath); os.IsNotExist(err) {
		fmt.Printf("Redis host data path does not exist: %s\n", config.Paths.RedisHostDataPath)
	}
}
