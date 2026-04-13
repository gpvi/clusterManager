//go:build integration
// +build integration

package model

import (
	"testing"
)

// 获取当前执行文件的路径

//func getCurrentDirectory2() string {
//	_, filename, _, _ := runtime.Caller(0)
//	root := path.Dir(path.Dir(filename))
//	configPath := filepath.Join(root, "config", "config.json")
//	println(configPath)
//	return filepath.Dir(filename)
//}

func TestConfig(t *testing.T) {
	config := NewConfig()
	err := config.ReadConfig()
	if err != nil {
		t.Errorf("read config error: %v", err)
	}
	config.PrintConfig()
}
