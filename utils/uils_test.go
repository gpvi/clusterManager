package utils

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
)

func TestUtils(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	root := path.Dir(path.Dir(filename))
	filePath := filepath.Join(root, "config", "conf.yaml")

	// 打印当前工作目录和检查的路径以进行调试
	t.Logf("Current working directory: %v", root)
	t.Logf("Checking file path: %v", filePath)

	// 检查当前项目的 YAML 配置文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("File does not exist: %v", filePath)
	} else {
		t.Logf("File exists: %v", filePath)
	}
}
