package utils

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
)

func TestUtils(t *testing.T) {
	// 获取当前工作目录
	// 获取当前工作目录

	_, filename, _, _ := runtime.Caller(0)
	root := path.Dir(path.Dir(filename))
	dir := root
	// 构造 main.go 文件的绝对路径
	filePath := filepath.Join(dir, "config", "config.json")

	// 打印当前工作目录和检查的路径以进行调试
	t.Logf("Current working directory: %v", dir)
	t.Logf("Checking file path: %v", filePath)

	// 检查 main.go 文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("File does not exist: %v", filePath)
	} else {
		t.Logf("File exists: %v", filePath)
	}
}
