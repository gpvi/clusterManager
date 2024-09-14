package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"strconv"
	"strings"
)

// StringToUint16 将字符串转换为 uint16
func StringToUint16(str string) (uint16, error) {
	// 将字符串解析为整数
	num, err := strconv.Atoi(str)
	if err != nil {
		return 0, err // 返回错误
	}

	// 检查是否超出 uint16 的范围
	if num < 0 || num > 65535 {
		return 0, fmt.Errorf("数字超出 uint16 范围")
	}

	// 转换为 uint16
	return uint16(num), nil
}

// ParseIPPort 将IP:Port 格式的字符串分割为 IP 和 Port
func ParseIPPort(ipPort string) (string, string) {
	parts := strings.Split(ipPort, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", ""
}

func ExecuteClusterCommand(ctx context.Context, client *redis.Client, args ...interface{}) (string, error) {
	result, err := client.Do(ctx, args...).Text()
	if err != nil {
		return "", err
	}
	return result, nil
}

// ParseInt64 简单的字符串转 int64
func ParseInt64(value string) int64 {
	var result int64
	_, err := fmt.Sscanf(value, "%d", &result)
	if err != nil {
		fmt.Printf("Error parsing int64: %v\n", err)
	}
	return result
}

// 将数据写入 JSON 文件
func writeToJSONFile(fileName string, data interface{}) error {
	// 将结构体转换为 JSON 字符串并格式化
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	// 创建或打开文件
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// 将 JSON 数据写入文件
	_, err = file.Write(jsonData)
	if err != nil {
		return err
	}

	fmt.Printf("Data written to file: %s\n", fileName)
	return nil
}

// 从 JSON 文件中读取数据
func readFromJSONFile(fileName string, data interface{}) error {
	// 读取文件内容
	fileData, err := os.ReadFile(fileName) // Go 1.16 后 ioutil 被弃用，改为 os.ReadFile
	if err != nil {
		return err
	}

	// 将 JSON 数据反序列化为结构体
	err = json.Unmarshal(fileData, data)
	if err != nil {
		return err
	}

	return nil
}
