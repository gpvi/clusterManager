package utils

import (
	"fmt"
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
