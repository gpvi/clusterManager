package DataStruct

import (
	"fmt"
	"testing"
)

func TestName(t *testing.T) {
	data := `147ff14ec72be84cff55a4e05b3d32e2d727da95 10.88.3.59:6379@16379 master - 0 1725596407022 3 connected 0-5460
c205ed8fd181f95b61d11525effdc478864c91d8 10.88.3.61:6379@16379 master - 0 1725596405004 0 connected 5461-10921
42eecdb2638230925c8ce268c2f16f35edb20d4a 10.88.3.60:6379@16379 master - 0 1725596406012 2 connected 10922-14814
7964bf21b2239e4c0df2663995f1333343380598 10.88.3.63:6379@16379 slave 147ff14ec72be84cff55a4e05b3d32e2d727da95 0 1725596405000 3 connected
426acff8f0d8eecf66f8979ba2fbe0038d020713 10.88.3.62:6379@16379 slave c205ed8fd181f95b61d11525effdc478864c91d8 0 1725596404000 0 connected
52af4b5e914aa0e780694dc5831adb6b05bffd43 10.88.3.58:6379@16379 myself,slave 42eecdb2638230925c8ce268c2f16f35edb20d4a 0 1725596406000 2 connected`

	nodes, err := ParseRedisClusterNodes(data)
	if err != nil {
		fmt.Println("Error parsing Redis cluster nodes:", err)
		return
	}

	// 输出解析结果
	for _, node := range nodes {
		fmt.Printf("%+v\n", node)
	}
}
