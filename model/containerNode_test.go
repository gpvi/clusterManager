package model

import (
	"context"
	"fmt"
	"github.com/containers/podman/v5/pkg/bindings"
	"os"
	"testing"
)

func CreateConnection() context.Context {
	conn, err := bindings.NewConnection(context.Background(), "unix:///Users/zhuoqun.niu/.local/share/containers/podman/machine/podman.sock")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)

	}
	return conn
}

func TestContainer(t *testing.T) {

}
