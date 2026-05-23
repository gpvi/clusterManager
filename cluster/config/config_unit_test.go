package config

import (
	"path"
	"testing"
)

func TestResolveContainerHostPathKeepsUnixStyleAbsolutePath(t *testing.T) {
	root := `E:\Projects\clusterManager\clusterManager-main`
	got := resolveContainerHostPath(root, "/mnt/e/Projects/clusterManager/clusterManager-main/configs")
	want := "/mnt/e/Projects/clusterManager/clusterManager-main/configs"
	if got != want {
		t.Fatalf("resolved path = %q, want %q", got, want)
	}
}

func TestResolveContainerHostPathResolvesRelativeWindowsPath(t *testing.T) {
	root := `E:\Projects\clusterManager\clusterManager-main`
	got := resolveContainerHostPath(root, "configs")
	want := `E:\Projects\clusterManager\clusterManager-main\configs`
	if got != want {
		t.Fatalf("resolved path = %q, want %q", got, want)
	}
}

func TestResolveLocalAbsPathResolvesRelativePath(t *testing.T) {
	root := `E:\Projects\clusterManager\clusterManager-main`
	got := resolveLocalAbsPath(root, "runtime")
	want := `E:\Projects\clusterManager\clusterManager-main\runtime`
	if got != want {
		t.Fatalf("resolved path = %q, want %q", got, want)
	}
}

func TestContainerConfigPathUsesUnixSeparators(t *testing.T) {
	cfg := &RuntimeConfig{RedisConfigPath: "/data/redis/config"}
	got := path.Join(cfg.RedisConfigPath, "redis.conf")
	want := "/data/redis/config/redis.conf"
	if got != want {
		t.Fatalf("container config path = %q, want %q", got, want)
	}
}
