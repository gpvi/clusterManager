package model

import (
	"path"
	"testing"
)

func TestResolveContainerHostPathKeepsUnixStyleAbsolutePath(t *testing.T) {
	root := `E:\Projects\clusterManager\clusterManager-main`
	got := resolveContainerHostPath(root, "/mnt/e/Projects/clusterManager/clusterManager-main/setup/redis/config")
	want := "/mnt/e/Projects/clusterManager/clusterManager-main/setup/redis/config"
	if got != want {
		t.Fatalf("resolved path = %q, want %q", got, want)
	}
}

func TestResolveContainerHostPathResolvesRelativeWindowsPath(t *testing.T) {
	root := `E:\Projects\clusterManager\clusterManager-main`
	got := resolveContainerHostPath(root, "setup/redis/config")
	want := `E:\Projects\clusterManager\clusterManager-main\setup\redis\config`
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
	RedisConfigPath = "/data/redis/config"
	got := path.Join(RedisConfigPath, "redis.conf")
	want := "/data/redis/config/redis.conf"
	if got != want {
		t.Fatalf("container config path = %q, want %q", got, want)
	}
}

func TestParseDefaultPodmanConnectionURI(t *testing.T) {
	data := []byte(`[
		{"Name":"podman-machine-default-root","URI":"ssh://root@127.0.0.1:9280/run/podman/podman.sock","Identity":"C:\\Users\\niuzq\\.local\\share\\containers\\podman\\machine\\machine","IsMachine":true,"Default":false},
		{"Name":"podman-machine-default","URI":"ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock","Identity":"C:\\Users\\niuzq\\.local\\share\\containers\\podman\\machine\\machine","IsMachine":true,"Default":true}
	]`)

	got, err := parseDefaultPodmanConnectionURI(data)
	if err != nil {
		t.Fatalf("parseDefaultPodmanConnectionURI returned error: %v", err)
	}
	want := "ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock"
	if got != want {
		t.Fatalf("default URI = %q, want %q", got, want)
	}
}

func TestParseDefaultPodmanConnection(t *testing.T) {
	PodmanIdentity = ""
	PodmanMachine = false

	data := []byte(`[
		{"Name":"podman-machine-default","URI":"ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock","Identity":"C:\\Users\\niuzq\\.local\\share\\containers\\podman\\machine\\machine","IsMachine":true,"Default":true}
	]`)

	got, err := parseDefaultPodmanConnection(data)
	if err != nil {
		t.Fatalf("parseDefaultPodmanConnection returned error: %v", err)
	}
	if got.Identity == "" {
		t.Fatal("expected identity to be parsed")
	}
	if !got.IsMachine {
		t.Fatal("expected machine flag to be true")
	}
	if PodmanIdentity != got.Identity {
		t.Fatalf("PodmanIdentity = %q, want %q", PodmanIdentity, got.Identity)
	}
	if !PodmanMachine {
		t.Fatal("PodmanMachine should be true")
	}
}

func TestParseDefaultPodmanConnectionURINoDefault(t *testing.T) {
	data := []byte(`[
		{"Name":"podman-machine-default-root","URI":"ssh://root@127.0.0.1:9280/run/podman/podman.sock","Default":false}
	]`)

	got, err := parseDefaultPodmanConnectionURI(data)
	if err != nil {
		t.Fatalf("parseDefaultPodmanConnectionURI returned error: %v", err)
	}
	if got != "" {
		t.Fatalf("default URI = %q, want empty string", got)
	}
}

func TestDefaultPodmanEndpointUsesConnectionList(t *testing.T) {
	t.Setenv("CONTAINER_HOST", "")

	prev := runPodmanConnectionList
	runPodmanConnectionList = func() ([]byte, error) {
		return []byte(`[{"URI":"ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock","Default":true}]`), nil
	}
	t.Cleanup(func() {
		runPodmanConnectionList = prev
	})

	got := defaultPodmanEndpoint()
	want := "ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock"
	if got != want {
		t.Fatalf("default endpoint = %q, want %q", got, want)
	}
}

func TestDefaultPodmanEndpointPrefersContainerHostEnv(t *testing.T) {
	t.Setenv("CONTAINER_HOST", "ssh://env@example/run/podman/podman.sock")

	prev := runPodmanConnectionList
	runPodmanConnectionList = func() ([]byte, error) {
		return []byte(`[{"URI":"ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock","Default":true}]`), nil
	}
	t.Cleanup(func() {
		runPodmanConnectionList = prev
	})

	got := defaultPodmanEndpoint()
	want := "ssh://env@example/run/podman/podman.sock"
	if got != want {
		t.Fatalf("default endpoint = %q, want %q", got, want)
	}
}
