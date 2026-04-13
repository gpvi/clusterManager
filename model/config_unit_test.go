package model

import "testing"

func TestParseDefaultPodmanConnectionURI(t *testing.T) {
	data := []byte(`[
		{"Name":"podman-machine-default-root","URI":"ssh://root@127.0.0.1:9280/run/podman/podman.sock","Default":false},
		{"Name":"podman-machine-default","URI":"ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock","Default":true}
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
