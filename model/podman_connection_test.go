package model

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCreatePodmanConnectionUsesExistingEndpoint(t *testing.T) {
	prevEndpoint := PodmanEndpoint
	prevIdentity := PodmanIdentity
	prevMachine := PodmanMachine
	prevInit := initializeRuntimeConfig
	prevCreate := createPodmanBindingsConnection
	t.Cleanup(func() {
		PodmanEndpoint = prevEndpoint
		PodmanIdentity = prevIdentity
		PodmanMachine = prevMachine
		initializeRuntimeConfig = prevInit
		createPodmanBindingsConnection = prevCreate
	})

	PodmanEndpoint = "ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock"
	PodmanIdentity = "C:\\Users\\niuzq\\.local\\share\\containers\\podman\\machine\\machine"
	PodmanMachine = true

	initCalled := false
	initializeRuntimeConfig = func() error {
		initCalled = true
		return nil
	}

	createCalled := false
	createPodmanBindingsConnection = func(ctx context.Context, uri, identity string, machine bool) (context.Context, error) {
		createCalled = true
		if uri != PodmanEndpoint {
			t.Fatalf("uri = %q, want %q", uri, PodmanEndpoint)
		}
		if identity != PodmanIdentity {
			t.Fatalf("identity = %q, want %q", identity, PodmanIdentity)
		}
		if machine != PodmanMachine {
			t.Fatalf("machine = %v, want %v", machine, PodmanMachine)
		}
		return context.WithValue(ctx, "podman-uri", uri), nil
	}

	ctx, err := CreatePodmanConnection(context.Background())
	if err != nil {
		t.Fatalf("CreatePodmanConnection returned error: %v", err)
	}
	if initCalled {
		t.Fatal("InitConfig should not be called when endpoint already exists")
	}
	if !createCalled {
		t.Fatal("bindings.NewConnection should be called")
	}
	if got := ctx.Value("podman-uri"); got != PodmanEndpoint {
		t.Fatalf("connection context value = %v, want %q", got, PodmanEndpoint)
	}
}

func TestCreatePodmanConnectionInitializesConfigWhenEndpointMissing(t *testing.T) {
	prevEndpoint := PodmanEndpoint
	prevIdentity := PodmanIdentity
	prevMachine := PodmanMachine
	prevInit := initializeRuntimeConfig
	prevCreate := createPodmanBindingsConnection
	t.Cleanup(func() {
		PodmanEndpoint = prevEndpoint
		PodmanIdentity = prevIdentity
		PodmanMachine = prevMachine
		initializeRuntimeConfig = prevInit
		createPodmanBindingsConnection = prevCreate
	})

	PodmanEndpoint = ""

	initCalled := false
	initializeRuntimeConfig = func() error {
		initCalled = true
		PodmanEndpoint = "ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock"
		PodmanIdentity = "C:\\Users\\niuzq\\.local\\share\\containers\\podman\\machine\\machine"
		PodmanMachine = true
		return nil
	}

	createPodmanBindingsConnection = func(ctx context.Context, uri, identity string, machine bool) (context.Context, error) {
		if uri != PodmanEndpoint {
			t.Fatalf("uri = %q, want %q", uri, PodmanEndpoint)
		}
		if identity != PodmanIdentity {
			t.Fatalf("identity = %q, want %q", identity, PodmanIdentity)
		}
		if machine != PodmanMachine {
			t.Fatalf("machine = %v, want %v", machine, PodmanMachine)
		}
		return ctx, nil
	}

	_, err := CreatePodmanConnection(context.Background())
	if err != nil {
		t.Fatalf("CreatePodmanConnection returned error: %v", err)
	}
	if !initCalled {
		t.Fatal("InitConfig should be called when endpoint is empty")
	}
}

func TestCreatePodmanConnectionReturnsInitError(t *testing.T) {
	prevEndpoint := PodmanEndpoint
	prevIdentity := PodmanIdentity
	prevMachine := PodmanMachine
	prevInit := initializeRuntimeConfig
	prevCreate := createPodmanBindingsConnection
	t.Cleanup(func() {
		PodmanEndpoint = prevEndpoint
		PodmanIdentity = prevIdentity
		PodmanMachine = prevMachine
		initializeRuntimeConfig = prevInit
		createPodmanBindingsConnection = prevCreate
	})

	PodmanEndpoint = ""
	initializeRuntimeConfig = func() error {
		return errors.New("boom")
	}
	createPodmanBindingsConnection = func(ctx context.Context, uri, identity string, machine bool) (context.Context, error) {
		t.Fatal("bindings.NewConnection should not be called when init fails")
		return ctx, nil
	}

	_, err := CreatePodmanConnection(context.Background())
	if err == nil {
		t.Fatal("expected init error")
	}
	if !strings.Contains(err.Error(), "init config fail:boom") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreatePodmanConnectionReturnsBindingError(t *testing.T) {
	prevEndpoint := PodmanEndpoint
	prevIdentity := PodmanIdentity
	prevMachine := PodmanMachine
	prevInit := initializeRuntimeConfig
	prevCreate := createPodmanBindingsConnection
	t.Cleanup(func() {
		PodmanEndpoint = prevEndpoint
		PodmanIdentity = prevIdentity
		PodmanMachine = prevMachine
		initializeRuntimeConfig = prevInit
		createPodmanBindingsConnection = prevCreate
	})

	PodmanEndpoint = "ssh://user@127.0.0.1:9280/run/user/1000/podman/podman.sock"
	PodmanIdentity = "C:\\Users\\niuzq\\.local\\share\\containers\\podman\\machine\\machine"
	PodmanMachine = true
	initializeRuntimeConfig = func() error {
		t.Fatal("InitConfig should not be called when endpoint already exists")
		return nil
	}
	createPodmanBindingsConnection = func(ctx context.Context, uri, identity string, machine bool) (context.Context, error) {
		return ctx, errors.New("cannot connect")
	}

	_, err := CreatePodmanConnection(context.Background())
	if err == nil {
		t.Fatal("expected connection error")
	}
	if !strings.Contains(err.Error(), "create podman conection fail:cannot connect") {
		t.Fatalf("unexpected error: %v", err)
	}
}
