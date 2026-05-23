//go:build integration
// +build integration

package action

import (
	"os"
	"testing"
)

func requireIntegrationTest(t *testing.T) {
	t.Helper()
	if os.Getenv("CLUSTER_RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("set CLUSTER_RUN_INTEGRATION_TESTS=1 to run K8s integration tests")
	}
}
