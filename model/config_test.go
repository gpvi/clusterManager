//go:build integration
// +build integration

package model

import (
	"testing"
)

func TestConfig(t *testing.T) {
	cfg, err := InitConfig()
	if err != nil {
		t.Errorf("read config error: %v", err)
	}
	cfg.PrintConfig()
}
