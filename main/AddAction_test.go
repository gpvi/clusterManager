package main

import (
	"context"
	"testing"
)

func TestAddAction(t *testing.T) {
	ctx := context.Background()
	err := AddAction(ctx, 1, 2)
	if err != nil {
		t.Errorf("AddAction() error = %v", err)
	}
}
