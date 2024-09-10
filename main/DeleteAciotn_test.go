package main

import "testing"

func TestDeleteAllAction(t *testing.T) {
	
	ctxPodman := CreatePodmanConnection()
	DeleteAllContainers(ctxPodman)
}
