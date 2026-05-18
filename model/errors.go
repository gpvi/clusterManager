package model

import "errors"

var (
	ErrClusterNotFound  = errors.New("cluster not found")
	ErrClusterExists    = errors.New("cluster already exists")
	ErrNoNodesAvailable = errors.New("no nodes available")
)
