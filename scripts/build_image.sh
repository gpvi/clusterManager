#!/bin/bash
# Build custom Redis image for clusterManager.
# Uses the Dockerfile in configs/ directory.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

podman build -t myredis -f "$PROJECT_ROOT/configs/Dockerfile" "$PROJECT_ROOT"
