$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path ".gocache" | Out-Null
$env:GOCACHE = (Resolve-Path ".gocache").Path

go build -tags containers_image_openpgp -o cluster .
