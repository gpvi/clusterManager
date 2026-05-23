$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path ".gocache" | Out-Null
$env:GOCACHE = (Resolve-Path ".gocache").Path

$testConfigDir = Join-Path (Get-Location) ".testcontainers"
$testTmpDir = Join-Path $testConfigDir "tmp"
New-Item -ItemType Directory -Force -Path $testTmpDir | Out-Null
$containersConfPath = Join-Path $testConfigDir "containers.conf"
$containersConfContent = @"
[engine]
tmp_dir = "$($testTmpDir -replace '\\','/')"
"@
Set-Content -Path $containersConfPath -Value $containersConfContent -NoNewline
$env:CONTAINERS_CONF = $containersConfPath

go test -tags containers_image_openpgp ./...
