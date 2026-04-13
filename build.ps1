$ErrorActionPreference = "Stop"

go build -tags containers_image_openpgp -o cluster .
