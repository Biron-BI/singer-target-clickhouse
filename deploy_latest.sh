#!/bin/bash

set -o errexit
set -o pipefail

KOTLIN_IMAGE="ghcr.io/biron-bi/target-clickhouse:kotlin"

docker build -t "$KOTLIN_IMAGE" .

# requires login
# https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic
# minimal scope for PAT is "write:packages"
docker push "$KOTLIN_IMAGE"
