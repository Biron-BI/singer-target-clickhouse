#!/bin/bash

set -o errexit
set -o pipefail

TAG=$(npm pkg get version | sed 's/"//g')

# requires a previous call to `npm publish`
docker build --build-arg TAG="$TAG" -t ghcr.io/biron-bi/target-clickhouse:"$TAG" .

# requires login
# https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic
# minimal scope for PAT is "write:packages"
docker push ghcr.io/biron-bi/target-clickhouse:"$TAG"

docker build --build-arg TAG="$TAG" -t ghcr.io/biron-bi/target-clickhouse:latest .

docker push ghcr.io/biron-bi/target-clickhouse:latest
