#!/bin/bash

TAG=$(npm pkg get version | sed 's/"//g')

docker build --build-arg TAG="$TAG" -t ghcr.io/biron-bi/target-clickhouse:"$TAG" .

#Require login
#https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry
docker push ghcr.io/biron-bi/target-clickhouse:"$TAG"

docker build --build-arg TAG="$TAG" -t ghcr.io/biron-bi/target-clickhouse:latest .

docker push ghcr.io/biron-bi/target-clickhouse:latest
