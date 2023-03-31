#!/bin/bash

docker build --build-arg TAG="$(npm pkg get version | sed 's/"//g')" -t ghcr.io/biron-bi/target-clickhouse:"$(npm pkg get version | sed 's/"//g')" .

docker push ghcr.io/biron-bi/target-clickhouse:"$(npm pkg get version | sed 's/"//g')"

docker build --build-arg TAG="$(npm pkg get version | sed 's/"//g')" -t ghcr.io/biron-bi/target-clickhouse:latest .

docker push ghcr.io/biron-bi/target-clickhouse:latest
