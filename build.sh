#!/usr/bin/env bash

GOOS=${GOOS:-linux} GOARCH=${GOARCH:-amd64} CGO_ENABLED=0 go build -ldflags="-s -w" -o mysqlbak .
