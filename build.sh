#!/usr/bin/env bash

go build -tags netgo -ldflags '-extldflags "-static"'
