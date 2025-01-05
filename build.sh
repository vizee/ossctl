#!/bin/bash

set -e

export GOPROXY=https://mirrors.aliyun.com/goproxy/
export CGO_ENABLED=0
go build -v -o ossctl
tar cvzf ./ossctl_$(go env GOOS)_$(go env GOARCH).tar.gz ./ossctl
