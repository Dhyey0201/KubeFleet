#!/usr/bin/env bash
# Source from project scripts.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PATH="$ROOT/.tools/go/bin:$ROOT/.tools:$PATH"
export GOPATH="$ROOT/.cache/gopath" GOCACHE="$ROOT/.cache/go-build"
export KUBECONFIG="${KUBEFLEET_KUBECONFIG:-$ROOT/.tools/kubefleet.kubeconfig}"
export GOTOOLCHAIN=local

export GOLANGCI_LINT_CACHE="$ROOT/.cache/lint"
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=2}"
