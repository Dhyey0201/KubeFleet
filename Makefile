SHELL := /bin/bash
.SHELLFLAGS := -euo pipefail -c
.PHONY: help doctor bootstrap cluster-up compatibility generate manifests fmt vet lint build test test-unit race envtest test-integration test-webhook e2e images worker-linux install-development dev-up dev-down smoke demo monitoring benchmark-smoke benchmark-full check-generated verify
.PHONY: benchmark-smoke-suite test-scripts render-check validate-manifests
help:
	@echo 'bootstrap doctor cluster-up install-development monitoring build fmt vet lint test race envtest e2e smoke demo benchmark-smoke benchmark-full verify'
doctor:
	bash scripts/doctor.sh
bootstrap:
	bash scripts/bootstrap.sh
cluster-up:
	bash scripts/cluster-up.sh
compatibility:
	bash scripts/compatibility.sh
generate manifests:
	bash scripts/generate.sh
fmt:
	source scripts/env.sh; gofmt -w api cmd internal test
vet:
	source scripts/env.sh; go vet ./...
lint:
	source scripts/env.sh; golangci-lint run
build:
	source scripts/env.sh; mkdir -p bin; for name in manager kubefleet worker loadgen collector; do go build -trimpath -o bin/$$name ./cmd/$$name; done
test test-unit:
	source scripts/env.sh; go test ./...
race:
	source scripts/env.sh; go test -race ./...
envtest test-integration test-webhook:
	bash scripts/envtest.sh
e2e:
	bash scripts/e2e.sh
worker-linux:
	source scripts/env.sh; mkdir -p bin; CGO_ENABLED=0 GOOS=linux go build -trimpath -o bin/worker-linux ./cmd/worker
images: worker-linux
	source scripts/env.sh; docker build --provenance=false -f Dockerfile.worker -t kubefleet-worker:compat .
install-development:
	bash scripts/install-development.sh
dev-up:
	bash scripts/dev-up.sh
monitoring:
	bash scripts/monitoring.sh
dev-down:
	bash scripts/cluster-down.sh
smoke:
	bash scripts/smoke.sh
demo:
	bash scripts/demo.sh
benchmark-smoke:
	bash scripts/benchmark.sh smoke
benchmark-smoke-suite:
	for mix in balanced asymmetric arrival mixed priority; do bash scripts/benchmark.sh smoke $$mix; done
benchmark-full:
	for mix in balanced asymmetric arrival mixed priority; do bash scripts/benchmark.sh full $$mix; done
check-generated:
	bash scripts/check-generated.sh
verify: build test race vet lint check-generated test-scripts render-check envtest
test-scripts:
	bash scripts/test-guard.sh

render-check:
	bash scripts/render-check.sh
validate-manifests:
	bash scripts/server-render-check.sh
