#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$ROOT"
platform=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$(uname -m)" in arm64|aarch64) arch=arm64 ;; x86_64) arch=amd64 ;; *) echo 'Unsupported architecture' >&2; exit 1 ;; esac
case "$platform" in darwin|linux) ;; *) echo 'Unsupported operating system' >&2; exit 1 ;; esac
mkdir -p .tools
check_hash() {
  python3 - "$1" "$2" "$3" <<'PY'
import hashlib, sys
archive, sidecar, algorithm = sys.argv[1:]
expected=open(sidecar).read().split()[0]
h=hashlib.new(algorithm)
with open(archive,'rb') as f:
    for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
if h.hexdigest()!=expected:raise SystemExit('Checksum mismatch: '+archive)
print('Verified '+archive)
PY
}
if [[ ! -x .tools/go/bin/go ]] || [[ "$(.tools/go/bin/go version)" != *go1.26.8* ]]; then
  curl -fsSL 'https://go.dev/dl/?mode=json&include=all' -o .tools/go-releases.json
  python3 - "$platform" "$arch" <<'PY'
import json,sys
for release in json.load(open('.tools/go-releases.json')):
    if release['version']=='go1.26.8':
        for f in release['files']:
            if f['os']==sys.argv[1] and f['arch']==sys.argv[2] and f['kind']=='archive':
                open('.tools/go.sha256','w').write(f['sha256']);break
        else:raise SystemExit('Go archive missing')
        break
else:raise SystemExit('Pinned Go release unavailable')
PY
  curl -fsSL "https://go.dev/dl/go1.26.8.$platform-$arch.tar.gz" -o .tools/go.tar.gz
  check_hash .tools/go.tar.gz .tools/go.sha256 sha256
  tar -xzf .tools/go.tar.gz -C .tools
fi
for tool in kind kubectl; do
  if [[ "$tool" == kind ]]; then
    url="https://github.com/kubernetes-sigs/kind/releases/download/v0.29.0/kind-$platform-$arch"
    checksum_url="$url.sha256sum"
  else
    url="https://dl.k8s.io/release/v1.34.2/bin/$platform/$arch/kubectl"
    checksum_url="$url.sha256"
  fi
  curl -fsSL "$url" -o ".tools/$tool.download"
  curl -fsSL "$checksum_url" -o ".tools/$tool.sha256"
  check_hash ".tools/$tool.download" ".tools/$tool.sha256" sha256
  mv ".tools/$tool.download" ".tools/$tool"
  chmod +x ".tools/$tool"
done
base='https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-v1.33.0'
curl -fsSL "$base/envtest-v1.33.0-$platform-$arch.tar.gz" -o .tools/envtest.tar.gz
curl -fsSL "$base/envtest-v1.33.0-$platform-$arch.tar.gz.sha512" -o .tools/envtest.sha512
check_hash .tools/envtest.tar.gz .tools/envtest.sha512 sha512
tar -xzf .tools/envtest.tar.gz -C .tools
GOBIN="$ROOT/.tools" go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.18.0
go mod download
if [[ ! -x .tools/golangci-lint ]] || [[ "$(.tools/golangci-lint version)" != *'version 2.4.0'* ]]; then
  python3 - "$ROOT" <<'PYSPACE'
import shutil,sys
if shutil.disk_usage(sys.argv[1]).free < 8*1024**3:
    raise SystemExit('Pinned linter source build requires >=8 GiB free disk.')
PYSPACE
  GOBIN="$ROOT/.tools" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.4.0
fi
