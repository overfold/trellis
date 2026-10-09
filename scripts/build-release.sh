#!/usr/bin/env bash
# Build the exact Linux amd64 archive published by CI. Output must be outside source.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${VERSION:?Set VERSION to the release tag}"
output="${1:?Usage: build-release.sh OUTPUT_DIRECTORY}"
mkdir -p "$output"
output="$(realpath "$output")"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v1
for binary in trellis trellisctl trellis-health-probe; do
    go -C "$root" build -trimpath -buildvcs=false \
        -ldflags "-buildid= -X github.com/overfold/trellis/orchestrator/internal/version.version=${VERSION}" \
        -o "$tmp/$binary" "./orchestrator/cmd/$binary"
done
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -cf - -C "$tmp" trellis trellisctl trellis-health-probe | gzip -n >"$output/trellis_linux_x64.tar.gz"
sha256sum "$output/trellis_linux_x64.tar.gz" | sed 's|  .*|  trellis_linux_x64.tar.gz|' >"$output/trellis_linux_x64.tar.gz.sha256"
printf 'version=%s\nos=linux\narch=amd64\ngoamd64=v1\ncgo=0\ngo=%s\n' \
    "$VERSION" "$(go version | awk '{print $3}')" >"$output/trellis_linux_x64.build-info"
