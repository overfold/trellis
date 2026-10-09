#!/usr/bin/env bash
# Check the actual release archive, not a separately compiled test binary.
set -euo pipefail
archive="$(realpath "${1:?Usage: release-platform_test.sh ARCHIVE VERSION [IMAGE ...]}")"
version="${2:?Expected release version}"
shift 2
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
tar -xzf "$archive" -C "$tmp"
command -v readelf >/dev/null
for binary in trellis trellisctl trellis-health-probe; do
    headers="$(readelf -l "$tmp/$binary")"
    dynamic="$(readelf -d "$tmp/$binary")"
    if grep -q INTERP <<<"$headers" || grep -q NEEDED <<<"$dynamic"; then
        echo "Release binary $binary requires a dynamic loader/library" >&2; exit 1
    fi
done
check='set -eu
[ "$(/release/trellis --version | awk "{print \$NF}")" = "$1" ]
[ "$(/release/trellisctl version)" = "$1" ]
printf "data_dir: /tmp/trellis-data\n" >/tmp/trellis-config.yaml
/release/trellis config-paths --config /tmp/trellis-config.yaml | grep -q /tmp/trellis-data
rc=0; /release/trellis-health-probe || rc=$?
[ "$rc" = 64 ]'
if [ "$#" = 0 ]; then
    # Native execution without a container engine.
    [ "$("$tmp/trellis" --version | awk '{print $NF}')" = "$version" ]
    [ "$("$tmp/trellisctl" version)" = "$version" ]
    rc=0; "$tmp/trellis-health-probe" || rc=$?; [ "$rc" = 64 ]
    echo 'PASS static release native execution'
else
    for image in "$@"; do
        docker run --rm --network none -v "$tmp:/release:ro" "$image" sh -c "$check" sh "$version"
        printf 'PASS release execution: %s\n' "$image"
    done
fi
