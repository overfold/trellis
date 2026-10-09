#!/usr/bin/env bash
# Real integrity helpers and full entrypoints; no host mutation or network access.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        printf 'FAIL release test: %s / %s\n' "${scenario:-setup}" "${entrypoint:-helpers}" >&2
        [ ! -f "$tmp/output" ] || cat "$tmp/output" >&2
    fi
    rm -rf "$tmp"
}
trap cleanup EXIT
source "$script_dir/common.sh"
mkdir -p "$tmp/payload"
export EXECUTION_LOG="$tmp/executed" MUTATION_LOG="$tmp/mutated"
printf '#!/bin/sh\necho executed >>"$EXECUTION_LOG"\necho "trellis v-test"\n' >"$tmp/payload/trellis"
cp "$tmp/payload/trellis" "$tmp/payload/trellisctl"
cp "$tmp/payload/trellis" "$tmp/payload/trellis-health-probe"
chmod +x "$tmp/payload/"*
tar -czf "$tmp/valid.tar.gz" -C "$tmp/payload" .
digest="$(sha256sum "$tmp/valid.tar.gz")"; digest="${digest%% *}"
# A substituted binary still reports the requested version, but has other bytes.
echo '# substituted' >>"$tmp/payload/trellis"
tar -czf "$tmp/substituted.tar.gz" -C "$tmp/payload" .
curl() {
    if [[ " $* " == *' -o '* ]]; then
        [ "${*: -1}" = https://example.test/linux-x64 ]
        local dest="" previous="" arg
        for arg in "$@"; do
            if [ "$previous" = -o ]; then dest="$arg"; fi
            previous="$arg"
        done
        cp "$ARCHIVE" "$dest"
    else
        cat "$METADATA"
    fi
}
export -f curl
for scenario in valid uppercase substituted missing null-digest malformed mismatch wrong-platform duplicate malformed-json http; do
    export METADATA="$tmp/$scenario.json" ARCHIVE="$tmp/valid.tar.gz"
    value="sha256:$digest"; name=trellis_linux_x64.tar.gz; url=https://example.test/linux-x64
    case "$scenario" in
        uppercase) value="sha256:${digest^^}" ;;
        substituted) ARCHIVE="$tmp/substituted.tar.gz" ;;
        missing) value="" ;;
        malformed) value=sha256:123 ;;
        mismatch) value="sha256:$(printf '%064d' 0)" ;;
        wrong-platform) name=trellis_darwin_arm64.tar.gz ;;
        http) url=http://example.test/linux-x64 ;;
    esac
    jq -n --arg digest "$value" --arg name "$name" --arg url "$url" '
        {tag_name:"v-test",assets:[
        {name:"trellis_linux_arm64.tar.gz",browser_download_url:"https://example.test/wrong",digest:$digest},
        {name:$name,browser_download_url:$url,digest:$digest}]}' >"$METADATA"
    if [ "$scenario" = duplicate ]; then
        jq '.assets += [.assets[1]]' "$METADATA" >"$tmp/duplicate"; mv "$tmp/duplicate" "$METADATA"
    fi
    if [ "$scenario" = null-digest ]; then
        jq 'del(.assets[1].digest)' "$METADATA" >"$tmp/no-digest"; mv "$tmp/no-digest" "$METADATA"
    fi
    [ "$scenario" != malformed-json ] || echo '{' >"$METADATA"
    mkdir "$tmp/stage-$scenario"
    rm -f "$EXECUTION_LOG"
    if (fetch_latest_release; download_release "$tmp/stage-$scenario") >"$tmp/output" 2>&1; then
        [[ "$scenario" = valid || "$scenario" = uppercase ]] || { cat "$tmp/output"; exit 1; }
        grep -qx executed "$EXECUTION_LOG"
        test -x "$tmp/stage-$scenario/trellis-health-probe"
    else
        [[ "$scenario" != valid && "$scenario" != uppercase ]] || { cat "$tmp/output"; exit 1; }
        test ! -e "$EXECUTION_LOG"
        test ! -e "$tmp/stage-$scenario/trellis"
        case "$scenario" in substituted|mismatch) grep -q 'SHA-256 mismatch' "$tmp/output" ;; esac
    fi
    printf 'PASS release integrity: %s\n' "$scenario"
done

for invalid in '' sha256:bad; do
    rm -f "$EXECUTION_LOG"
    mkdir "$tmp/direct-${invalid:-missing}"
    if (RELEASE_DIGEST="$invalid"; download_release "$tmp/direct-${invalid:-missing}") >"$tmp/output" 2>&1; then
        echo 'Download accepted invalid digest' >&2; exit 1
    fi
    grep -q 'Missing or malformed SHA-256' "$tmp/output"
    test ! -e "$EXECUTION_LOG"
    test ! -e "$tmp/direct-${invalid:-missing}/trellis_linux_x64.tar.gz"
done
printf 'PASS download independently rejects missing/malformed digest before fetching\n'

# Exercise both entrypoints with real download verification and a substituted
# same-version artifact. State writes, host service calls, and binaries must stay untouched.
export METADATA="$tmp/valid.json" ARCHIVE="$tmp/substituted.tar.gz"
for entrypoint in install-core upgrade; do
    root="$tmp/$entrypoint"
    mkdir -p "$root/scripts" "$root/bin" "$root/etc"
    cp "$script_dir/$entrypoint.sh" "$root/scripts/"
    printf 'source %q\nrequire_root_linux_amd64() { :; }\nrequire_commands() { :; }\n' "$script_dir/common.sh" >"$root/scripts/common.sh"
    printf '#!/bin/sh\necho "trellis v-old"\n' >"$root/bin/trellis"
    cp "$root/bin/trellis" "$root/bin/trellisctl"
    chmod +x "$root/bin/"*
    touch "$root/etc/trellis.yaml"
    systemctl() { echo "$*" >>"$MUTATION_LOG"; }
    export -f systemctl
    rm -f "$EXECUTION_LOG" "$MUTATION_LOG"
    args=(); [ "$entrypoint" != install-core ] || args=(--yes --advertise 192.0.2.10)
    if INSTALL_DIR="$root/bin" CONFIG_DIR="$root/etc" STATE_ROOT="$root/state" \
        SERVICE_FILE="$root/service" bash "$root/scripts/$entrypoint.sh" "${args[@]}" >"$tmp/output" 2>&1; then
        echo "Accepted substituted release in $entrypoint" >&2; exit 1
    fi
    grep -q 'SHA-256 mismatch' "$tmp/output"
    test ! -e "$EXECUTION_LOG"; test ! -e "$MUTATION_LOG"; test ! -e "$root/state"
    grep -q v-old "$root/bin/trellis"
    printf 'PASS stage before mutation: %s\n' "$entrypoint"
done

# Real executable inode: atomically replacing the path must not fool the running
# version check. Scripts cannot model /proc/PID/exe, which points to their shell.
cat >"$tmp/daemon.go" <<'GO'
package main
import ("fmt"; "os"; "time")
var version string
func main() {
    if len(os.Args) > 1 { fmt.Println("trellis", version); return }
    _ = os.WriteFile(os.Getenv("READY_FILE"), []byte("ready"), 0600)
    for { time.Sleep(time.Second) }
}
GO
for version in old new; do
    CGO_ENABLED=0 go build -ldflags "-X main.version=v-$version" -o "$tmp/daemon-$version" "$tmp/daemon.go"
done
python3 - "$tmp" "$script_dir/common.sh" <<'PY'
import os, pathlib, shutil, subprocess, sys, time
root = pathlib.Path(sys.argv[1])
binary = root / 'daemon'
shutil.copy(root / 'daemon-old', binary)
env = dict(os.environ, READY_FILE=str(root / 'ready'))
p = subprocess.Popen([str(binary)], env=env)
try:
    for _ in range(100):
        if (root / 'ready').exists(): break
        time.sleep(.02)
    else: raise AssertionError('daemon failed to start')
    os.replace(root / 'daemon-new', binary)
    assert subprocess.check_output([str(binary), '--version'], text=True).strip() == 'trellis v-new'
    script = 'set -euo pipefail; source "$1"; daemon_pid="$2"; systemctl() { echo "$daemon_pid"; }; RELEASE_TAG=v-new; ! verify_running_version; RELEASE_TAG=v-old; verify_running_version'
    subprocess.run(['bash', '-c', script, 'sh', sys.argv[2], str(p.pid)], check=True)
finally:
    p.terminate()
    p.wait(timeout=5)
print('PASS running version uses retained executable inode, not replaced disk binary')
PY
