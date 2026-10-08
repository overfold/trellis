#!/usr/bin/env bash
# Security boundaries only; never change host services, packages, or config.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/hostile" "$tmp/bin"
for helper in common.sh install-core.sh; do
    printf 'echo HOSTILE_HELPER >&2; exit 79\n' >"$tmp/hostile/$helper"
done
cat >"$tmp/bin/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
[[ " $* " == *" --proto =https "* && " $* " == *" --proto-redir =https "* ]]
target=""
while [ "$#" -gt 0 ]; do
    if [ "$1" = -o ]; then target="$2"; shift 2; else shift; fi
done
[ -n "$target" ]
printf 'echo TRUSTED_REMOTE_HELPER; exit 0\n' >"$target"
MOCK
chmod +x "$tmp/bin/curl"
for entrypoint in install install-core upgrade uninstall; do
    # Deliberately pipe into bash from a directory with attacker-controlled helpers.
    output="$(cat "$script_dir/$entrypoint.sh" | (cd "$tmp/hostile"; PATH="$tmp/bin:$PATH" bash) 2>&1)"
    grep -qx TRUSTED_REMOTE_HELPER <<<"$output"
    ! grep -q HOSTILE_HELPER <<<"$output"
    printf 'PASS piped %s ignores current-directory helpers and enforces HTTPS\n' "$entrypoint"
done

mkdir -p "$tmp/bundle/gvisor-bin"
for name in runsc containerd-shim-runsc-v1; do
    cat >"$tmp/bundle/$name" <<'RUNTIME'
#!/usr/bin/env bash
if [ "${1:-}" = install ] || [ "${1:-}" = uninstall ]; then
    printf 'clobbered\n' >"$DOCKER_FIXTURE"
    exit 79
fi
RUNTIME
    chmod +x "$tmp/bundle/$name"
done
printf 'sidecar\n' >"$tmp/bundle/gvisor-bin/sentry"
tar -cjf "$tmp/valid.tar.bz2" -C "$tmp/bundle" .
tar -cjf "$tmp/missing-sidecars.tar.bz2" -C "$tmp/bundle" runsc containerd-shim-runsc-v1
for mode in valid mismatch malformed missing-sidecars incomplete unrelated; do
    (
        source "$script_dir/common.sh"
        STATE_ROOT="$tmp/$mode/state"; STATE_FILE="$STATE_ROOT/install-state"
        INSTALL_DIR="$tmp/$mode/bin"; WORK_TMP="$tmp/$mode/work"
        export PATH="$INSTALL_DIR:$PATH" DOCKER_FIXTURE="$tmp/$mode/docker.json"
        mkdir -p "$INSTALL_DIR" "$WORK_TMP"
        load_install_state
        printf '{"runtimes":{"other":{}},"log-driver":"local"}\n' >"$DOCKER_FIXTURE"
        cp "$DOCKER_FIXTURE" "$tmp/$mode/docker-before.json"
        # Ignore any real host runtime; this fixture owns only its custom paths.
        command() {
            if [ "${1:-}" = -v ] && { [ "${2:-}" = runsc ] || [ "${2:-}" = containerd-shim-runsc-v1 ]; }; then
                [ -x "$INSTALL_DIR/$2" ]
            else builtin command "$@"; fi
        }
        curl() {
            local target="" url="" archive="$tmp/valid.tar.bz2"
            [[ " $* " == *" --proto =https "* && " $* " == *" --proto-redir =https "* ]]
            while [ "$#" -gt 0 ]; do
                case "$1" in -o) target="$2"; shift 2 ;; https:*) url="$1"; shift ;; *) shift ;; esac
            done
            [ "$mode" != missing-sidecars ] || archive="$tmp/missing-sidecars.tar.bz2"
            if [[ "$url" = *.sha512 ]]; then
                if [ "$mode" = malformed ]; then printf 'bad digest\n' >"$target"
                else printf '%s  gvisor.tar.bz2\n' "$(sha512sum "$archive" | cut -d' ' -f1)" >"$target"; fi
            else
                cp "$archive" "$target"
                [ "$mode" != mismatch ] || printf 'tampered' >>"$target"
            fi
        }
        apt-get() { echo 'Unexpected package maintainer hooks' >&2; exit 1; }
        systemctl() { echo 'Unexpected Docker/containerd reconfiguration' >&2; exit 1; }
        if [ "$mode" = incomplete ]; then cp "$tmp/bundle/runsc" "$INSTALL_DIR/runsc"; fi
        if [ "$mode" = unrelated ]; then mkdir "$INSTALL_DIR/trellis-gvisor"; printf 'unrelated\n' >"$INSTALL_DIR/trellis-gvisor/keep"; fi
        if [ "$mode" != valid ]; then
            if (install_gvisor) >"$tmp/$mode/output" 2>&1; then echo "Accepted invalid bundle/installation: $mode" >&2; exit 1; fi
            [ "$GVISOR_BUNDLE_OWNED" = false ]
            [ ! -e "$INSTALL_DIR/containerd-shim-runsc-v1" ]
            if [ "$mode" = incomplete ]; then cmp "$tmp/bundle/runsc" "$INSTALL_DIR/runsc"; fi
            if [ "$mode" = unrelated ]; then grep -qx unrelated "$INSTALL_DIR/trellis-gvisor/keep"; fi
        else
            install_gvisor
            [ "$GVISOR_ENABLED" = true ] && [ "$GVISOR_BUNDLE_OWNED" = true ]
            grep -qx gvisor_bundle_owned=true "$STATE_FILE"
            [ -f "$INSTALL_DIR/trellis-gvisor/gvisor-bin/sentry" ]
            # Resume a crash between publishing the bundle and its second link.
            rm "$INSTALL_DIR/containerd-shim-runsc-v1"
            install_gvisor
            [ -x "$INSTALL_DIR/containerd-shim-runsc-v1" ]
            # Removal honors exact owned links, not unrelated replacement files.
            rm "$INSTALL_DIR/runsc"
            printf 'unrelated replacement\n' >"$INSTALL_DIR/runsc"
            GVISOR_CONFIG_OWNED=true
            remove_owned_dependencies
            grep -qx 'unrelated replacement' "$INSTALL_DIR/runsc"
            [ ! -e "$INSTALL_DIR/containerd-shim-runsc-v1" ] && [ ! -e "$INSTALL_DIR/trellis-gvisor" ]
        fi
        cmp "$DOCKER_FIXTURE" "$tmp/$mode/docker-before.json"
        printf 'PASS Docker-neutral verified gVisor bundle: %s\n' "$mode"
    )
done
