#!/usr/bin/env bash
# Exercise uninstall against temporary paths and mocked host/cluster commands.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/scripts" "$tmp/mocks"
cp "$script_dir/uninstall.sh" "$script_dir/common.sh" "$tmp/scripts/"
# Allow the same isolated tests to run without root or a supported host OS.
printf '\nrequire_root_linux_amd64() { :; }\n' >>"$tmp/scripts/common.sh"
cat >"$tmp/mocks/systemctl" <<'MOCK'
#!/usr/bin/env bash
printf 'systemctl %s\n' "$*" >>"$CALL_LOG"
exit 0
MOCK
cat >"$tmp/mocks/ctr" <<'MOCK'
#!/usr/bin/env bash
printf 'ctr %s\n' "$*" >>"$CALL_LOG"
if [ "$*" = '-n trellis containers ls -q' ]; then printf 'test-container\n'; fi
MOCK
cat >"$tmp/mocks/trellisctl" <<'MOCK'
#!/usr/bin/env bash
printf 'trellisctl %s\n' "$*" >>"$CALL_LOG"
if [ "$*" = 'nodes list --output json' ]; then
    [ "$SCENARIO" = graceful ] || exit 1
    printf '[\n{"id":"test-node"},\n{"id":"other-node"}\n]\n'
fi
MOCK
chmod +x "$tmp/mocks/"*

for scenario in membership-failure force force-purge graceful; do
    (
        export SCENARIO="$scenario" CALL_LOG="$tmp/$scenario.calls"
        export PATH="$tmp/mocks:$PATH"
        root="$tmp/$scenario"
        export INSTALL_DIR="$root/bin" STATE_ROOT="$root/state"
        export DATA_DIR="$STATE_ROOT/data" STATE_FILE="$STATE_ROOT/install-state"
        export CONFIG_DIR="$root/config"
        export CONFIG_FILE="$CONFIG_DIR/trellis.yaml"
        export SECRETS_KEY_FILE="$CONFIG_DIR/secrets.key" SERVICE_FILE="$root/trellis.service"
        export RUN_DIR="$root/run"
        mkdir -p "$INSTALL_DIR" "$DATA_DIR" "$CONFIG_DIR" "$RUN_DIR" "$STATE_ROOT/recovery/previous"
        cp "$tmp/mocks/trellisctl" "$INSTALL_DIR/trellisctl"
        touch "$INSTALL_DIR/trellis" "$INSTALL_DIR/trellis-health-probe" "$SERVICE_FILE"
        printf 'test-node\n' >"$DATA_DIR/node-id"
        printf 'durable-state\n' >"$DATA_DIR/state"
        printf 'test-config\n' >"$CONFIG_FILE"
        printf 'test-secret-key\n' >"$SECRETS_KEY_FILE"
        printf 'complete=true\n' >"$STATE_FILE"
        args=(--yes)
        case "$scenario" in
            force) args+=(--force) ;;
            force-purge) args+=(--force --purge) ;;
        esac
        if [ "$scenario" = membership-failure ]; then
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then
                echo 'Expected membership inspection failure' >&2; exit 1
            fi
            grep -q -- 'rerun with --force' "$root/output"
            test -f "$SERVICE_FILE" && test -f "$INSTALL_DIR/trellisctl"
            test -f "$DATA_DIR/state" && test -f "$SECRETS_KEY_FILE"
            ! grep -q 'systemctl stop\|ctr ' "$CALL_LOG"
        else
            bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1
            test ! -e "$SERVICE_FILE" && test ! -e "$INSTALL_DIR/trellisctl"
            test ! -e "$INSTALL_DIR/trellis" && test ! -e "$INSTALL_DIR/trellis-health-probe"
            test ! -e "$RUN_DIR" && test ! -e "$CONFIG_DIR" && test ! -e "$DATA_DIR"
            grep -q 'systemctl stop trellis' "$CALL_LOG"
            grep -q 'ctr -n trellis tasks kill test-container -s SIGKILL' "$CALL_LOG"
            if [ "$scenario" = graceful ]; then
                grep -q 'trellisctl nodes drain test-node' "$CALL_LOG"
                grep -q 'trellisctl nodes remove test-node' "$CALL_LOG"
            else
                ! grep -q trellisctl "$CALL_LOG"
                grep -q 'Cluster membership is unchanged' "$root/output"
                grep -q 'remove node test-node' "$root/output"
            fi
            if [ "$scenario" = force-purge ]; then
                test ! -e "$STATE_ROOT"
            else
                archives=("$STATE_ROOT"/recovery/*/data)
                test "${#archives[@]}" -eq 1
                archive="${archives[0]%/data}"
                grep -qx durable-state "$archive/data/state"
                grep -qx test-secret-key "$archive/secrets.key"
                grep -qx test-config "$archive/config/trellis.yaml"
                grep -qx complete=true "$archive/install-state"
                test -d "$STATE_ROOT/recovery/previous"
            fi
        fi
        printf 'PASS uninstall: %s\n' "$scenario"
    )
done
