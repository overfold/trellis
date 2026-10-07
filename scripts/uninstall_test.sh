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
printf '\nremove_owned_dependencies() { echo dependencies >>"$CALL_LOG"; }\n' >>"$tmp/scripts/common.sh"
cat >"$tmp/mocks/systemctl" <<'MOCK'
#!/usr/bin/env bash
printf 'systemctl %s\n' "$*" >>"$CALL_LOG"
[ "$SCENARIO" != stop-failure ] || [ "$*" != 'stop trellis' ] || exit 1
exit 0
MOCK
cat >"$tmp/mocks/ctr" <<'MOCK'
#!/usr/bin/env bash
printf 'ctr %s\n' "$*" >>"$CALL_LOG"
case "$*" in
    '-n trellis tasks ls -q')
        [ "$SCENARIO" != task-inspect-failure ] || { echo 'socket unavailable' >&2; exit 1; }
        if [ "$SCENARIO" != taskless ] && [[ "$SCENARIO" != graceful* ]]; then printf 'test-container\n'; fi
        ;;
    '-n trellis tasks delete --force test-container')
        [ "$SCENARIO" != task-delete-failure ] || { echo 'shim did not respond' >&2; exit 1; }
        touch "${CTR_LISTED}.task-deleted"
        ;;
    '-n trellis containers rm test-container')
        # An active task must be synchronously removed before its container.
        [ "$SCENARIO" = taskless ] || [[ "$SCENARIO" = graceful* ]] || test -e "${CTR_LISTED}.task-deleted" || exit 1
        [ "$SCENARIO" != container-delete-failure ] || { echo 'snapshot is busy' >&2; exit 1; }
        ;;
    *'tasks kill'*|*'tasks delete'*) echo 'unsafe task cleanup' >&2; exit 1 ;;
esac
if [ "$*" = '-n trellis containers ls -q' ]; then
    [ "$SCENARIO" != inspect-failure ] || exit 1
    if [ ! -e "$CTR_LISTED" ] || [ "$SCENARIO" = containers-remain ]; then
        touch "$CTR_LISTED"
        printf 'test-container\n'
    elif [ "$SCENARIO" = verify-failure ]; then
        exit 1
    fi
fi
MOCK
cat >"$tmp/mocks/trellisctl" <<'MOCK'
#!/usr/bin/env bash
printf 'trellisctl %s\n' "$*" >>"$CALL_LOG"
if [ "$*" = 'nodes list --output json' ]; then
    case "$SCENARIO" in
        graceful) printf '[\n{"id":"test-node"},\n{"id":"other-node"}\n]\n' ;;
        graceful-compact) printf '[{"id":"test-node"},{"id":"other-node"}]\n' ;;
        single-node) printf '[{"id":"test-node"}]\n' ;;
        single-label) printf '[\n{"id":"test-node",\n"labels":{"id":"rack-a"}}\n]\n' ;;
        single-pretty) printf '[\n{"id":"test-node"}\n]\n' ;;
        malformed) printf '[{"id":' ;;
        wrong-shape) printf '{"id":"test-node"}\n' ;;
        empty) printf '[]\n' ;;
        *) exit 1 ;;
    esac
fi
MOCK
cat >"$tmp/mocks/trellis" <<'MOCK'
#!/usr/bin/env bash
printf 'trellis %s\n' "$*" >>"$CALL_LOG"
[ "$SCENARIO" != cleanup-failure ]
MOCK
cat >"$tmp/mocks/rm" <<'MOCK'
#!/usr/bin/env bash
printf 'rm %s\n' "$*" >>"$CALL_LOG"
if [ "$SCENARIO" = purge-failure ] && [ "$*" = "-rf $DATA_DIR" ]; then
    /bin/rm -f "$DATA_DIR/state"
    echo 'Device or resource busy' >&2
    exit 1
fi
exec /bin/rm "$@"
MOCK
cat >"$tmp/mocks/mv" <<'MOCK'
#!/usr/bin/env bash
if [ "$SCENARIO" = archive-failure ] && [ "${1:-}" = "$DATA_DIR" ]; then
    echo 'archive move failed' >&2
    exit 1
fi
exec /bin/mv "$@"
MOCK
chmod +x "$tmp/mocks/"*

for scenario in membership-failure malformed wrong-shape empty cleanup-failure stop-failure task-inspect-failure task-delete-failure container-delete-failure inspect-failure verify-failure containers-remain purge-failure archive-failure taskless force force-purge graceful graceful-compact single-node single-label single-pretty; do
    (
        export SCENARIO="$scenario" CALL_LOG="$tmp/$scenario.calls" CTR_LISTED="$tmp/$scenario.ctr-listed"
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
        cp "$tmp/mocks/trellis" "$INSTALL_DIR/trellis"
        touch "$INSTALL_DIR/trellis-health-probe" "$SERVICE_FILE"
        printf 'test-node\n' >"$DATA_DIR/node-id"
        printf 'durable-state\n' >"$DATA_DIR/state"
        printf 'test-config\n' >"$CONFIG_FILE"
        printf 'test-secret-key\n' >"$SECRETS_KEY_FILE"
        printf 'complete=true\n' >"$STATE_FILE"
        args=(--yes)
        case "$scenario" in
            force|taskless) args+=(--force) ;;
            force-purge) args+=(--force --purge) ;;
            archive-failure) args+=(--force) ;;
        esac
        if [ "$scenario" = membership-failure ] || [[ "$scenario" = malformed || "$scenario" = wrong-shape || "$scenario" = empty ]]; then
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then
                echo 'Expected membership inspection failure' >&2; exit 1
            fi
            if [ "$scenario" = membership-failure ]; then
                grep -q -- 'rerun with --force' "$root/output"
            else
                grep -q 'Invalid cluster membership output' "$root/output"
                ! grep -q 'trellisctl nodes drain\|trellisctl nodes remove' "$CALL_LOG"
            fi
            test -f "$SERVICE_FILE" && test -f "$INSTALL_DIR/trellisctl"
            test -f "$DATA_DIR/state" && test -f "$SECRETS_KEY_FILE"
            ! grep -q 'systemctl stop\|ctr ' "$CALL_LOG"
        elif [ "$scenario" = purge-failure ] || [ "$scenario" = archive-failure ]; then
            if [ "$scenario" = purge-failure ]; then args+=(--force --purge); fi
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then
                echo "Expected $scenario" >&2; exit 1
            fi
            test -f "$SERVICE_FILE" && test -x "$INSTALL_DIR/trellis"
            test -f "$CONFIG_FILE" && test -f "$SECRETS_KEY_FILE" && test -f "$STATE_FILE"
            ! grep -q 'systemctl disable trellis\|dependencies' "$CALL_LOG"
            if [ "$scenario" = purge-failure ]; then
                grep -q 'Data may be partially deleted' "$root/output"
                export SCENARIO=force-purge
            else
                grep -q 'archive move failed' "$root/output"
                export SCENARIO=force
            fi
            # The exact same uninstall command must work after fixing the cause.
            bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/retry-output" 2>&1
            test ! -e "$SERVICE_FILE" && test ! -e "$INSTALL_DIR/trellis"
            if [ "$scenario" = purge-failure ]; then
                test ! -e "$STATE_ROOT"
            else
                grep -qx durable-state "$STATE_ROOT"/recovery/*/data/state
            fi
        elif [[ "$scenario" = *-failure || "$scenario" = containers-remain ]]; then
            if bash "$tmp/scripts/uninstall.sh" --yes --force --purge >"$root/output" 2>&1; then
                echo "Expected $scenario" >&2; exit 1
            fi
            if [ "$scenario" = cleanup-failure ]; then
                grep -q 'trellis local-cleanup --data-dir' "$CALL_LOG"
            else
                ! grep -q 'trellis local-cleanup' "$CALL_LOG"
            fi
            if [ "$scenario" = task-delete-failure ]; then
                grep -q 'shim did not respond' "$root/output"
                grep -q 'Could not stop and delete Trellis task test-container' "$root/output"
            elif [ "$scenario" = container-delete-failure ]; then
                grep -q 'snapshot is busy' "$root/output"
                grep -q 'Could not remove Trellis container test-container' "$root/output"
            fi
            test -f "$SERVICE_FILE" && test -x "$INSTALL_DIR/trellis"
            test -f "$DATA_DIR/state" && test -f "$SECRETS_KEY_FILE" && test -f "$STATE_FILE"
            ! grep -q 'systemctl disable trellis' "$CALL_LOG"
        else
            bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1
            test ! -e "$SERVICE_FILE" && test ! -e "$INSTALL_DIR/trellisctl"
            test ! -e "$INSTALL_DIR/trellis" && test ! -e "$INSTALL_DIR/trellis-health-probe"
            test ! -e "$RUN_DIR" && test ! -e "$CONFIG_DIR" && test ! -e "$DATA_DIR"
            grep -q 'systemctl stop trellis' "$CALL_LOG"
            if [ "$scenario" = taskless ] || [[ "$scenario" = graceful* ]]; then
                ! grep -q 'ctr -n trellis tasks delete' "$CALL_LOG"
            else
                grep -q 'ctr -n trellis tasks delete --force test-container' "$CALL_LOG"
            fi
            grep -q "trellis local-cleanup --data-dir $DATA_DIR --config $CONFIG_FILE" "$CALL_LOG"
            cleanup_line="$(grep -n 'trellis local-cleanup' "$CALL_LOG" | cut -d: -f1)"
            remove_line="$(grep -n 'ctr -n trellis containers rm test-container' "$CALL_LOG" | cut -d: -f1)"
            test "$cleanup_line" -gt "$remove_line"
            data_line="$(grep -n "rm -rf $CONFIG_DIR\|rm -rf $STATE_ROOT $CONFIG_DIR" "$CALL_LOG" | head -1 | cut -d: -f1)"
            dependencies_line="$(grep -n '^dependencies' "$CALL_LOG" | cut -d: -f1)"
            test "$dependencies_line" -gt "$data_line"
            test "$data_line" -gt "$cleanup_line"
            if [[ "$scenario" = graceful* ]]; then
                grep -q 'trellisctl nodes drain test-node' "$CALL_LOG"
                grep -q 'trellisctl nodes remove test-node' "$CALL_LOG"
            elif [[ "$scenario" = single* ]]; then
                ! grep -q 'trellisctl nodes drain\|trellisctl nodes remove' "$CALL_LOG"
                grep -q 'Single-node cluster' "$root/output"
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
