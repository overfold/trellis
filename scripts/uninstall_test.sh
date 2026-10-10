#!/usr/bin/env bash
# Exercise uninstall against temporary paths and mocked host/cluster commands.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/scripts" "$tmp/mocks"
go build -o "$tmp/config-decoder" "$script_dir/../orchestrator/cmd/trellis"
export CONFIG_DECODER="$tmp/config-decoder"
cp "$script_dir/uninstall.sh" "$script_dir/common.sh" "$tmp/scripts/"
# Allow the same isolated tests to run without root or a supported host OS.
printf '\nrequire_root_linux_amd64() { :; }\n' >>"$tmp/scripts/common.sh"
cat >"$tmp/mocks/dpkg-query" <<'MOCK'
#!/usr/bin/env bash
test -f "$PACKAGES/${*: -1}" || exit 1
printf 'install ok installed\n'
MOCK
cat >"$tmp/mocks/apt-get" <<'MOCK'
#!/usr/bin/env bash
printf 'dependencies %s\n' "$*" >>"$CALL_LOG"
[ "$SCENARIO" != dependency-archive ] && [ "$SCENARIO" != dependency-purge ] || exit 1
[ "$1" = remove ] || exit 1
shift 3
for package in "$@"; do /bin/rm "$PACKAGES/$package" || exit 1; done
touch "$PACKAGE_REMOVED"
MOCK
cat >"$tmp/mocks/systemctl" <<'MOCK'
#!/usr/bin/env bash
printf 'systemctl %s\n' "$*" >>"$CALL_LOG"
[ "$SCENARIO" != stop-failure ] || [ "$*" != 'stop trellis' ] || exit 1
if [ "$*" = 'is-active --quiet trellis' ]; then test ! -e "${PACKAGE_REMOVED}.stopped"; exit $?; fi
[ "$*" != 'stop trellis' ] || touch "${PACKAGE_REMOVED}.stopped"
exit 0
MOCK
cat >"$tmp/mocks/ctr" <<'MOCK'
#!/usr/bin/env bash
printf 'ctr %s\n' "$*" >>"$CALL_LOG"
if [ "${1:-}" = --address ]; then
    address="$2"; shift 2
else
    address=/run/containerd/containerd.sock
fi
# The reachable default daemon is empty; only the configured daemon owns work.
if [ "$address" != "$EXPECTED_SOCKET" ]; then exit 0; fi
case "$*" in
    '-n trellis tasks ls -q')
        [ "$SCENARIO" != task-inspect-failure ] && [ "$SCENARIO" != graceful-query-failure ] || { echo 'socket unavailable' >&2; exit 1; }
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
        touch "${CTR_LISTED}.container-deleted"
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
# The real-CLI/TLS regressions separately verify these pinned flags.
shift 8
printf 'trellisctl %s\n' "$*" >>"$CALL_LOG"
if [ "$*" = 'nodes list --output json' ]; then
    case "$SCENARIO" in
        graceful|graceful-custom|graceful-query-failure|graceful-leader*) printf '[\n{"id":"test-node","control_plane":"voter"},\n{"id":"other-node"}\n]\n' ;;
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
if [ "$*" = 'nodes leader --output json' ]; then
    case "$SCENARIO" in
        graceful-leader) printf '{"leader_id":"test-node"}\n' ;;
        graceful-leader-error) exit 1 ;;
        graceful-leader-invalid) printf '{"leader_id":null}\n' ;;
        graceful-leader-zero) printf '{"leader_id":"00000000-0000-0000-0000-000000000000"}\n' ;;
        *) printf '{"leader_id":"other-node"}\n' ;;
    esac
fi
[ "$*" != 'nodes transfer-leadership' ] || { echo 'uninstall must not transfer leadership' >&2; exit 1; }
[ "$*" != 'nodes drain test-node' ] || touch "${CTR_LISTED}.drained"
if [ "$SCENARIO" = graceful-leader-race ] && [ "$*" = 'nodes remove test-node' ]; then
    echo 'target is the current leader; transfer leadership first' >&2
    exit 1
fi
[ "$*" != 'nodes remove test-node' ] || touch "${CTR_LISTED}.member-removed"
MOCK
cat >"$tmp/mocks/trellis" <<'MOCK'
#!/usr/bin/env bash
if [ "${1:-}" = config-paths ]; then exec "$CONFIG_DECODER" "$@"; fi
printf 'trellis %s\n' "$*" >>"$CALL_LOG"
[ "$SCENARIO" != cleanup-failure ] || exit 1
if [ "${1:-}" = local-cleanup ]; then
    [ "$2" = --data-dir ] || exit 1
    /bin/rm -f "$3/journal"
fi
MOCK
cat >"$tmp/mocks/rm" <<'MOCK'
#!/usr/bin/env bash
printf 'rm %s\n' "$*" >>"$CALL_LOG"
if [ "$SCENARIO" = purge-failure ] && [ "$*" = "-rf $DATA_DIR" ]; then
    /bin/rm -f "$DATA_DIR/state"
    echo 'Device or resource busy' >&2
    exit 1
fi
[ "$SCENARIO" != purge-config-failure ] || [ "$*" != "-rf $CONFIG_DIR" ] || exit 1
[ "$SCENARIO" != purge-recovery-failure ] || [ "$*" != "-rf $STATE_ROOT/recovery" ] || exit 1
exec /bin/rm "$@"
MOCK
cat >"$tmp/mocks/cp" <<'MOCK'
#!/usr/bin/env bash
if { [ "$SCENARIO" = archive-failure ] && [ "${2:-}" = "$DATA_DIR" ]; } ||
   { [ "$SCENARIO" = archive-key-failure ] && [ "${2:-}" = "$SECRETS_KEY_FILE" ]; }; then
    echo 'archive copy failed' >&2
    exit 1
fi
exec /bin/cp "$@"
MOCK
chmod +x "$tmp/mocks/"*

for scenario in ${UNINSTALL_SCENARIOS:-membership-failure malformed wrong-shape empty cleanup-failure stop-failure task-inspect-failure task-delete-failure container-delete-failure inspect-failure verify-failure containers-remain purge-failure purge-config-failure purge-recovery-failure archive-failure archive-key-failure taskless force force-purge graceful graceful-compact single-node single-label single-pretty custom-archive custom-purge quoted-archive quoted-purge alias-archive alias-purge graceful-custom graceful-query-failure graceful-leader graceful-leader-error graceful-leader-invalid graceful-leader-zero graceful-leader-race invalid-config missing-config missing-id empty-id dependency-archive dependency-purge}; do
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
        export EXPECTED_SOCKET=/run/containerd/containerd.sock PACKAGE_REMOVED="$root/package-removed"
        export PACKAGES="$root/packages"
        mkdir -p "$PACKAGES"
        for package in containerd.io wireguard-tools runsc iproute2 iptables; do printf 'installed\n' >"$PACKAGES/$package"; done
        case "$scenario" in
            custom-*|quoted-*|alias-*|graceful-custom|graceful-query-failure|purge-recovery-failure)
                export DATA_DIR="$root/actual data" SECRETS_KEY_FILE="$root/external key"
                [[ "$scenario" = quoted-* ]] || export EXPECTED_SOCKET="$root/active.sock"
                ;;
        esac
        mkdir -p "$INSTALL_DIR" "$DATA_DIR" "$CONFIG_DIR" "$RUN_DIR" "$STATE_ROOT/recovery/previous"
        cp "$tmp/mocks/trellisctl" "$INSTALL_DIR/trellisctl"
        cp "$tmp/mocks/trellis" "$INSTALL_DIR/trellis"
        touch "$INSTALL_DIR/trellis-health-probe" "$SERVICE_FILE"
        printf 'test-node\n' >"$DATA_DIR/node-id"
        printf 'durable-state\n' >"$DATA_DIR/state"
        touch "$DATA_DIR/journal"
        printf 'data_dir: %s\nsecrets_key: %s\n' "$DATA_DIR" "$SECRETS_KEY_FILE" >"$CONFIG_FILE"
        case "$scenario" in
            custom-*|graceful-custom|graceful-query-failure|purge-recovery-failure) printf 'containerd_socket: "%s" # active daemon\n' "$EXPECTED_SOCKET" >>"$CONFIG_FILE" ;;
            quoted-*) printf 'data_dir: "%s" # configured path\nsecrets_key: "%s"\n' "$DATA_DIR" "$SECRETS_KEY_FILE" >"$CONFIG_FILE" ;;
            alias-*) printf "ca_cert: &data '%s' # spaced path\napi_key: &key '%s'\nagent_listen: &socket '%s'\ndata_dir: *data\nsecrets_key: *key\ncontainerd_socket: *socket\n" "$DATA_DIR" "$SECRETS_KEY_FILE" "$EXPECTED_SOCKET" >"$CONFIG_FILE" ;;
            invalid-config) printf 'data_dir: [\n' >"$CONFIG_FILE" ;;
            missing-id) /bin/rm "$DATA_DIR/node-id" ;;
            empty-id) printf ' \n' >"$DATA_DIR/node-id" ;;
        esac
        cp "$CONFIG_FILE" "$root/original-config"
        printf 'test-secret-key\n' >"$SECRETS_KEY_FILE"
        printf 'complete=true\ncontainerd_owned=true\n' >"$STATE_FILE"
        [[ "$scenario" != dependency-* ]] || printf 'wireguard_owned=true\n' >>"$STATE_FILE"
        export TRELLIS_CONFIG="$root/operator.yaml" TRELLIS_ADMINISTRATOR_KEY=test-key
        touch "$TRELLIS_CONFIG"
        args=(--yes)
        if [ "$scenario" = missing-config ]; then
            /bin/rm "$CONFIG_FILE"
            touch "${PACKAGE_REMOVED}.stopped"
            args+=(--force)
        fi
        case "$scenario" in
            force|taskless) args+=(--force) ;;
            force-purge) args+=(--force --purge) ;;
            archive-failure|archive-key-failure) args+=(--force) ;;
            custom-*|quoted-*|alias-*|dependency-*) args+=(--force); [[ "$scenario" != *-purge ]] || args+=(--purge) ;;
        esac
        if [[ "$scenario" = invalid-config || "$scenario" = missing-config || "$scenario" = missing-id || "$scenario" = empty-id ]]; then
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then
                echo "Accepted $scenario before deletion" >&2; exit 1
            fi
            for retained in "$DATA_DIR/state" "$SECRETS_KEY_FILE" "$STATE_FILE"; do test -f "$retained"; done
            ! grep -q 'systemctl stop\|ctr \|trellis local-cleanup\|dependencies' "$CALL_LOG" 2>/dev/null
        elif [[ "$scenario" = dependency-* ]]; then
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then
                echo 'Expected package failure' >&2; exit 1
            fi
            for retained in "$STATE_FILE" "$CONFIG_FILE" "$DATA_DIR/state"; do test -f "$retained"; done
            test -x "$INSTALL_DIR/trellis"
            test ! -e "$PACKAGE_REMOVED"
            for package in containerd.io wireguard-tools runsc iproute2 iptables; do grep -qx installed "$PACKAGES/$package"; done
            export SCENARIO=force
            bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/retry-output" 2>&1
            test -f "$PACKAGE_REMOVED"
            test ! -e "$STATE_FILE"
            test ! -e "$INSTALL_DIR/trellis"
            [ "$(grep -c '^dependencies remove -y -qq wireguard-tools containerd.io$' "$CALL_LOG")" = 2 ]
            test ! -e "$PACKAGES/containerd.io"
            test ! -e "$PACKAGES/wireguard-tools"
            for shared in runsc iproute2 iptables; do grep -qx installed "$PACKAGES/$shared"; done
            ! grep -q 'dependencies.*runsc\|dependencies.*iproute\|dependencies.*iptables' "$CALL_LOG"
            if [[ "$scenario" = *-purge ]]; then test ! -e "$STATE_ROOT"; else grep -qx durable-state "$STATE_ROOT"/recovery/*/data/state; fi
        elif [[ "$scenario" = graceful-leader* ]]; then
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then echo 'Unsafe leader preflight/removal accepted' >&2; exit 1; fi
            for retained in "$DATA_DIR/state" "$SECRETS_KEY_FILE" "$STATE_FILE" "$CONFIG_FILE" "$INSTALL_DIR/trellis"; do test -f "$retained"; done
            test ! -e "${CTR_LISTED}.member-removed"
            ! grep -q 'systemctl stop\|trellis local-cleanup\|dependencies\|nodes transfer-leadership' "$CALL_LOG"
            if [ "$scenario" = graceful-leader-race ]; then
                test -e "${CTR_LISTED}.drained"
                grep -q 'node remains drained' "$root/output"
            else
                test ! -e "${CTR_LISTED}.drained"
                if [ "$scenario" = graceful-leader ]; then grep -q 'Transfer leadership' "$root/output"; fi
            fi
        elif [ "$scenario" = graceful-query-failure ]; then
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then echo 'Failed evacuation accepted' >&2; exit 1; fi
            for retained in "$DATA_DIR/state" "$SECRETS_KEY_FILE" "$STATE_FILE" "$CONFIG_FILE" "$INSTALL_DIR/trellis"; do test -f "$retained"; done
            test ! -e "${CTR_LISTED}.member-removed"
            ! grep -q 'systemctl stop\|trellis local-cleanup\|dependencies' "$CALL_LOG"
            grep -q 'nodes undrain test-node' "$CALL_LOG"
        elif [ "$scenario" = membership-failure ] || [[ "$scenario" = malformed || "$scenario" = wrong-shape || "$scenario" = empty ]]; then
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then
                echo 'Expected membership inspection failure' >&2; exit 1
            fi
            if [ "$scenario" = membership-failure ]; then
                grep -q -- 'rerun with --force' "$root/output"
            else
                grep -q 'Invalid cluster membership output' "$root/output"
                ! grep -q 'trellisctl nodes drain\|trellisctl nodes remove' "$CALL_LOG"
            fi
            for retained in "$SERVICE_FILE" "$INSTALL_DIR/trellisctl" "$DATA_DIR/state" "$SECRETS_KEY_FILE" "$STATE_FILE"; do test -f "$retained"; done
            ! grep -q 'systemctl stop\|ctr ' "$CALL_LOG"
        elif [[ "$scenario" = purge*-failure || "$scenario" = archive*-failure ]]; then
            if [[ "$scenario" = purge* ]]; then args+=(--force --purge); fi
            if bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1; then
                echo "Expected $scenario" >&2; exit 1
            fi
            test -f "$SERVICE_FILE"
            test -x "$INSTALL_DIR/trellis"
            ! grep -q 'systemctl disable trellis' "$CALL_LOG"
            test -f "$STATE_FILE"
            if [ "$scenario" = purge-failure ]; then
                test -f "$CONFIG_FILE"
                test -f "$SECRETS_KEY_FILE"
                grep -q 'Data may be partially deleted' "$root/output"
                export SCENARIO=force-purge
            elif [[ "$scenario" = purge* ]]; then
                if [ "$scenario" = purge-recovery-failure ]; then cmp "$root/original-config" "$CONFIG_FILE"; fi
                export SCENARIO=force-purge
            else
                for retained in "$DATA_DIR/state" "$CONFIG_FILE" "$SECRETS_KEY_FILE"; do test -f "$retained"; done
                grep -q 'archive copy failed' "$root/output"
                export SCENARIO=force
            fi
            # The exact same uninstall command must work after fixing the cause.
            bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/retry-output" 2>&1
            if [ "$scenario" = purge-recovery-failure ]; then
                ! grep '^ctr ' "$CALL_LOG" | grep -vF "ctr --address $EXPECTED_SOCKET "
            fi
            test ! -e "$SERVICE_FILE"
            test ! -e "$INSTALL_DIR/trellis"
            if [[ "$scenario" = purge* ]]; then
                test ! -e "$STATE_ROOT"
            else
                grep -qx durable-state "$STATE_ROOT"/recovery/*/data/state
                grep -qx test-secret-key "$STATE_ROOT"/recovery/*/secrets.key
                cmp "$root/original-config" "$STATE_ROOT"/recovery/*/config/trellis.yaml
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
            for retained in "$SERVICE_FILE" "$DATA_DIR/state" "$SECRETS_KEY_FILE" "$STATE_FILE"; do test -f "$retained"; done
            test -x "$INSTALL_DIR/trellis"
            ! grep -q 'systemctl disable trellis' "$CALL_LOG"
        else
            bash "$tmp/scripts/uninstall.sh" "${args[@]}" >"$root/output" 2>&1
            test -f "${CTR_LISTED}.container-deleted"
            test ! -e "$DATA_DIR/journal"
            for removed in "$SERVICE_FILE" "$INSTALL_DIR/trellisctl" "$INSTALL_DIR/trellis" "$INSTALL_DIR/trellis-health-probe" "$RUN_DIR" "$CONFIG_DIR" "$DATA_DIR" "$SECRETS_KEY_FILE"; do test ! -e "$removed"; done
            grep -q 'systemctl stop trellis' "$CALL_LOG"
            if [ "$scenario" = taskless ] || [[ "$scenario" = graceful* ]]; then
                ! grep -q 'ctr .*tasks delete' "$CALL_LOG"
            else
                grep -q 'ctr .*tasks delete --force test-container' "$CALL_LOG"
            fi
            grep -q "trellis local-cleanup --data-dir $DATA_DIR --config $CONFIG_FILE" "$CALL_LOG"
            cleanup_line="$(grep -n 'trellis local-cleanup' "$CALL_LOG" | cut -d: -f1)"
            remove_line="$(grep -n 'ctr .*containers rm test-container' "$CALL_LOG" | cut -d: -f1)"
            test "$cleanup_line" -gt "$remove_line"
            data_line="$(grep -n "rm -rf $CONFIG_DIR\|rm -rf $DATA_DIR $CONFIG_DIR" "$CALL_LOG" | head -1 | cut -d: -f1)"
            dependencies_line="$(grep -n '^dependencies' "$CALL_LOG" | cut -d: -f1)"
            test "$dependencies_line" -lt "$data_line"
            test "$dependencies_line" -gt "$cleanup_line"
            if [[ "$scenario" = graceful* ]]; then
                ! grep -q 'nodes transfer-leadership' "$CALL_LOG"
                test -e "${CTR_LISTED}.member-removed"
                grep -q 'trellisctl nodes drain test-node' "$CALL_LOG"
                grep -q 'trellisctl nodes remove test-node' "$CALL_LOG"
            elif [[ "$scenario" = single* ]]; then
                ! grep -q 'trellisctl nodes drain\|trellisctl nodes remove\|trellisctl nodes leader\|trellisctl nodes transfer-leadership' "$CALL_LOG"
                grep -q 'Single-node cluster' "$root/output"
            else
                ! grep -q trellisctl "$CALL_LOG"
                grep -q 'Cluster membership is unchanged' "$root/output"
                grep -q 'remove node test-node' "$root/output"
            fi
            if [ "$scenario" = force-purge ] || [[ "$scenario" = *-purge ]]; then
                test ! -e "$STATE_ROOT"
            else
                archives=("$STATE_ROOT"/recovery/*/data)
                test "${#archives[@]}" -eq 1
                archive="${archives[0]%/data}"
                grep -qx durable-state "$archive/data/state"
                test ! -e "$archive/data/journal"
                grep -qx test-secret-key "$archive/secrets.key"
                cmp "$root/original-config" "$archive/config/trellis.yaml"
                grep -qx complete=true "$archive/install-state"
                test -d "$STATE_ROOT/recovery/previous"
            fi
        fi
        printf 'PASS uninstall: %s\n' "$scenario"
    )
done
