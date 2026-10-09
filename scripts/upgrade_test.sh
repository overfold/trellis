#!/usr/bin/env bash
# Run the full upgrade flow with mocked releases, CLI, and host services.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
source "$script_dir/common.sh"
for invalid in '' 'null' '{}' '[{}]' '[{"id":0}]' '[{"id":""}]' '[{"id":"node-a"}] []'; do
    if printf '%s' "$invalid" | count_nodes_json >"$tmp/count" 2>/dev/null; then
        echo "Accepted invalid membership: $invalid" >&2; exit 1
    fi
done
printf 'PASS structural membership shape validation\n'
go build -o "$tmp/trellis" "$script_dir/../orchestrator/cmd/trellis"
export CONFIG_DECODER="$tmp/trellis"
# Exercise the real evacuation helper, including failure with empty stdout.
(
    mkdir -p "$tmp/ctr-bin"
    cat >"$tmp/ctr-bin/ctr" <<'CTR'
#!/usr/bin/env bash
[ "$*" = '--address /tmp/custom.sock -n trellis tasks ls -q' ] || exit 90
[ "$CTR_RESULT" = empty ] || exit 1
CTR
    chmod +x "$tmp/ctr-bin/ctr"
    export PATH="$tmp/ctr-bin:$PATH" CONTAINERD_SOCKET=/tmp/custom.sock CTR_RESULT=error
    if wait_for_local_allocations_to_stop; then echo 'Query error accepted as evacuation' >&2; exit 1; fi
    export CTR_RESULT=empty
    wait_for_local_allocations_to_stop
)
printf 'PASS evacuation query failure and configured socket\n'
cp "$script_dir/upgrade.sh" "$tmp/upgrade.sh"
printf 'source %q\nsource %q\n' "$script_dir/common.sh" "$tmp/mocks.sh" >"$tmp/common.sh"
cat >"$tmp/mocks.sh" <<'MOCKS'
require_root_linux_amd64() { :; }
require_commands() { :; }
fetch_latest_release() { RELEASE_TAG=v-new; }
download_release() {
    cp "$CONFIG_DECODER" "$1/trellis"
    for binary in trellisctl trellis-health-probe; do
        cp "$INSTALL_DIR/trellisctl" "$1/$binary"
    done
}
getent() { printf 'operator:x:1000:1000::%s:/bin/bash\n' "$OPERATOR_HOME"; }
systemctl() { printf '%s\n' "$*" >>"$SERVICE_LOG"; }
write_service() { :; }
write_state_version() { printf '%s\n' "$1" >"$VERSION_LOG"; }
running_binary() { printf '%s\n' "$INSTALL_DIR/trellis.running"; }
verify_running_version() { [[ "$SCENARIO" != *version-rollback ]]; }
wait_for_service() {
    case "$SCENARIO" in *signal-install) kill -TERM $$ ;; *interrupt-install) kill -INT $$ ;; esac
    [[ "$SCENARIO" != *version-rollback ]] || return 0
    [[ "$SCENARIO" != *rollback ]]
}
wait_for_local_allocations_to_stop() {
    case "$SCENARIO" in *signal) kill -TERM $$ ;; *interrupt) kill -INT $$ ;; esac
    [[ "$SCENARIO" != *timeout ]]
}
cp() {
    if [[ "$SCENARIO" = *failure && "${*: -1}" = */trellisctl.old ]]; then return 1; fi
    command cp "$@"
}
journalctl() { :; }
sleep() { :; }
MOCKS
for scenario in single single-label single-pretty multi multi-compact malformed wrong-shape empty explicit missing unauthorized timeout rollback version-rollback disk-version-rollback draining-version-rollback root quoted-data missing-id invalid-config draining-success draining-timeout draining-rollback draining-signal draining-interrupt draining-signal-install draining-interrupt-install draining-failure multi-signal multi-interrupt multi-signal-install multi-interrupt-install multi-failure multi-drain-error absent-local missing-status duplicate-local; do
    (
        export SCENARIO="$scenario"
        export INSTALL_DIR="$tmp/$scenario/bin" CONFIG_DIR="$tmp/$scenario/etc"
        export STATE_ROOT="$tmp/$scenario/state" RUN_DIR="$tmp/$scenario/run"
        export SERVICE_FILE="$tmp/$scenario/trellis.service" SECRETS_KEY_FILE="$CONFIG_DIR/secrets.key"
        export OPERATOR_HOME="$tmp/$scenario/home" SUDO_USER=operator
        export CALL_LOG="$tmp/$scenario.calls" SERVICE_LOG="$tmp/$scenario.services"
        export VERSION_LOG="$tmp/$scenario.version"
        touch "$CALL_LOG" "$SERVICE_LOG"
        unset TRELLIS_CONFIG
        # Maintenance must not use an ambient remote context or credentials.
        export TRELLIS_CONTEXT=remote TRELLIS_TOKEN=wrong-token
        export TRELLIS_ADMINISTRATOR_KEY=wrong-admin TRELLIS_ADDR=remote.example:8128
        export TRELLIS_CA_CERT=wrong-ca TRELLIS_CERT=wrong-cert TRELLIS_KEY=wrong-key
        mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$STATE_ROOT/data" "$RUN_DIR" "$OPERATOR_HOME/.config/trellis"
        touch "$RUN_DIR/ca.crt"
        printf 'data_dir: "%s/data"\ncontainerd_socket: "/tmp/custom.sock"\n' "$STATE_ROOT" >"$CONFIG_DIR/trellis.yaml"
        printf 'node-a\n' >"$STATE_ROOT/data/node-id"
        if [ "$scenario" = quoted-data ]; then
            mkdir -p "$STATE_ROOT/custom data"
            mv "$STATE_ROOT/data/node-id" "$STATE_ROOT/custom data/node-id"
            printf "data_dir: '%s/custom data' # comment\n" "$STATE_ROOT" >"$CONFIG_DIR/trellis.yaml"
        elif [ "$scenario" = missing-id ]; then
            rm "$STATE_ROOT/data/node-id"
        elif [ "$scenario" = invalid-config ]; then
            printf 'data_dir: [invalid]\n' >"$CONFIG_DIR/trellis.yaml"
        fi
        export EXPECTED_CONFIG="$OPERATOR_HOME/.config/trellis/config.yaml"
        if [ "$scenario" = explicit ]; then
            export TRELLIS_CONFIG="$tmp/custom-config.yaml" EXPECTED_CONFIG="$tmp/custom-config.yaml"
        elif [ "$scenario" = root ]; then
            unset SUDO_USER
        fi
        if [ "$scenario" != missing ]; then touch "$EXPECTED_CONFIG"; fi
        cat >"$INSTALL_DIR/trellisctl" <<'CTL'
#!/usr/bin/env bash
set -euo pipefail
[ "$TRELLIS_CONFIG" = "$EXPECTED_CONFIG" ]
[ -z "${TRELLIS_TOKEN:-}" ]
[ -z "${TRELLIS_ADMINISTRATOR_KEY:-}" ]
[ "$1" = --context ] && [ "$2" = local ]
[ "$3" = --server-addr ] && [ "$4" = https://127.0.0.1:8128 ]
[ "$5" = --ca-cert ] && [ "$6" = "$RUN_DIR/ca.crt" ]
[ "$7" = --cert= ] && [ "$8" = --key= ]
shift 8
printf '%s\n' "$*" >>"$CALL_LOG"
case "$*" in
    'nodes list --output json')
        if [ "$SCENARIO" = unauthorized ]; then
            printf 'status 401: invalid credential\n' >&2
            exit 1
        fi
        case "$SCENARIO" in
            single-label) printf '[\n{"id":"node-a","status":"healthy",\n"labels":{"id":"rack-a"}}\n]\n'; exit 0 ;;
            single-pretty) printf '[\n{"id":"node-a","status":"healthy"}\n]\n'; exit 0 ;;
            multi-compact) printf '[{"id":"node-a","status":"healthy"},{"id":"node-b"}]\n'; exit 0 ;;
            draining-*) printf '[{"id":"node-a","status":"draining"},{"id":"node-b"}]\n'; exit 0 ;;
            absent-local) printf '[{"id":"node-b","status":"healthy"}]\n'; exit 0 ;;
            missing-status) printf '[{"id":"node-a"}]\n'; exit 0 ;;
            duplicate-local) printf '[{"id":"node-a","status":"healthy"},{"id":"node-a","status":"draining"}]\n'; exit 0 ;;
            malformed) printf '[{"id":'; exit 0 ;;
            wrong-shape) printf '{"id":"node-a"}\n'; exit 0 ;;
            empty) printf '[]\n'; exit 0 ;;
        esac
        if [ "$SCENARIO" = single ] || [ "$SCENARIO" = root ]; then
            printf '[{"id":"node-a","status":"healthy"}]\n'
        else
            printf '[\n{"id":"node-a","status":"healthy"},\n{"id":"node-b"}\n]\n'
        fi
        ;;
    'nodes drain node-a') [ "$SCENARIO" != multi-drain-error ] ;;
    'nodes undrain node-a') ;;
    *) exit 1 ;;
esac
CTL
        chmod +x "$INSTALL_DIR/trellisctl"
        printf '#!/bin/sh\necho v-old\n' >"$INSTALL_DIR/trellis"
        chmod +x "$INSTALL_DIR/trellis"
        cp "$INSTALL_DIR/trellis" "$INSTALL_DIR/trellis.running"
        if [ "$scenario" = disk-version-rollback ]; then
            printf '#!/bin/sh\necho v-new\n' >"$INSTALL_DIR/trellis"
        fi
        # Test runners may inherit ignored SIGINT; Bash cannot trap a signal
        # ignored at startup. Restore it so these exercise a real interrupt.
        if env --default-signal=INT bash "$tmp/upgrade.sh" >"$tmp/$scenario.output" 2>&1; then
            case "$scenario" in single*|multi|multi-compact|explicit|root|quoted-data|draining-success) ;; *) exit 1 ;; esac
            [ "$(cat "$VERSION_LOG")" = v-new ]
            if [[ "$scenario" = single* ]] || [ "$scenario" = root ]; then
                ! grep -q 'nodes drain' "$CALL_LOG"
                grep -q 'Single-node cluster' "$tmp/$scenario.output"
            elif [[ "$scenario" != draining-* ]]; then
                grep -qx 'nodes drain node-a' "$CALL_LOG"
                grep -qx 'nodes undrain node-a' "$CALL_LOG"
            fi
        else
            rc=$?
            case "$scenario" in
                *signal*) [ "$rc" -eq 143 ] ;;
                *interrupt*) [ "$rc" -eq 130 ] ;;
            esac
            case "$scenario" in missing|unauthorized|timeout|*rollback|malformed|wrong-shape|empty|missing-id|invalid-config|draining-*|multi-*|absent-local|missing-status|duplicate-local) ;; *) cat "$tmp/$scenario.output"; exit 1 ;; esac
            [ "$("$INSTALL_DIR/trellis" --version)" = v-old ]
            test ! -e "$VERSION_LOG"
            if [[ "$scenario" = *rollback || "$scenario" = *-install ]]; then
                grep -qx 'stop trellis' "$SERVICE_LOG"
            else
                ! grep -q '^stop trellis$' "$SERVICE_LOG"
            fi
            if [[ "$scenario" = multi-* || "$scenario" = rollback || "$scenario" = version-rollback || "$scenario" = disk-version-rollback ]]; then
                grep -qx 'nodes undrain node-a' "$CALL_LOG"
            fi
            case "$scenario" in
                missing) grep -q 'Operator config missing' "$tmp/$scenario.output" ;;
                unauthorized) grep -q 'status 401: invalid credential' "$tmp/$scenario.output" ;;
                malformed|wrong-shape|empty)
                    grep -q 'Invalid cluster membership output' "$tmp/$scenario.output"
                    ! grep -q 'nodes drain' "$CALL_LOG"
                    ;;
                timeout) grep -qx 'nodes undrain node-a' "$CALL_LOG" ;;
            esac
        fi
        if [[ "$scenario" = draining-* ]]; then
            ! grep -q 'nodes drain\|nodes undrain' "$CALL_LOG"
        fi
        printf 'PASS upgrade: %s\n' "$scenario"
    )
done
