#!/usr/bin/env bash
# Run the full upgrade flow with mocked releases, CLI, and host services.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cp "$script_dir/upgrade.sh" "$tmp/upgrade.sh"
printf 'source %q\nsource %q\n' "$script_dir/common.sh" "$tmp/mocks.sh" >"$tmp/common.sh"
cat >"$tmp/mocks.sh" <<'MOCKS'
require_root_linux_amd64() { :; }
require_commands() { :; }
fetch_latest_release() { RELEASE_TAG=v-new; }
download_release() {
    for binary in trellis trellisctl trellis-health-probe; do
        cp "$INSTALL_DIR/trellisctl" "$1/$binary"
    done
}
getent() { printf 'operator:x:1000:1000::%s:/bin/bash\n' "$OPERATOR_HOME"; }
systemctl() { printf '%s\n' "$*" >>"$SERVICE_LOG"; }
write_service() { :; }
write_state_version() { printf '%s\n' "$1" >"$VERSION_LOG"; }
wait_for_service() { [ "$SCENARIO" != rollback ]; }
wait_for_local_allocations_to_stop() { [ "$SCENARIO" != timeout ]; }
journalctl() { :; }
sleep() { :; }
MOCKS
for scenario in single multi explicit missing unauthorized timeout rollback root; do
    (
        export SCENARIO="$scenario"
        export INSTALL_DIR="$tmp/$scenario/bin" CONFIG_DIR="$tmp/$scenario/etc"
        export STATE_ROOT="$tmp/$scenario/state" RUN_DIR="$tmp/$scenario/run"
        export SERVICE_FILE="$tmp/$scenario/trellis.service" SECRETS_KEY_FILE="$CONFIG_DIR/secrets.key"
        export OPERATOR_HOME="$tmp/$scenario/home" SUDO_USER=operator
        export CALL_LOG="$tmp/$scenario.calls" SERVICE_LOG="$tmp/$scenario.services"
        export VERSION_LOG="$tmp/$scenario.version"
        unset TRELLIS_CONFIG
        # Maintenance must not use an ambient remote context or credentials.
        export TRELLIS_CONTEXT=remote TRELLIS_TOKEN=wrong-token
        export TRELLIS_ADMINISTRATOR_KEY=wrong-admin TRELLIS_ADDR=remote.example:8128
        export TRELLIS_CA_CERT=wrong-ca TRELLIS_CERT=wrong-cert TRELLIS_KEY=wrong-key
        mkdir -p "$INSTALL_DIR" "$CONFIG_DIR" "$STATE_ROOT/data" "$RUN_DIR" "$OPERATOR_HOME/.config/trellis"
        touch "$CONFIG_DIR/trellis.yaml" "$RUN_DIR/ca.crt"
        printf 'node-a\n' >"$STATE_ROOT/data/node-id"
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
        if [ "$SCENARIO" = single ] || [ "$SCENARIO" = root ]; then
            printf '[{"id":"node-a"}]\n'
        else
            printf '[\n{"id":"node-a"},\n{"id":"node-b"}\n]\n'
        fi
        ;;
    'nodes drain node-a'|'nodes undrain node-a') ;;
    *) exit 1 ;;
esac
CTL
        chmod +x "$INSTALL_DIR/trellisctl"
        printf '#!/bin/sh\necho v-old\n' >"$INSTALL_DIR/trellis"
        chmod +x "$INSTALL_DIR/trellis"
        if bash "$tmp/upgrade.sh" >"$tmp/$scenario.output" 2>&1; then
            case "$scenario" in single|multi|explicit|root) ;; *) exit 1 ;; esac
            [ "$(cat "$VERSION_LOG")" = v-new ]
            if [ "$scenario" = single ] || [ "$scenario" = root ]; then
                ! grep -q 'nodes drain' "$CALL_LOG"
                grep -q 'Single-node cluster' "$tmp/$scenario.output"
            else
                grep -qx 'nodes drain node-a' "$CALL_LOG"
                grep -qx 'nodes undrain node-a' "$CALL_LOG"
            fi
        else
            case "$scenario" in missing|unauthorized|timeout|rollback) ;; *) cat "$tmp/$scenario.output"; exit 1 ;; esac
            [ "$("$INSTALL_DIR/trellis" --version)" = v-old ]
            test ! -e "$VERSION_LOG"
            if [ "$scenario" = rollback ]; then
                grep -qx 'stop trellis' "$SERVICE_LOG"
                grep -qx 'nodes undrain node-a' "$CALL_LOG"
            else
                ! grep -q '^stop trellis$' "$SERVICE_LOG"
            fi
            case "$scenario" in
                missing) grep -q 'Operator config missing' "$tmp/$scenario.output" ;;
                unauthorized) grep -q 'status 401: invalid credential' "$tmp/$scenario.output" ;;
                timeout) grep -qx 'nodes undrain node-a' "$CALL_LOG" ;;
            esac
        fi
        printf 'PASS upgrade: %s\n' "$scenario"
    )
done
