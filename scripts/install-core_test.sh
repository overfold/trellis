#!/usr/bin/env bash
# Exercise the operator-access phase without changing host services or packages.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
go build -o "$tmp/real-trellisctl" "$script_dir/../orchestrator/cmd/trellisctl"
export REAL_CTL="$tmp/real-trellisctl"
awk '/^ui_section "Operator access"/ {copy=1} /^unset administrator_private_key administrator_public_key/ {copy=0} copy' \
    "$script_dir/install-core.sh" >"$tmp/operator-access.sh"
mkdir "$tmp/bin"
cat >"$tmp/bin/trellisctl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$CALL_LOG"
if [[ " $* " == *" credentials create "* ]]; then
    printf 'new-operator-token\n'
elif [[ " $* " == *" nodes status "* ]]; then
    [ -z "${TRELLIS_TOKEN:-}" ]
    [ -z "${TRELLIS_ADMINISTRATOR_KEY:-}" ]
    if [ "${READINESS_FAIL:-false}" = true ]; then
        printf 'Status: unhealthy\n'
    else
        printf 'Status: healthy\n'
    fi
else
    exec "$REAL_CTL" "$@"
fi
MOCK
chmod +x "$tmp/bin/trellisctl"

for scenario in replacement resume join first-install worker-timeout; do
    (
        source "$script_dir/common.sh"
        export TEST_HOME="$tmp/$scenario" CALL_LOG="$tmp/$scenario.calls"
        INSTALL_DIR="$tmp/bin"; RUN_DIR="$tmp/run"; WORK_TMP="$tmp"
        SUDO_USER=test-operator
        existing_config=false; join_addr=""; existing_join=""; administrator_private_key=test-key
        advertise_host=192.0.2.10
        case "$scenario" in
            resume) existing_config=true ;;
            join) join_addr=node-a:8128; administrator_private_key="" ;;
            worker-timeout) existing_config=true; export READINESS_FAIL=true ;;
        esac
        mkdir -p "$TEST_HOME/.config/trellis" "$RUN_DIR"
        printf 'public-ca\n' >"$RUN_DIR/ca.crt"
        cat >"$TEST_HOME/.config/trellis/config.yaml" <<'CONFIG'
current_context: remote
contexts:
  local:
    token: old-operator-token
    ca_cert: old-ca
  remote:
    server_addr: remote.example:8128
    token: remote-token
    ca_cert: remote-ca
CONFIG
        if [ "$scenario" = first-install ]; then
            sed -i '/^  local:/,/^  remote:/{ /^  remote:/!d; }' "$TEST_HOME/.config/trellis/config.yaml"
        fi
        # Deliberately conflicting ambient configuration must not leak into local.
        export TRELLIS_CONFIG="$tmp/unrelated.yaml" TRELLIS_CONTEXT=remote
        export TRELLIS_CA_CERT=stale-env-ca TRELLIS_ADDR=wrong.example:8128
        export TRELLIS_CERT=wrong-cert TRELLIS_KEY=wrong-key TRELLIS_TOKEN=wrong-token
        getent() { printf 'test-operator:x:1000:1000::%s:/bin/bash\n' "$TEST_HOME"; }
        id() { printf 'test-operator\n'; }
        install() { mkdir -p "${@: -1}"; }
        chown() { :; }
        sleep() { :; }
        if ( source "$tmp/operator-access.sh" ) >"$tmp/$scenario.output" 2>&1; then
            [ "$scenario" != worker-timeout ]
        else
            [ "$scenario" = worker-timeout ]
            grep -q 'control-plane API is healthy, but the local worker is not ready' "$tmp/$scenario.output"
            ! grep -q 'Local worker is registered and healthy' "$tmp/$scenario.output"
            printf 'PASS operator access: worker-timeout stops installation\n'
            exit 0
        fi
        cat "$tmp/$scenario.output"
        config="$TEST_HOME/.config/trellis/config.yaml"
        grep -q 'token: remote-token' "$config"
        grep -q 'ca_cert: remote-ca' "$config"
        if [ "$scenario" = resume ] || [ "$scenario" = join ]; then
            if [ "$scenario" = join ]; then test ! -e "$CALL_LOG"; fi
            grep -q 'token: old-operator-token' "$config"
        else
            grep -q 'current_context: local' "$config"
            grep -q 'token: new-operator-token' "$config"
            grep -q "ca_cert_file: $RUN_DIR/ca.crt" "$config"
            ! grep -q 'old-ca\|wrong-\|stale-env-ca' "$config"
            grep -q -- '--context= --server-addr https://127.0.0.1:8128 --ca-cert' "$CALL_LOG"
            shown="$(TRELLIS_CONFIG="$config" "$REAL_CTL" --context= context show local)"
            grep -q "CA: file: $RUN_DIR/ca.crt" <<<"$shown"
            ! grep -q 'new-operator-token' <<<"$shown"
        fi
        if [ "$scenario" != join ]; then
            grep -q -- '--context local --server-addr https://127.0.0.1:8128.*nodes status 192.0.2.10:8127 --output table' "$CALL_LOG"
        fi
        printf 'PASS operator access: %s\n' "$scenario"
    )
done

for scenario in delayed unhealthy unavailable; do
    (
        source "$script_dir/common.sh"
        CALL_LOG="$tmp/readiness-$scenario.calls"
        env() {
            printf '%s\n' "$*" >>"$CALL_LOG"
            case "$scenario" in
                delayed)
                    case "$(wc -l <"$CALL_LOG")" in
                        1) return 1 ;; # Worker has not registered yet.
                        2) printf 'Status: unhealthy\n' ;;
                        *) printf 'Status: healthy\n' ;;
                    esac
                    ;;
                unhealthy) printf 'Status: unhealthy\n' ;;
                unavailable) return 1 ;;
            esac
        }
        sleep() { :; }
        if wait_for_local_node "$tmp/config.yaml" 192.0.2.10:8127; then
            [ "$scenario" = delayed ]
            [ "$(wc -l <"$CALL_LOG")" -eq 3 ]
        else
            [ "$scenario" != delayed ]
            [ "$(wc -l <"$CALL_LOG")" -eq 30 ]
        fi
        grep -q -- '-u TRELLIS_TOKEN -u TRELLIS_ADMINISTRATOR_KEY' "$CALL_LOG"
        grep -q -- 'nodes status 192.0.2.10:8127 --output table' "$CALL_LOG"
        printf 'PASS worker readiness: %s\n' "$scenario"
    )
done
