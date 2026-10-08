#!/usr/bin/env bash
# Exercise the operator-access phase without changing host services or packages.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
go build -o "$tmp/real-trellisctl" "$script_dir/../orchestrator/cmd/trellisctl"
export REAL_CTL="$tmp/real-trellisctl"

# Inspect mktemp's inode before the first write, and the completed inode before
# publication. All destinations are deliberately in traversable custom paths.
source "$script_dir/common.sh"
mktemp() {
    local file
    file="$(command mktemp "$@")"
    [ "$(stat -c %a "$file")" = 600 ] && [ ! -s "$file" ] || return 1
    printf 'created %s\n' "$file" >>"$PERMISSION_LOG"
    printf '%s\n' "$file"
}
mv() {
    local source="$2" target="$3"
    [ "$1" = -f ]
    [ "$(stat -c %a "$source")" = 600 ]
    [ -s "$source" ]
    command mv "$@"
    printf 'published %s\n' "$target" >>"$PERMISSION_LOG"
}
awk '/^read_secret\(\)/ {copy=1} /^write_service$/ {copy=0} copy' \
    "$script_dir/install-core.sh" >"$tmp/create-config.sh"
for mode in new control-plane worker reuse; do
    (
        umask 022
        export PERMISSION_LOG="$tmp/$mode.permissions"
        CONFIG_DIR="$tmp/$mode-config"; DATA_DIR="$tmp/$mode-data"
        mkdir -p "$CONFIG_DIR" "$DATA_DIR" "$tmp/$mode-keys"
        chmod 755 "$CONFIG_DIR" "$tmp/$mode-keys"
        CONFIG_FILE="$CONFIG_DIR/custom.yaml"; SECRETS_KEY_FILE="$tmp/$mode-keys/custom.key"
        advertise_host=node; runs_workloads=true; control_plane=true
        join_addr=""; join_secrets_key_id=""; join_token_file=""; join_secrets_file=""; ca_cert_file=""
        if [ "$mode" != new ]; then
            join_addr=control:8128
            TRELLIS_JOIN_TOKEN=join-secret; TRELLIS_SECRETS_KEY=cluster-secret
            ca_cert_file="$CONFIG_DIR/source.crt"; printf 'public-ca\n' >"$ca_cert_file"
        fi
        if [ "$mode" = worker ]; then control_plane=false; fi
        if [ "$mode" = control-plane ]; then
            printf 'previous-key\n' >"$SECRETS_KEY_FILE"
            if [ "$(id -u)" = 0 ]; then command chown 65534:65534 "$SECRETS_KEY_FILE"; fi
        fi
        if [ "$mode" = reuse ]; then
            printf 'control_plane: true\n' >"$CONFIG_FILE"
            printf 'original-key\n' >"$SECRETS_KEY_FILE"
            before="$(stat -c '%i:%u:%g' "$CONFIG_FILE")"
        fi
        source "$tmp/create-config.sh" >/dev/null 2>&1
        [ "$(stat -c %a "$CONFIG_FILE")" = 600 ]
        if [ "$mode" = reuse ]; then
            [ "$before" = "$(stat -c '%i:%u:%g' "$CONFIG_FILE")" ]
            grep -qx original-key "$SECRETS_KEY_FILE"
            test ! -e "$PERMISSION_LOG"
        else
            grep -q "published $CONFIG_FILE" "$PERMISSION_LOG"
            if [ "$mode" = worker ]; then
                test ! -e "$SECRETS_KEY_FILE"
            else
                grep -q "published $SECRETS_KEY_FILE" "$PERMISSION_LOG"
                [ "$(stat -c %a "$SECRETS_KEY_FILE")" = 600 ]
                if [ "$mode" = control-plane ]; then
                    [ "$(id -u):$(id -g)" = "$(stat -c '%u:%g' "$SECRETS_KEY_FILE")" ]
                    grep -qx cluster-secret "$SECRETS_KEY_FILE"
                fi
            fi
            if [ "$mode" != new ]; then grep -qx 'join_token: join-secret' "$CONFIG_FILE"; fi
        fi
    )
    printf 'PASS private installer creation: %s\n' "$mode"
done

for mode in file environment precedence absent empty-file empty-env missing-file multiline enrolled external; do
    (
        unset TRELLIS_JOIN_TOKEN
        export PERMISSION_LOG="$tmp/resume-$mode.permissions"
        CONFIG_DIR="$tmp/resume-$mode/config"; DATA_DIR="$tmp/resume-$mode/data"
        mkdir -p "$CONFIG_DIR" "$DATA_DIR/tls"
        CONFIG_FILE="$CONFIG_DIR/trellis.yaml"; SECRETS_KEY_FILE="$CONFIG_DIR/secrets.key"
        printf 'node_signing_mode: managed\njoin: original:8128\njoin_token: exhausted\nca_cert: original-ca\ncontrol_plane: true\nagent_advertise: original:8127\n' >"$CONFIG_FILE"
        printf 'original-key\n' >"$SECRETS_KEY_FILE"
        printf 'original-id\n' >"$DATA_DIR/node-id"
        cp "$CONFIG_FILE" "$CONFIG_DIR/before"
        before_owner="$(stat -c '%u:%g' "$CONFIG_FILE")"
        join_token_file=""
        case "$mode" in
            file|precedence|empty-file|multiline)
                join_token_file="$CONFIG_DIR/replacement"
                printf 'replacement-file\n' >"$join_token_file"
                [ "$mode" != empty-file ] || : >"$join_token_file"
                [ "$mode" != multiline ] || printf 'token\ninjected: value\n' >"$join_token_file"
                [ "$mode" != precedence ] || export TRELLIS_JOIN_TOKEN=ignored-environment
                ;;
            environment|enrolled|external) export TRELLIS_JOIN_TOKEN=replacement-environment ;;
            empty-env) export TRELLIS_JOIN_TOKEN= ;;
            missing-file) join_token_file="$CONFIG_DIR/missing" ;;
        esac
        [ "$mode" != enrolled ] || printf 'stored-certificate\n' >"$DATA_DIR/tls/node-cert"
        [ "$mode" != external ] || sed -i 's/mode: managed/mode: external/' "$CONFIG_FILE"
        if [[ "$mode" = empty-* || "$mode" = missing-file || "$mode" = multiline ]]; then
            if (source "$tmp/create-config.sh") >"$CONFIG_DIR/output" 2>&1; then
                echo 'Accepted invalid replacement token' >&2; exit 1
            fi
            cmp "$CONFIG_DIR/before" "$CONFIG_FILE"
            test ! -e "$PERMISSION_LOG"
        else
            source "$tmp/create-config.sh" >/dev/null 2>&1
            case "$mode" in
                file|precedence|environment)
                    if [ "$mode" = environment ]; then expected=replacement-environment; else expected=replacement-file; fi
                    grep -qx "join_token: $expected" "$CONFIG_FILE"
                    [ "$(grep -c '^join_token:' "$CONFIG_FILE")" -eq 1 ]
                    diff <(sed '/^join_token:/d' "$CONFIG_DIR/before") <(sed '/^join_token:/d' "$CONFIG_FILE")
                    grep -q "published $CONFIG_FILE" "$PERMISSION_LOG"
                    [ "$(stat -c %a "$CONFIG_FILE")" = 600 ]
                    [ "$(stat -c '%u:%g' "$CONFIG_FILE")" = "$before_owner" ]
                    ;;
                *) grep -qx 'join_token: exhausted' "$CONFIG_FILE"; test ! -e "$PERMISSION_LOG" ;;
            esac
        fi
        grep -qx original-key "$SECRETS_KEY_FILE"
        grep -qx original-id "$DATA_DIR/node-id"
        [ "$mode" != enrolled ] || grep -qx stored-certificate "$DATA_DIR/tls/node-cert"
        printf 'PASS managed join replacement: %s\n' "$mode"
    )
done

# Completed installs return before reading replacement input or publishing config.
(
    source "$script_dir/common.sh"
    require_root_linux_amd64() { :; }; require_commands() { :; }
    CONFIG_FILE="$tmp/completed.yaml"; INSTALL_DIR="$tmp/completed-bin"; STATE_FILE="$tmp/completed-state"
    mkdir -p "$INSTALL_DIR"
    printf '#!/bin/sh\n' >"$INSTALL_DIR/trellis"; chmod +x "$INSTALL_DIR/trellis"
    printf 'join_token: untouched\n' >"$CONFIG_FILE"
    printf 'complete=true\n' >"$STATE_FILE"
    join_token_file=/missing/replacement
    export TRELLIS_JOIN_TOKEN=replacement
    awk '/^load_install_state$/ {copy=1} /^resuming=false$/ {copy=0} copy' "$script_dir/install-core.sh" >"$tmp/completed.sh"
    (source "$tmp/completed.sh") >/dev/null
    grep -qx 'join_token: untouched' "$CONFIG_FILE"
)
printf 'PASS completed install ignores replacement\n'

for demo_node in worker-1 worker-2 control control-new; do
    (
        umask 022
        root="$tmp/demo-$demo_node"
        mkdir -p "$root/share" "$root/etc" "$root/systemd" "$root/data"
        chmod 755 "$root/etc" "$root/share"
        if [ "$demo_node" != control-new ]; then
            printf 'existing-admin-key\n' >"$root/share/administrator-key.pem"
            printf 'existing-public-key\n' >"$root/share/administrator-public-key"
        fi
        printf 'public-ca\n' >"$root/share/node-ca.crt"
        printf 'public-ca\n' >"$root/data/node-ca.crt"
        printf 'worker-join-secret\n' >"$root/share/join-token-$demo_node"
        export PERMISSION_LOG="$root/permissions"
        hostname() { printf '%s\n' "${demo_node%-new}"; }
        systemctl() { :; }
        install() { if [ "$2" = 0644 ]; then command install "$@"; fi; }
        trellisctl() { printf 'minted-token\n'; }
        openssl() {
            [ "$demo_node" = control-new ] || { echo 'unexpected key generation on existing demo cluster' >&2; return 1; }
            command openssl "$@"
        }
        sed -e "s|/vagrant/bin|$root/share|g" -e "s|/var/lib/trellis/data|$root/data|g" \
            -e "s|/etc/trellis|$root/etc|g" -e "s|/etc/systemd/system|$root/systemd|g" \
            "$script_dir/../orchestrator/demo/trellis.sh" >"$root/provision.sh"
        source "$root/provision.sh"
        [ "$(stat -c %a "$root/etc/trellis.yaml")" = 600 ]
        grep -q "published $root/etc/trellis.yaml" "$PERMISSION_LOG"
        if [[ "$demo_node" = control* ]]; then
            if [ "$demo_node" = control-new ]; then
                [ "$(stat -c %a "$root/share/administrator-key.pem")" = 600 ]
                grep -q "published $root/share/administrator-key.pem" "$PERMISSION_LOG"
            fi
            for worker in worker-1 worker-2; do
                [ "$(stat -c %a "$root/share/join-token-$worker")" = 600 ]
                grep -qx minted-token "$root/share/join-token-$worker"
            done
        else
            grep -qx 'join_token: worker-join-secret' "$root/etc/trellis.yaml"
        fi
    )
    printf 'PASS private Vagrant provisioning: %s\n' "$demo_node"
done
unset -f mktemp mv

# Execute the complete worker install against temporary paths. A deliberately
# unreadable/nonexistent secrets file and ambient secrets prove the worker path
# neither consumes control-plane key inputs nor creates private cluster material.
mkdir -p "$tmp/worker-installer" "$tmp/worker-bin" "$tmp/worker/home/.config/trellis"
cp "$script_dir/install-core.sh" "$tmp/worker-installer/install-core.sh"
cat >"$tmp/worker-installer/common.sh" <<EOF
source "$script_dir/common.sh"
require_root_linux_amd64() { :; }
require_commands() { :; }
detect_advertise_ipv4() { printf '192.0.2.20\\n'; }
fetch_latest_release() { RELEASE_TAG=v-worker-test; }
networking_tools_present() { return 0; }
download_release() {
    for binary in trellis trellisctl trellis-health-probe; do
        printf '#!/bin/sh\\nexit 0\\n' >"\$1/\$binary"
    done
}
write_service() { printf 'worker service\\n' >"\$SERVICE_FILE"; }
wait_for_service() { printf 'relayed-local-api\\n' >>"\$CALL_LOG"; }
wait_for_local_node() { printf 'registered-worker %s\\n' "\$2" >>"\$CALL_LOG"; }
EOF
cat >"$tmp/worker-bin/systemctl" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$tmp/worker-bin/containerd" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$tmp/worker-bin/openssl" <<'EOF'
#!/bin/sh
echo 'worker installer invoked openssl' >&2
exit 97
EOF
cat >"$tmp/worker-bin/getent" <<EOF
#!/bin/sh
printf 'test-operator:x:1000:1000::${tmp}/worker/home:/bin/sh\\n'
EOF
cat >"$tmp/worker-bin/id" <<'EOF'
#!/bin/sh
printf 'test-operator\n'
EOF
cat >"$tmp/worker-bin/chown" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod +x "$tmp/worker-bin/"*
cat >"$tmp/worker/home/.config/trellis/config.yaml" <<'EOF'
current_context: local
contexts:
  local:
    token: existing-operator-token
EOF
printf 'join-token-value\n' >"$tmp/worker/join-token"
printf 'public-ca-certificate\n' >"$tmp/worker/cluster-ca.crt"
(
    export PATH="$tmp/worker-bin:$PATH" HOME="$tmp/worker/home" SUDO_USER=test-operator
    export INSTALL_DIR="$tmp/worker/install" STATE_ROOT="$tmp/worker/state"
    export CONFIG_DIR="$tmp/worker/etc" RUN_DIR="$tmp/worker/run"
    export SERVICE_FILE="$tmp/worker/trellis.service" CALL_LOG="$tmp/worker.calls"
    export TRELLIS_SECRETS_KEY='ambient-secret-must-not-be-read'
    export TRELLIS_SECRETS_KEY_ID='ambient-key-id-must-not-be-written'
    bash "$tmp/worker-installer/install-core.sh" --yes --worker --runs-workloads false \
        --advertise 192.0.2.20 --join control.example:8128 \
        --join-token-file "$tmp/worker/join-token" --ca-cert-file "$tmp/worker/cluster-ca.crt" \
        --secrets-key-file "$tmp/worker/does-not-exist"
) >"$tmp/worker.output" 2>&1
worker_config="$tmp/worker/etc/trellis.yaml"
grep -qx 'control_plane: false' "$worker_config"
grep -qx 'runs_workloads: false' "$worker_config"
grep -qx 'join: control.example:8128' "$worker_config"
grep -qx 'ca_cert: .*node-ca.crt' "$worker_config"
! grep -Eq '^(raft_advertise|secrets_key|secrets_key_id|ca_key|administrator_public_key):' "$worker_config"
! grep -Rq 'ambient-secret-must-not-be-read\|ambient-key-id-must-not-be-written' "$tmp/worker" "$tmp/worker.calls"
test ! -e "$tmp/worker/etc/secrets.key"
test ! -e "$tmp/worker/etc/ca.key"
test ! -e "$tmp/worker/state/data/raft"
! grep -q '^join_token:' "$worker_config"
grep -qx 'relayed-local-api' "$tmp/worker.calls"
grep -qx 'registered-worker 192.0.2.20:8127' "$tmp/worker.calls"
printf 'PASS executable worker install is keyless, joins, and verifies registration\n'
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
        grep -q -- '--context local --server-addr https://127.0.0.1:8128.*nodes status 192.0.2.10:8127 --output table' "$CALL_LOG"
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
