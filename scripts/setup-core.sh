#!/usr/bin/env bash
set -euo pipefail

RAW_COMMON="https://raw.githubusercontent.com/clofour/trellis/main/scripts/common.sh"
COMMON_TMP=""
WORK_TMP=""
STARTED=false

cleanup() {
    local rc=$?
    [ -z "$WORK_TMP" ] || rm -rf "$WORK_TMP"
    [ -z "$COMMON_TMP" ] || rm -rf "$COMMON_TMP"
    if [ "$rc" -ne 0 ] && [ "$STARTED" = true ]; then
        printf '\n'
        ui_warn "Setup did not finish. The completed steps were kept; rerun the same command to resume."
    fi
}
trap cleanup EXIT

load_common() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd || true)"
    if [ -n "$script_dir" ] && [ -f "${script_dir}/common.sh" ]; then
        # shellcheck source=common.sh
        source "${script_dir}/common.sh"
        return
    fi
    command -v curl >/dev/null 2>&1 || { echo "error: curl is required" >&2; exit 1; }
    COMMON_TMP="$(mktemp -d)"
    curl -fsSL "$RAW_COMMON" -o "${COMMON_TMP}/common.sh"
    # shellcheck source=/dev/null
    source "${COMMON_TMP}/common.sh"
}
load_common

usage() {
    cat <<'EOF_USAGE'
Install a Trellis node.

Usage:
  setup.sh [options]

Options:
  --advertise HOST              Address peers and workloads can use to reach this node
  --join HOST:8128              Join an existing cluster instead of creating one
  --enrollment-token-file FILE  Read the managed-mode node enrollment token from FILE
  --ca-cert-file FILE           Pin the existing cluster node CA certificate
  --secrets-key-file FILE       Read the existing cluster secrets key from FILE
  --secrets-key-id ID           Existing cluster key ID when it was explicitly configured
  --with-networking             Install WireGuard dependencies for namespace networking
  --with-gvisor                 Install gVisor/runsc
  --with-dashboard              Deploy the read-only Trellis dashboard
  --dashboard-write             Give the dashboard cluster/write access (implies --with-dashboard)
  -y, --yes                     Apply the displayed plan without confirmation
  -h, --help                    Show this help

Environment alternatives for joins:
  TRELLIS_ENROLLMENT_TOKEN      Existing managed-mode enrollment token
  TRELLIS_SECRETS_KEY           Existing cluster 32-byte/base64 secrets key
  TRELLIS_SECRETS_KEY_ID        Existing cluster key ID when explicitly configured
EOF_USAGE
}

advertise_host=""
join_addr=""
enrollment_token_file=""
ca_cert_file=""
join_secrets_file=""
join_secrets_key_id="${TRELLIS_SECRETS_KEY_ID:-}"
with_networking=false
with_gvisor=false
with_dashboard=false
dashboard_access="read"
assume_yes=false
administrator_private_key="${TRELLIS_ADMINISTRATOR_KEY:-}"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --advertise) [ "$#" -ge 2 ] || ui_die "--advertise requires a value"; advertise_host="$2"; shift 2 ;;
        --join) [ "$#" -ge 2 ] || ui_die "--join requires a host:port"; join_addr="$2"; shift 2 ;;
        --enrollment-token-file) [ "$#" -ge 2 ] || ui_die "--enrollment-token-file requires a path"; enrollment_token_file="$2"; shift 2 ;;
        --ca-cert-file) [ "$#" -ge 2 ] || ui_die "--ca-cert-file requires a path"; ca_cert_file="$2"; shift 2 ;;
        --secrets-key-file) [ "$#" -ge 2 ] || ui_die "--secrets-key-file requires a path"; join_secrets_file="$2"; shift 2 ;;
        --secrets-key-id) [ "$#" -ge 2 ] || ui_die "--secrets-key-id requires a value"; join_secrets_key_id="$2"; shift 2 ;;
        --with-networking) with_networking=true; shift ;;
        --with-gvisor) with_gvisor=true; shift ;;
        --with-dashboard) with_dashboard=true; shift ;;
        --dashboard-write) with_dashboard=true; dashboard_access="write"; shift ;;
        -y|--yes) assume_yes=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) ui_die "Unknown option: $1" ;;
    esac
done

require_root_linux_amd64
require_commands curl tar systemctl openssl awk grep install mktemp
load_install_state

# An interrupted setup keeps the features it already installed. Explicit flags
# may add capabilities, but rerunning the installer never silently removes them.
[ "$NETWORKING_ENABLED" != true ] || with_networking=true
[ "$GVISOR_ENABLED" != true ] || with_gvisor=true
if [ "$DASHBOARD_INSTALLED" = true ]; then
    with_dashboard=true
    [ "$DASHBOARD_ACCESS_STATE" != write ] || dashboard_access=write
fi

# Recognize complete installs that predate the install-state file without claiming
# ownership of packages that Trellis cannot prove it installed.
if [ ! -f "$STATE_FILE" ] && [ -x "${INSTALL_DIR}/trellis" ] && [ -f "$CONFIG_FILE" ] && [ -f "$SERVICE_FILE" ]; then
    STATE_COMPLETE=true
    STATE_VERSION="$("${INSTALL_DIR}/trellis" --version 2>/dev/null | awk '{print $NF}' || true)"
    CONTAINERD_OWNED=false; CONTAINERD_CONFIG_OWNED=false; DOCKER_REPO_OWNED=false; DOCKER_KEY_OWNED=false
    RUNSC_OWNED=false; GVISOR_REPO_OWNED=false; GVISOR_KEY_OWNED=false; GVISOR_CONFIG_OWNED=false; WIREGUARD_OWNED=false
    NETWORKING_ENABLED=false; GVISOR_ENABLED=false; DASHBOARD_INSTALLED=false; DASHBOARD_NAMESPACE=default; DASHBOARD_ACCESS_STATE=read
    write_install_state
fi

if [ "$STATE_COMPLETE" = true ] && [ -x "${INSTALL_DIR}/trellis" ] && [ -f "$CONFIG_FILE" ]; then
    ui_title "setup"
    ui_step "Trellis ${STATE_VERSION:-unknown} is already installed"
    ui_detail "Upgrade: curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/upgrade.sh | sudo bash"
    exit 0
fi

resuming=false
if [ -f "$STATE_FILE" ] || [ -x "${INSTALL_DIR}/trellis" ] || [ -f "$CONFIG_FILE" ] || [ -f "$SERVICE_FILE" ]; then
    resuming=true
fi

existing_config=false
existing_join=""
if [ -f "$CONFIG_FILE" ]; then
    load_node_config_paths
    existing_config=true
    configured_advertise="$(awk -F': ' '$1 == "agent_advertise" {sub(/:8127$/, "", $2); print $2; exit}' "$CONFIG_FILE")"
    existing_join="$(awk -F': ' '$1 == "join" {print $2; exit}' "$CONFIG_FILE")"
    [ -z "$configured_advertise" ] || advertise_host="$configured_advertise"
fi
if [ -z "$advertise_host" ]; then
    advertise_host="$(detect_advertise_ipv4 2>/dev/null || true)"
fi
[ -n "$advertise_host" ] || ui_die "Could not determine a routable IPv4 advertise address. Pass --advertise HOST explicitly."

if [ -n "$join_addr" ] && [[ "$join_addr" != *:* ]]; then
    ui_die "--join must be an existing node address such as node-a:8128"
fi
if [ -n "$enrollment_token_file" ] && [ ! -r "$enrollment_token_file" ]; then ui_die "Cannot read $enrollment_token_file"; fi
if [ -n "$ca_cert_file" ] && [ ! -r "$ca_cert_file" ]; then ui_die "Cannot read $ca_cert_file"; fi
if [ -n "$join_secrets_file" ] && [ ! -r "$join_secrets_file" ]; then ui_die "Cannot read $join_secrets_file"; fi

fetch_latest_release

containerd_action="reuse existing installation"
if ! command -v containerd >/dev/null 2>&1; then containerd_action="install automatically"; fi
cluster_action="create a new cluster"
if [ "$existing_config" = true ]; then
    cluster_action="reuse existing node configuration"
    [ -z "$existing_join" ] || cluster_action="resume join to ${existing_join}"
elif [ -n "$join_addr" ]; then
    cluster_action="join ${join_addr}"
fi

ui_title "setup"
if [ "$resuming" = true ]; then
    ui_warn "A previous setup appears incomplete. Trellis will reuse completed state and continue."
    printf '\n'
fi
ui_section "Plan"
ui_detail "Version       ${RELEASE_TAG}"
ui_detail "Node address  ${advertise_host}"
ui_detail "Cluster       ${cluster_action}"
ui_detail "containerd    ${containerd_action}"
ui_detail "Networking    $([ "$with_networking" = true ] && printf 'enabled' || printf 'disabled')"
ui_detail "gVisor        $([ "$with_gvisor" = true ] && printf 'enabled' || printf 'disabled')"
ui_detail "Dashboard     $([ "$with_dashboard" = true ] && printf '%s' "$dashboard_access" || printf 'not installed')"

if [ "$assume_yes" != true ]; then
    printf '\n%sApply this plan? [Y/n] %s' "$BOLD" "$RESET"
    read -r answer </dev/tty
    case "${answer:-y}" in [Yy]*) ;; *) ui_detail "No changes made."; exit 0 ;; esac
fi
STARTED=true

STATE_COMPLETE=false
STATE_VERSION="$RELEASE_TAG"
DASHBOARD_NAMESPACE="${DASHBOARD_NAMESPACE:-default}"
write_install_state

ui_section "Host"
if command -v containerd >/dev/null 2>&1; then
    if systemctl is-active --quiet containerd; then
        ui_step "containerd is ready"
    else
        systemctl enable --now containerd >/dev/null
        ui_step "Started existing containerd"
    fi
else
    install_containerd
fi

WORK_TMP="$(mktemp -d)"
ui_step "Downloading Trellis ${RELEASE_TAG}"
download_release "$WORK_TMP"
install -d -m 0755 "$INSTALL_DIR"
install -m 0755 "${WORK_TMP}/trellis" "${INSTALL_DIR}/.trellis.new"
install -m 0755 "${WORK_TMP}/trellisctl" "${INSTALL_DIR}/.trellisctl.new"
install -m 0755 "${WORK_TMP}/trellis-health-probe" "${INSTALL_DIR}/.trellis-health-probe.new"
mv "${INSTALL_DIR}/.trellis.new" "${INSTALL_DIR}/trellis"
mv "${INSTALL_DIR}/.trellisctl.new" "${INSTALL_DIR}/trellisctl"
mv "${INSTALL_DIR}/.trellis-health-probe.new" "${INSTALL_DIR}/trellis-health-probe"
ui_step "Installed trellis, trellisctl, and trellis-health-probe"

install -d -m 0750 "$DATA_DIR" "$CONFIG_DIR"

read_secret() {
    local prompt="$1" file="$2" env_value="$3" value=""
    if [ -n "$file" ]; then
        value="$(cat "$file")"
    elif [ -n "$env_value" ]; then
        value="$env_value"
    else
        printf '%s: ' "$prompt" >/dev/tty
        read -r -s value </dev/tty
        printf '\n' >/dev/tty
    fi
    [ -n "$value" ] || ui_die "$prompt is required."
    printf '%s' "$value"
}

if [ ! -f "$CONFIG_FILE" ]; then
    admin_public_key_config=""
    if [ -n "$join_addr" ]; then
        enrollment_token="$(read_secret "Existing cluster enrollment token" "$enrollment_token_file" "${TRELLIS_ENROLLMENT_TOKEN:-}")"
        [ -n "$ca_cert_file" ] || ui_die "--ca-cert-file is required when joining so enrollment uses the pinned cluster CA."
        install -m 0644 "$ca_cert_file" "${CONFIG_DIR}/node-ca.crt"
        secrets_value="$(read_secret "Existing cluster secrets key" "$join_secrets_file" "${TRELLIS_SECRETS_KEY:-}")"
        printf '%s\n' "$secrets_value" >"$SECRETS_KEY_FILE"
        unset secrets_value
    else
        administrator_key_pem="$(openssl genpkey -algorithm ED25519)"
        administrator_private_key="$(printf '%s\n' "$administrator_key_pem" | openssl pkey -outform DER | base64 | tr -d '=\n')"
        administrator_public_key="$(printf '%s\n' "$administrator_key_pem" | openssl pkey -pubout -outform DER | base64 | tr -d '=\n')"
        unset administrator_key_pem
        admin_public_key_config="administrator_public_key: ${administrator_public_key}"
        enrollment_token="trls_enroll_$(head -c 32 /dev/urandom | base64 | tr -d '=\n')"
        openssl rand -base64 32 >"$SECRETS_KEY_FILE"
    fi
    chmod 600 "$SECRETS_KEY_FILE"
    cat >"$CONFIG_FILE" <<EOF_CONFIG
cluster: default
${admin_public_key_config}
enrollment_token: ${enrollment_token}
node_signing_mode: managed
data_dir: ${DATA_DIR}
agent_advertise: ${advertise_host}:8127
server_advertise: ${advertise_host}:8128
raft_advertise: ${advertise_host}:8129
secrets_key: ${SECRETS_KEY_FILE}
EOF_CONFIG
    if [ -n "$join_addr" ]; then
        printf 'join: %s\n' "$join_addr" >>"$CONFIG_FILE"
        printf 'ca_cert: %s\n' "${CONFIG_DIR}/node-ca.crt" >>"$CONFIG_FILE"
        [ -z "$join_secrets_key_id" ] || printf 'secrets_key_id: %s\n' "$join_secrets_key_id" >>"$CONFIG_FILE"
    fi
    chmod 600 "$CONFIG_FILE"
    ui_step "Created node configuration"
    if [ -n "${administrator_private_key:-}" ]; then
        ui_warn "Save this base64 PKCS#8 administrator private key in an operator password manager; Trellis does not retain it: ${administrator_private_key}"
    fi
else
    [ -f "$SECRETS_KEY_FILE" ] || ui_die "${CONFIG_FILE} exists but ${SECRETS_KEY_FILE} is missing; restore the matching key and rerun setup."
    chmod 600 "$CONFIG_FILE" "$SECRETS_KEY_FILE"
    ui_step "Reusing existing node configuration"
fi

write_service
systemctl enable --now trellis >/dev/null
if ! wait_for_service "$WORK_TMP"; then
    journalctl -u trellis -n 20 --no-pager >&2 || true
    ui_die "Trellis did not become healthy."
fi
ui_step "Trellis service is healthy"

if [ "$with_networking" = true ] && [ "$NETWORKING_ENABLED" != true ]; then install_networking; fi
if [ "$with_gvisor" = true ] && [ "$GVISOR_ENABLED" != true ]; then install_gvisor; fi
if [ "$with_networking" = true ] || [ "$with_gvisor" = true ]; then
    systemctl restart trellis
    wait_for_service "$WORK_TMP" || ui_die "Trellis did not become healthy after dependency setup."
fi

ui_section "Operator access"
operator_user="${SUDO_USER:-root}"
if [ "$operator_user" = "root" ]; then
    operator_home="/root"; operator_group="root"
else
    operator_home="$(getent passwd "$operator_user" | cut -d: -f6)"
    operator_group="$(id -gn "$operator_user")"
    [ -n "$operator_home" ] || ui_die "Could not determine home directory for ${operator_user}."
fi
operator_config="${operator_home}/.config/trellis/config.yaml"
if [ -f "$operator_config" ] && grep -q '^  local:' "$operator_config" 2>/dev/null; then
    ui_step "Existing local trellisctl context kept for ${operator_user}"
else
    if [ -z "${administrator_private_key:-}" ]; then
        ui_detail "No administrator credential was copied to this joining node; configure trellisctl from an operator workstation."
    else
        operator_token=""
        for _ in $(seq 1 30); do
            operator_token="$(TRELLIS_ADMINISTRATOR_KEY="$administrator_private_key" local_ctl "$WORK_TMP" credentials create --scope cluster --access write --output table 2>/dev/null || true)"
            [ -n "$operator_token" ] && break
            sleep 1
        done
        [ -n "$operator_token" ] || ui_die "Trellis is running, but an operator credential could not be created."
        operator_config_home="${operator_home}/.config"
        install -d -m 0700 -o "$operator_user" -g "$operator_group" "$operator_config_home"
        HOME="$operator_home" XDG_CONFIG_HOME="$operator_config_home" \
            "${INSTALL_DIR}/trellisctl" --token "$operator_token" --namespace default context save local --use >/dev/null
        if [ "$operator_user" != "root" ]; then chown -R "${operator_user}:${operator_group}" "${operator_config_home}/trellis"; fi
        unset operator_token
        ui_step "Saved local cluster/write context for ${operator_user}"
    fi
fi
if [ "$with_dashboard" = true ]; then
    [ -n "${administrator_private_key:-}" ] || ui_die "Dashboard deployment requires an operator-side administrator key and is not performed while joining a node."
    ui_section "Dashboard"
    dashboard_operator_token="$(TRELLIS_ADMINISTRATOR_KEY="$administrator_private_key" local_ctl "$WORK_TMP" credentials create --scope cluster --access write)"
    TRELLIS_TOKEN="$dashboard_operator_token" deploy_dashboard "$WORK_TMP" "$RELEASE_TAG" default "$dashboard_access"
    unset dashboard_operator_token
    DASHBOARD_INSTALLED=true
    DASHBOARD_NAMESPACE=default
    DASHBOARD_ACCESS_STATE="$dashboard_access"
    write_install_state
    ui_step "Dashboard deployed on port 3000"
    if [ "$dashboard_access" = write ]; then
        ui_warn "The dashboard has cluster/write access. Put port 3000 behind your own HTTPS and identity-aware proxy."
    fi
fi
unset administrator_private_key administrator_public_key admin_public_key_config enrollment_token

STATE_COMPLETE=true
STATE_VERSION="$RELEASE_TAG"
write_install_state
ui_done "Trellis ${RELEASE_TAG} is ready"
ui_detail "Config   ${CONFIG_FILE}"
ui_detail "Verify   trellisctl nodes list"
ui_detail "Learn    docs/public/getting-started.md"
ui_detail "Upgrade  curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/upgrade.sh | sudo bash"
