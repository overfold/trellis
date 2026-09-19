#!/usr/bin/env bash
# Shared lifecycle helpers for Trellis setup, upgrade, and uninstall.
# This file is sourced by the entrypoint scripts; it is not meant to be run directly.

REPO="${REPO:-clofour/trellis}"
RAW_BASE="${RAW_BASE:-https://raw.githubusercontent.com/${REPO}/main/scripts}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
STATE_ROOT="${STATE_ROOT:-/var/lib/trellis}"
DATA_DIR="${DATA_DIR:-${STATE_ROOT}/data}"
STATE_FILE="${STATE_FILE:-${STATE_ROOT}/install-state}"
CONFIG_DIR="${CONFIG_DIR:-/etc/trellis}"
CONFIG_FILE="${CONFIG_FILE:-${CONFIG_DIR}/trellis.yaml}"
SECRETS_KEY_FILE="${SECRETS_KEY_FILE:-${CONFIG_DIR}/secrets.key}"
SERVICE_FILE="${SERVICE_FILE:-/etc/systemd/system/trellis.service}"
RUN_DIR="${RUN_DIR:-/run/trellis}"

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    BOLD=$'\033[1m'; DIM=$'\033[2m'; BLUE=$'\033[34m'; GREEN=$'\033[32m'
    YELLOW=$'\033[33m'; RED=$'\033[31m'; RESET=$'\033[0m'
else
    BOLD=""; DIM=""; BLUE=""; GREEN=""; YELLOW=""; RED=""; RESET=""
fi

ui_title() { printf '\n%s%sTrellis %s%s\n\n' "$BOLD" "$BLUE" "$1" "$RESET"; }
ui_section() { printf '%s◇%s %s%s%s\n' "$BLUE" "$RESET" "$BOLD" "$1" "$RESET"; }
ui_detail() { printf '%s│%s  %s\n' "$DIM" "$RESET" "$*"; }
ui_step() { printf '%s✓%s %s\n' "$GREEN" "$RESET" "$*"; }
ui_warn() { printf '%s!%s %s\n' "$YELLOW" "$RESET" "$*" >&2; }
ui_die() { printf '%sx%s %s\n' "$RED" "$RESET" "$*" >&2; exit 1; }
ui_done() { printf '\n%s◆%s %s%s%s\n' "$GREEN" "$RESET" "$BOLD" "$1" "$RESET"; }

require_root_linux_amd64() {
    [ "$(uname -s)" = "Linux" ] || ui_die "This script only supports Linux."
    [ "$(uname -m)" = "x86_64" ] || ui_die "This script only supports x86_64 (amd64)."
    [ "$(id -u)" -eq 0 ] || ui_die "Run this script as root (or with sudo)."
}

require_commands() {
    local cmd
    for cmd in "$@"; do
        command -v "$cmd" >/dev/null 2>&1 || ui_die "Required command not found: $cmd"
    done
}

state_get() {
    local key="$1" default="${2:-}" value
    [ -f "$STATE_FILE" ] || { printf '%s' "$default"; return; }
    value="$(awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$STATE_FILE")"
    printf '%s' "${value:-$default}"
}

load_install_state() {
    STATE_COMPLETE="$(state_get complete false)"
    STATE_VERSION="$(state_get version "")"
    CONTAINERD_OWNED="$(state_get containerd_owned false)"
    CONTAINERD_CONFIG_OWNED="$(state_get containerd_config_owned false)"
    DOCKER_REPO_OWNED="$(state_get docker_repo_owned false)"
    DOCKER_KEY_OWNED="$(state_get docker_key_owned false)"
    RUNSC_OWNED="$(state_get runsc_owned false)"
    GVISOR_REPO_OWNED="$(state_get gvisor_repo_owned false)"
    GVISOR_KEY_OWNED="$(state_get gvisor_key_owned false)"
    GVISOR_CONFIG_OWNED="$(state_get gvisor_config_owned false)"
    WIREGUARD_OWNED="$(state_get wireguard_owned false)"
    NETWORKING_ENABLED="$(state_get networking_enabled false)"
    GVISOR_ENABLED="$(state_get gvisor_enabled false)"
    DASHBOARD_INSTALLED="$(state_get dashboard_installed false)"
    DASHBOARD_NAMESPACE="$(state_get dashboard_namespace default)"
    DASHBOARD_ACCESS_STATE="$(state_get dashboard_access read)"
}


load_node_config_paths() {
    [ -f "$CONFIG_FILE" ] || return 0
    local configured_data configured_key
    configured_data="$(awk -F': ' '$1 == "data_dir" {print $2; exit}' "$CONFIG_FILE")"
    configured_key="$(awk -F': ' '$1 == "secrets_key" {print $2; exit}' "$CONFIG_FILE")"
    [ -z "$configured_data" ] || DATA_DIR="$configured_data"
    [ -z "$configured_key" ] || SECRETS_KEY_FILE="$configured_key"
}

write_install_state() {
    install -d -m 0750 "$STATE_ROOT"
    local tmp
    tmp="$(mktemp "${STATE_ROOT}/.install-state.XXXXXX")"
    cat >"$tmp" <<EOF
format=1
complete=${STATE_COMPLETE}
version=${STATE_VERSION}
containerd_owned=${CONTAINERD_OWNED}
containerd_config_owned=${CONTAINERD_CONFIG_OWNED}
docker_repo_owned=${DOCKER_REPO_OWNED}
docker_key_owned=${DOCKER_KEY_OWNED}
runsc_owned=${RUNSC_OWNED}
gvisor_repo_owned=${GVISOR_REPO_OWNED}
gvisor_key_owned=${GVISOR_KEY_OWNED}
gvisor_config_owned=${GVISOR_CONFIG_OWNED}
wireguard_owned=${WIREGUARD_OWNED}
networking_enabled=${NETWORKING_ENABLED}
gvisor_enabled=${GVISOR_ENABLED}
dashboard_installed=${DASHBOARD_INSTALLED}
dashboard_namespace=${DASHBOARD_NAMESPACE}
dashboard_access=${DASHBOARD_ACCESS_STATE}
EOF
    chmod 600 "$tmp"
    mv "$tmp" "$STATE_FILE"
}

write_state_version() {
    [ -f "$STATE_FILE" ] || return 0
    local tmp
    tmp="$(mktemp "${STATE_ROOT}/.install-state.XXXXXX")"
    awk -F= -v version="$1" '
        BEGIN { done = 0 }
        $1 == "version" { print "version=" version; done = 1; next }
        { print }
        END { if (!done) print "version=" version }
    ' "$STATE_FILE" >"$tmp"
    chmod 600 "$tmp"
    mv "$tmp" "$STATE_FILE"
}

is_ipv4() {
    local value="$1" octet
    local -a octets
    IFS=. read -r -a octets <<<"$value"
    [ "${#octets[@]}" -eq 4 ] || return 1
    for octet in "${octets[@]}"; do
        [[ "$octet" =~ ^[0-9]+$ ]] || return 1
        (( 10#$octet <= 255 )) || return 1
    done
}

is_private_ipv4() {
    local value="$1" a b c d
    is_ipv4 "$value" || return 1
    IFS=. read -r a b c d <<<"$value"
    [ "$a" -eq 10 ] ||
        { [ "$a" -eq 172 ] && [ "$b" -ge 16 ] && [ "$b" -le 31 ]; } ||
        { [ "$a" -eq 192 ] && [ "$b" -eq 168 ]; }
}

detect_private_ipv4() {
    local value
    if command -v ip >/dev/null 2>&1; then
        value="$(ip -4 route get 1.1.1.1 2>/dev/null |
            awk '{ for (i = 1; i <= NF; i++) if ($i == "src") { print $(i + 1); exit } }')"
        if [ -n "$value" ] && is_private_ipv4 "$value"; then printf '%s\n' "$value"; return 0; fi
        while read -r value; do
            if is_private_ipv4 "$value"; then printf '%s\n' "$value"; return 0; fi
        done < <(ip -o -4 addr show scope global 2>/dev/null | awk '{ sub(/\/.*/, "", $4); print $4 }')
    fi
    while read -r value; do
        if is_private_ipv4 "$value"; then printf '%s\n' "$value"; return 0; fi
    done < <(hostname -I 2>/dev/null | tr ' ' '\n')
    return 1
}

detect_advertise_ipv4() {
    local value
    if command -v ip >/dev/null 2>&1; then
        value="$(ip -4 route get 1.1.1.1 2>/dev/null |
            awk '{ for (i = 1; i <= NF; i++) if ($i == "src") { print $(i + 1); exit } }')"
        if [ -n "$value" ] && is_ipv4 "$value" && [[ "$value" != 127.* ]] && [ "$value" != "0.0.0.0" ]; then
            printf '%s\n' "$value"; return 0
        fi
        while read -r value; do
            if is_ipv4 "$value" && [[ "$value" != 127.* ]] && [ "$value" != "0.0.0.0" ]; then
                printf '%s\n' "$value"; return 0
            fi
        done < <(ip -o -4 addr show scope global 2>/dev/null | awk '{ sub(/\/.*/, "", $4); print $4 }')
    fi
    while read -r value; do
        if is_ipv4 "$value" && [[ "$value" != 127.* ]] && [ "$value" != "0.0.0.0" ]; then
            printf '%s\n' "$value"; return 0
        fi
    done < <(hostname -I 2>/dev/null | tr ' ' '\n')
    return 1
}

detect_distro() {
    [ -f /etc/os-release ] || ui_die "Cannot detect Linux distribution."
    # shellcheck disable=SC1091
    . /etc/os-release
    DISTRO_ID="${ID:-}"
    DISTRO_CODENAME="${VERSION_CODENAME:-}"
    case "$DISTRO_ID" in
        debian|ubuntu) ;;
        *) ui_die "Automatic dependency installation supports Debian and Ubuntu. Install dependencies manually and retry." ;;
    esac
    [ -n "$DISTRO_CODENAME" ] || ui_die "Could not determine distribution codename."
}

fetch_latest_release() {
    local release_json
    release_json="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest")" ||
        ui_die "Failed to find the latest release."
    RELEASE_TAG="$(printf '%s' "$release_json" | grep -oP '"tag_name":\s*"\K[^"]+' | head -1 || true)"
    BIN_URL="$(printf '%s' "$release_json" |
        grep -oP '"browser_download_url":\s*"\K[^"]*trellis_linux_x64\.tar\.gz' | head -1 || true)"
    [ -n "$RELEASE_TAG" ] || ui_die "Latest release is missing a tag name."
    [ -n "$BIN_URL" ] || ui_die "Release ${RELEASE_TAG} is missing trellis_linux_x64.tar.gz."
}

download_release() {
    local dir="$1"
    curl -fsSL -o "${dir}/trellis_linux_x64.tar.gz" "$BIN_URL"
    tar -xzf "${dir}/trellis_linux_x64.tar.gz" -C "$dir"
    [ -x "${dir}/trellis" ] && [ -x "${dir}/trellisctl" ] ||
        ui_die "Release archive does not contain trellis and trellisctl."
    local reported
    reported="$("${dir}/trellis" --version 2>/dev/null | awk '{print $NF}' || true)"
    [ "$reported" = "$RELEASE_TAG" ] ||
        ui_die "Downloaded binary reports ${reported:-unknown}, expected ${RELEASE_TAG}."
}

write_service() {
    cat >"$SERVICE_FILE" <<EOF
[Unit]
Description=Trellis node
After=containerd.service network-online.target
Wants=containerd.service network-online.target

[Service]
ExecStart=${INSTALL_DIR}/trellis --config ${CONFIG_FILE}
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
}

local_ctl() {
    local temp_root="$1"
    shift
    TRELLIS_CONFIG="${temp_root}/root-config.yaml" "${INSTALL_DIR}/trellisctl" "$@"
}

wait_for_service() {
    local temp_root="$1" attempt
    for attempt in $(seq 1 30); do
        if systemctl is-active --quiet trellis &&
            local_ctl "$temp_root" nodes list >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

wait_for_local_allocations_to_stop() {
    command -v ctr >/dev/null 2>&1 || return 1
    local deadline=$((SECONDS + 300))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if [ -z "$(ctr -n trellis tasks ls -q 2>/dev/null || true)" ]; then
            return 0
        fi
        sleep 2
    done
    return 1
}

install_containerd() {
    detect_distro
    ui_step "Installing containerd"
    apt-get update -qq >/dev/null
    apt-get install -y -qq ca-certificates curl >/dev/null
    install -m 0755 -d /etc/apt/keyrings

    if [ ! -f /etc/apt/keyrings/docker.asc ]; then
        curl -fsSL "https://download.docker.com/linux/${DISTRO_ID}/gpg" -o /etc/apt/keyrings/docker.asc
        chmod a+r /etc/apt/keyrings/docker.asc
        DOCKER_KEY_OWNED=true
        write_install_state
    fi
    if [ ! -f /etc/apt/sources.list.d/docker.sources ]; then
        cat >/etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/${DISTRO_ID}
Suites: ${DISTRO_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
        DOCKER_REPO_OWNED=true
        write_install_state
    fi

    apt-get update -qq >/dev/null
    apt-get install -y -qq containerd.io >/dev/null
    CONTAINERD_OWNED=true
    write_install_state
    if [ ! -f /etc/containerd/config.toml ]; then
        install -d -m 0755 /etc/containerd
        containerd config default >/etc/containerd/config.toml
        CONTAINERD_CONFIG_OWNED=true
        write_install_state
    fi
    systemctl enable --now containerd >/dev/null
    write_install_state
}

install_networking() {
    detect_distro
    ui_step "Installing namespace-networking dependencies"
    local had_wireguard=false
    command -v wg >/dev/null 2>&1 && had_wireguard=true
    apt-get update -qq >/dev/null
    apt-get install -y -qq wireguard-tools iproute2 iptables >/dev/null
    $had_wireguard || WIREGUARD_OWNED=true
    NETWORKING_ENABLED=true
    write_install_state
}

install_gvisor() {
    detect_distro
    ui_step "Installing gVisor"
    local had_runsc=false had_runsc_config=false
    command -v runsc >/dev/null 2>&1 && had_runsc=true
    grep -q 'io.containerd.runsc.v1' /etc/containerd/config.toml 2>/dev/null && had_runsc_config=true
    apt-get update -qq >/dev/null
    apt-get install -y -qq ca-certificates curl gnupg >/dev/null

    if [ ! -f /usr/share/keyrings/gvisor-archive-keyring.gpg ]; then
        curl -fsSL https://gvisor.dev/archive.key |
            gpg --dearmor -o /usr/share/keyrings/gvisor-archive-keyring.gpg
        GVISOR_KEY_OWNED=true
        write_install_state
    fi
    if [ ! -f /etc/apt/sources.list.d/gvisor.list ]; then
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/gvisor-archive-keyring.gpg] https://storage.googleapis.com/gvisor/releases release main" \
            >/etc/apt/sources.list.d/gvisor.list
        GVISOR_REPO_OWNED=true
        write_install_state
    fi
    apt-get update -qq >/dev/null
    apt-get install -y -qq runsc >/dev/null
    $had_runsc || RUNSC_OWNED=true
    write_install_state
    runsc install >/dev/null
    $had_runsc_config || GVISOR_CONFIG_OWNED=true
    write_install_state
    systemctl restart containerd
    GVISOR_ENABLED=true
    write_install_state
}

dashboard_manifest() {
    local path="$1" tag="$2" namespace="$3" access="$4" allow_writes=""
    [ "$access" = "write" ] && allow_writes='          TRELLIS_ALLOW_WRITES: "true"'
    cat >"$path" <<EOF
namespace: ${namespace}
name: trellis-dashboard
task_groups:
  - name: web
    count: 1
    api_access:
      scope: cluster
      access: ${access}
    tasks:
      - name: dashboard
        image: ghcr.io/clofour/trellis-ui:${tag}
        env:
          TRELLIS_NAMESPACE: ${namespace}
${allow_writes}
        resources:
          cpu: 250
          memory: 512MiB
        networking:
          mode: host
          ports:
            - port: 3000
        health_check:
          type: http
          port: 3000
          path: /
EOF
}

deploy_dashboard() {
    local temp_root="$1" tag="$2" namespace="$3" access="$4"
    local manifest="${temp_root}/trellis-dashboard.yaml"
    dashboard_manifest "$manifest" "$tag" "$namespace" "$access"
    local_ctl "$temp_root" --namespace "$namespace" jobs apply --file "$manifest" --wait >/dev/null
}

package_installed() {
    dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q '^install ok installed$'
}

remove_owned_dependencies() {
    command -v apt-get >/dev/null 2>&1 || return 0
    command -v dpkg-query >/dev/null 2>&1 || return 0
    local removals=() repo_changed=false

    if [ "$GVISOR_CONFIG_OWNED" = true ] && command -v runsc >/dev/null 2>&1; then
        runsc uninstall >/dev/null 2>&1 || true
    fi
    if [ "$RUNSC_OWNED" = true ] && package_installed runsc; then
        removals+=(runsc)
    fi
    if [ "$WIREGUARD_OWNED" = true ] && package_installed wireguard-tools; then
        removals+=(wireguard-tools)
    fi
    if [ "$CONTAINERD_OWNED" = true ] && package_installed containerd.io; then
        removals+=(containerd.io)
    fi
    if [ "${#removals[@]}" -gt 0 ]; then
        ui_step "Removing Trellis-owned packages: ${removals[*]}"
        apt-get remove -y -qq "${removals[@]}" >/dev/null
    fi

    if [ "$DOCKER_REPO_OWNED" = true ]; then rm -f /etc/apt/sources.list.d/docker.sources; repo_changed=true; fi
    if [ "$DOCKER_KEY_OWNED" = true ]; then rm -f /etc/apt/keyrings/docker.asc; repo_changed=true; fi
    if [ "$GVISOR_REPO_OWNED" = true ]; then rm -f /etc/apt/sources.list.d/gvisor.list; repo_changed=true; fi
    if [ "$GVISOR_KEY_OWNED" = true ]; then rm -f /usr/share/keyrings/gvisor-archive-keyring.gpg; repo_changed=true; fi
    if $repo_changed; then
        apt-get update -qq >/dev/null
        ui_step "Removed Trellis-owned package repositories"
    fi

    if [ "$CONTAINERD_CONFIG_OWNED" = true ] && [ "$CONTAINERD_OWNED" = true ]; then
        rm -f /etc/containerd/config.toml
        rmdir /etc/containerd 2>/dev/null || true
    fi
}
