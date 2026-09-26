#!/usr/bin/env bash
set -euo pipefail

REPO="${REPO:-clofour/trellis}"
RAW_BASE="${RAW_BASE:-https://raw.githubusercontent.com/${REPO}/main/scripts}"
TMP=""

cleanup() { [ -z "$TMP" ] || rm -rf "$TMP"; }
trap cleanup EXIT

usage() {
    cat <<'HELP'
Install a Trellis node.

Usage: setup.sh [options]

Options:
  --advertise HOST              Address peers and workloads can use to reach this node
  --join HOST:8128              Join an existing cluster
  --enrollment-token-file FILE  Read the managed-mode enrollment credential from FILE
  --ca-cert-file FILE           Pin the existing cluster node CA certificate
  --secrets-key-file FILE       Read the existing cluster secrets key from FILE
  --secrets-key-id ID           Existing cluster key ID, when explicitly configured
  --with-networking             Install namespace networking (default)
  --without-networking          Skip namespace networking
  --with-gvisor                 Install gVisor/runsc (default)
  --without-gvisor              Skip gVisor/runsc
  --with-dashboard              Deploy the read-only dashboard
  --dashboard-write             Deploy the dashboard with cluster/write access
  -y, --yes                     Use the resulting plan without the interactive planner
  -h, --help                    Show this help

Interactive setup shows the complete plan first. Press Enter to install it, or
choose Customize to change cluster mode, address, networking, gVisor, or dashboard.
Flags provide the same choices for automation.

Environment alternatives for joins:
  TRELLIS_ENROLLMENT_TOKEN      Existing managed-mode enrollment credential
  TRELLIS_SECRETS_KEY           Existing cluster 32-byte/base64 secrets key
  TRELLIS_SECRETS_KEY_ID        Existing cluster key ID, when explicitly configured
HELP
}

for arg in "$@"; do
    case "$arg" in -h|--help) usage; exit 0 ;; esac
done

resolve_engine() {
    local script_dir
    TMP="$(mktemp -d)"
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd || true)"
    if [ -n "$script_dir" ] && [ -f "$script_dir/setup-core.sh" ] && [ -f "$script_dir/common.sh" ]; then
        cp "$script_dir/setup-core.sh" "$TMP/setup-core.sh"
        cp "$script_dir/common.sh" "$TMP/common-real.sh"
    else
        command -v curl >/dev/null 2>&1 || { echo "error: curl is required" >&2; exit 1; }
        curl -fsSL "$RAW_BASE/setup-core.sh" -o "$TMP/setup-core.sh"
        curl -fsSL "$RAW_BASE/common.sh" -o "$TMP/common-real.sh"
    fi
    cat >"$TMP/common.sh" <<'SHIM'
source "$(dirname "${BASH_SOURCE[0]}")/common-real.sh"
if [ "${TRELLIS_SETUP_PLAN_CONFIRMED:-}" = 1 ]; then
    _trellis_hide_details=false
    ui_title() { :; }
    ui_section() {
        if [ "$1" = Plan ]; then _trellis_hide_details=true; return; fi
        _trellis_hide_details=false
        printf '%s◇%s %s%s%s\n' "$BLUE" "$RESET" "$BOLD" "$1" "$RESET"
    }
    ui_detail() {
        [ "$_trellis_hide_details" = true ] || printf '%s│%s  %s\n' "$DIM" "$RESET" "$*"
    }
fi
SHIM
}
resolve_engine
# shellcheck source=/dev/null
source "$TMP/common-real.sh"
require_root_linux_amd64

advertise=""; join=""; enrollment_file=""; ca_file=""; key_file=""; key_id=""
networking=true; gvisor=true; dashboard=off; assume_yes=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --advertise) [ "$#" -ge 2 ] || ui_die "--advertise requires a value"; advertise="$2"; shift 2 ;;
        --join) [ "$#" -ge 2 ] || ui_die "--join requires HOST:8128"; join="$2"; shift 2 ;;
        --enrollment-token-file) [ "$#" -ge 2 ] || ui_die "--enrollment-token-file requires a path"; enrollment_file="$2"; shift 2 ;;
        --ca-cert-file) [ "$#" -ge 2 ] || ui_die "--ca-cert-file requires a path"; ca_file="$2"; shift 2 ;;
        --secrets-key-file) [ "$#" -ge 2 ] || ui_die "--secrets-key-file requires a path"; key_file="$2"; shift 2 ;;
        --secrets-key-id) [ "$#" -ge 2 ] || ui_die "--secrets-key-id requires a value"; key_id="$2"; shift 2 ;;
        --with-networking) networking=true; shift ;;
        --without-networking) networking=false; shift ;;
        --with-gvisor) gvisor=true; shift ;;
        --without-gvisor) gvisor=false; shift ;;
        --with-dashboard) dashboard=read; shift ;;
        --dashboard-write) dashboard=write; shift ;;
        -y|--yes) assume_yes=true; shift ;;
        *) ui_die "Unknown option: $1" ;;
    esac
done

load_install_state
if [ "$NETWORKING_ENABLED" = true ]; then networking=true; fi
if [ "$GVISOR_ENABLED" = true ]; then gvisor=true; fi
if [ "$DASHBOARD_INSTALLED" = true ]; then dashboard="$DASHBOARD_ACCESS_STATE"; fi

# Complete installs should retain the engine's fast already-installed path.
if { [ "$STATE_COMPLETE" = true ] && [ -x "$INSTALL_DIR/trellis" ] && [ -f "$CONFIG_FILE" ]; } || \
   { [ ! -f "$STATE_FILE" ] && [ -x "$INSTALL_DIR/trellis" ] && [ -f "$CONFIG_FILE" ] && [ -f "$SERVICE_FILE" ]; }; then
    bash "$TMP/setup-core.sh" --yes
    exit $?
fi

existing_config=false
if [ -f "$CONFIG_FILE" ]; then
    existing_config=true
    configured="$(awk -F': ' '$1 == "agent_advertise" {sub(/:8127$/, "", $2); print $2; exit}' "$CONFIG_FILE")"
    configured_join="$(awk -F': ' '$1 == "join" {print $2; exit}' "$CONFIG_FILE")"
    [ -z "$configured" ] || advertise="$configured"
    [ -z "$configured_join" ] || join="$configured_join"
fi
[ -n "$advertise" ] || advertise="$(detect_advertise_ipv4 2>/dev/null || true)"
[ -n "$advertise" ] || ui_die "Could not determine a routable IPv4 advertise address. Pass --advertise HOST explicitly."
[ -z "$join" ] || [[ "$join" == *:* ]] || ui_die "Join address must look like node-a:8128"
fetch_latest_release

cluster_label() { [ -n "$join" ] && printf 'join %s' "$join" || printf 'create a new cluster'; }
dashboard_label() { case "$dashboard" in read) printf 'read-only' ;; write) printf 'read/write' ;; *) printf 'not installed' ;; esac; }

show_plan() {
    ui_section "Plan"
    ui_detail "Version       $RELEASE_TAG"
    ui_detail "Node address  $advertise"
    ui_detail "Cluster       $(cluster_label)"
    ui_detail "Networking    $([ "$networking" = true ] && printf enabled || printf disabled)"
    ui_detail "gVisor        $([ "$gvisor" = true ] && printf installed || printf 'not installed')"
    ui_detail "Dashboard     $(dashboard_label)"
}

customize() {
    local choice value
    while true; do
        printf '\n'; ui_section "Customize setup"
        ui_detail "1. Cluster              $(cluster_label)"
        ui_detail "2. Node address         $advertise"
        ui_detail "3. Namespace networking $([ "$networking" = true ] && printf enabled || printf disabled)"
        ui_detail "4. Runtime sandbox      $([ "$gvisor" = true ] && printf 'gVisor installed' || printf 'gVisor not installed')"
        ui_detail "5. Dashboard            $(dashboard_label)"
        printf '\nSelect a setting to change, or press Enter when done: '
        read -r choice </dev/tty
        case "$choice" in
            "") return ;;
            1)
                [ "$existing_config" = false ] || { ui_warn "Cluster mode is fixed while resuming setup."; continue; }
                printf 'New cluster [1] or join existing [2] [1]: '; read -r value </dev/tty
                if [ "${value:-1}" = 2 ]; then
                    printf 'Existing node (HOST:8128): '; read -r join </dev/tty
                    [[ "$join" == *:* ]] || { join=""; ui_warn "Enter an address such as node-a:8128."; }
                else join=""; fi
                ;;
            2)
                [ "$existing_config" = false ] || { ui_warn "Node address is fixed while resuming setup."; continue; }
                printf 'Node address [%s]: ' "$advertise"; read -r value </dev/tty; [ -z "$value" ] || advertise="$value"
                ;;
            3) [ "$NETWORKING_ENABLED" != true ] || { ui_warn "Networking was already installed and will be kept."; continue; }; [ "$networking" = true ] && networking=false || networking=true ;;
            4) [ "$GVISOR_ENABLED" != true ] || { ui_warn "gVisor was already installed and will be kept."; continue; }; [ "$gvisor" = true ] && gvisor=false || gvisor=true ;;
            5)
                printf 'Disabled [1], read-only [2], or read/write [3]: '; read -r value </dev/tty
                case "$value" in 1) dashboard=off ;; 2) dashboard=read ;; 3) dashboard=write ;; *) ui_warn "Choose 1, 2, or 3." ;; esac
                ;;
            *) ui_warn "Choose 1-5, or press Enter when done." ;;
        esac
    done
}

confirmed=false
if [ "$assume_yes" = false ]; then
    ui_title "setup"
    while true; do
        show_plan
        printf '\n%sInstall%s [Enter]   %sCustomize%s [c]   Cancel [q]: ' "$BOLD" "$RESET" "$BOLD" "$RESET"
        read -r choice </dev/tty
        case "$choice" in
            ""|[Ii]|[Yy]) confirmed=true; break ;;
            [Cc]) customize; printf '\n' ;;
            [Qq]|[Nn]) ui_detail "No changes made."; exit 0 ;;
            *) ui_warn "Press Enter to install, c to customize, or q to cancel."; printf '\n' ;;
        esac
    done
fi

args=(--yes --advertise "$advertise")
[ -z "$join" ] || args+=(--join "$join")
[ -z "$enrollment_file" ] || args+=(--enrollment-token-file "$enrollment_file")
[ -z "$ca_file" ] || args+=(--ca-cert-file "$ca_file")
[ -z "$key_file" ] || args+=(--secrets-key-file "$key_file")
[ -z "$key_id" ] || args+=(--secrets-key-id "$key_id")
[ "$networking" = true ] && args+=(--with-networking)
[ "$gvisor" = true ] && args+=(--with-gvisor)
case "$dashboard" in read) args+=(--with-dashboard) ;; write) args+=(--dashboard-write) ;; esac

if [ "$confirmed" = true ]; then
    TRELLIS_SETUP_PLAN_CONFIRMED=1 bash "$TMP/setup-core.sh" "${args[@]}"
else
    bash "$TMP/setup-core.sh" "${args[@]}"
fi
