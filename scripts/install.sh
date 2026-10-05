#!/usr/bin/env bash
set -euo pipefail

REPO="${REPO:-overfold/trellis}"
RAW_BASE="${RAW_BASE:-https://raw.githubusercontent.com/${REPO}/main/scripts}"
TMP=""

cleanup() { [ -z "$TMP" ] || rm -rf "$TMP"; }
trap cleanup EXIT

usage() {
    cat <<'HELP'
Install a Trellis node.

Usage: install.sh [options]

Options:
  --advertise HOST              Address peers and workloads can use to reach this node
  --join HOST:8128              Join an existing cluster
  --join-token-file FILE        Read the node join token from FILE
  --ca-cert-file FILE           Pin the existing cluster node CA certificate
  --control-plane BOOL          Join the control plane (default: true)
  --worker                      Alias for --control-plane false
  --runs-workloads BOOL         Allow workload placement (default: true)
  --secrets-key-file FILE       Read the cluster secrets key (control-plane nodes only)
  --secrets-key-id ID           Existing cluster key ID, when explicitly configured
  --with-gvisor                 Install gVisor/runsc (default)
  --without-gvisor              Skip gVisor/runsc
  -y, --yes                     Use the resulting plan without the interactive planner
  -h, --help                    Show this help

Interactive installation shows the complete plan first. Press Enter to install it, or
choose Customize to change cluster mode, address, or gVisor. Every node gets
WireGuard namespace networking.
Flags provide the same choices for automation.

Environment alternatives for joins:
  TRELLIS_JOIN_TOKEN            Node join token from 'trellisctl nodes join-token create'
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
    if [ -n "$script_dir" ] && [ -f "$script_dir/install-core.sh" ] && [ -f "$script_dir/common.sh" ]; then
        cp "$script_dir/install-core.sh" "$TMP/install-core.sh"
        cp "$script_dir/common.sh" "$TMP/common-real.sh"
    else
        command -v curl >/dev/null 2>&1 || { echo "error: curl is required" >&2; exit 1; }
        curl -fsSL "$RAW_BASE/install-core.sh" -o "$TMP/install-core.sh"
        curl -fsSL "$RAW_BASE/common.sh" -o "$TMP/common-real.sh"
    fi
    cat >"$TMP/common.sh" <<'SHIM'
source "$(dirname "${BASH_SOURCE[0]}")/common-real.sh"
if [ "${TRELLIS_INSTALL_PLAN_CONFIRMED:-}" = 1 ]; then
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

advertise=""; join=""; join_token_file=""; ca_file=""; key_file=""; key_id=""
gvisor=true; assume_yes=false; control_plane=true; runs_workloads=true
while [ "$#" -gt 0 ]; do
    case "$1" in
        --advertise) [ "$#" -ge 2 ] || ui_die "--advertise requires a value"; advertise="$2"; shift 2 ;;
        --join) [ "$#" -ge 2 ] || ui_die "--join requires HOST:8128"; join="$2"; shift 2 ;;
        --join-token-file) [ "$#" -ge 2 ] || ui_die "--join-token-file requires a path"; join_token_file="$2"; shift 2 ;;
        --ca-cert-file) [ "$#" -ge 2 ] || ui_die "--ca-cert-file requires a path"; ca_file="$2"; shift 2 ;;
        --control-plane) [ "$#" -ge 2 ] || ui_die "--control-plane requires true or false"; control_plane="$2"; shift 2 ;;
        --worker) control_plane=false; shift ;;
        --runs-workloads) [ "$#" -ge 2 ] || ui_die "--runs-workloads requires true or false"; runs_workloads="$2"; shift 2 ;;
        --secrets-key-file) [ "$#" -ge 2 ] || ui_die "--secrets-key-file requires a path"; key_file="$2"; shift 2 ;;
        --secrets-key-id) [ "$#" -ge 2 ] || ui_die "--secrets-key-id requires a value"; key_id="$2"; shift 2 ;;
        --with-gvisor) gvisor=true; shift ;;
        --without-gvisor) gvisor=false; shift ;;
        -y|--yes) assume_yes=true; shift ;;
        *) ui_die "Unknown option: $1" ;;
    esac
done
case "$control_plane" in true|false) ;; *) ui_die "--control-plane requires true or false" ;; esac
case "$runs_workloads" in true|false) ;; *) ui_die "--runs-workloads requires true or false" ;; esac

load_install_state
if [ "$GVISOR_ENABLED" = true ]; then gvisor=true; fi

# Complete installs should retain the engine's fast already-installed path.
if { [ "$STATE_COMPLETE" = true ] && [ -x "$INSTALL_DIR/trellis" ] && [ -f "$CONFIG_FILE" ]; } || \
   { [ ! -f "$STATE_FILE" ] && [ -x "$INSTALL_DIR/trellis" ] && [ -f "$CONFIG_FILE" ] && [ -f "$SERVICE_FILE" ]; }; then
    bash "$TMP/install-core.sh" --yes
    exit $?
fi

existing_config=false
if [ -f "$CONFIG_FILE" ]; then
    existing_config=true
    configured="$(awk -F': ' '$1 == "agent_advertise" {sub(/:8127$/, "", $2); print $2; exit}' "$CONFIG_FILE")"
    configured_join="$(awk -F': ' '$1 == "join" {print $2; exit}' "$CONFIG_FILE")"
    configured_control_plane="$(awk -F': ' '$1 == "control_plane" {print $2; exit}' "$CONFIG_FILE")"
    configured_runs_workloads="$(awk -F': ' '$1 == "runs_workloads" {print $2; exit}' "$CONFIG_FILE")"
    [ -z "$configured" ] || advertise="$configured"
    [ -z "$configured_join" ] || join="$configured_join"
    control_plane="${configured_control_plane:-true}"
    runs_workloads="${configured_runs_workloads:-true}"
fi
[ -n "$advertise" ] || advertise="$(detect_advertise_ipv4 2>/dev/null || true)"
[ -n "$advertise" ] || ui_die "Could not determine a routable IPv4 advertise address. Pass --advertise HOST explicitly."
[ -z "$join" ] || [[ "$join" == *:* ]] || ui_die "Join address must look like node-a:8128"
fetch_latest_release

cluster_label() { [ -n "$join" ] && printf 'join %s' "$join" || printf 'create a new cluster'; }

show_plan() {
    ui_section "Plan"
    ui_detail "Version       $RELEASE_TAG"
    ui_detail "Node address  $advertise"
    ui_detail "Cluster       $(cluster_label)"
    ui_detail "Control plane $control_plane"
    ui_detail "Runs workloads $runs_workloads"
    ui_detail "gVisor        $([ "$gvisor" = true ] && printf installed || printf 'not installed')"
}

customize() {
    local choice value
    while true; do
        printf '\n'; ui_section "Customize installation"
        ui_detail "1. Cluster              $(cluster_label)"
        ui_detail "2. Node address         $advertise"
        ui_detail "3. Runtime sandbox      $([ "$gvisor" = true ] && printf 'gVisor installed' || printf 'gVisor not installed')"
        ui_detail "4. Control plane        $control_plane"
        ui_detail "5. Runs workloads       $runs_workloads"
        printf '\nSelect a setting to change, or press Enter when done: '
        read -r choice </dev/tty
        case "$choice" in
            "") return ;;
            1)
                [ "$existing_config" = false ] || { ui_warn "Cluster mode is fixed while resuming installation."; continue; }
                printf 'New cluster [1] or join existing [2] [1]: '; read -r value </dev/tty
                if [ "${value:-1}" = 2 ]; then
                    printf 'Existing node (HOST:8128): '; read -r join </dev/tty
                    [[ "$join" == *:* ]] || { join=""; ui_warn "Enter an address such as node-a:8128."; }
                else join=""; fi
                ;;
            2)
                [ "$existing_config" = false ] || { ui_warn "Node address is fixed while resuming installation."; continue; }
                printf 'Node address [%s]: ' "$advertise"; read -r value </dev/tty; [ -z "$value" ] || advertise="$value"
                ;;
            3) [ "$GVISOR_ENABLED" != true ] || { ui_warn "gVisor was already installed and will be kept."; continue; }; [ "$gvisor" = true ] && gvisor=false || gvisor=true ;;
            4)
                [ "$existing_config" = false ] || { ui_warn "Node role is fixed while resuming installation."; continue; }
                [ "$control_plane" = true ] && control_plane=false || control_plane=true
                ;;
            5)
                [ "$existing_config" = false ] || { ui_warn "Workload eligibility is fixed while resuming installation."; continue; }
                [ "$runs_workloads" = true ] && runs_workloads=false || runs_workloads=true
                ;;
            *) ui_warn "Choose 1-5, or press Enter when done." ;;
        esac
    done
}

confirmed=false
if [ "$assume_yes" = false ]; then
    ui_title "install"
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
args+=(--control-plane "$control_plane" --runs-workloads "$runs_workloads")
[ -z "$join" ] || args+=(--join "$join")
[ -z "$join_token_file" ] || args+=(--join-token-file "$join_token_file")
[ -z "$ca_file" ] || args+=(--ca-cert-file "$ca_file")
[ -z "$key_file" ] || args+=(--secrets-key-file "$key_file")
[ -z "$key_id" ] || args+=(--secrets-key-id "$key_id")
[ "$gvisor" = true ] && args+=(--with-gvisor)

if [ "$confirmed" = true ]; then
    TRELLIS_INSTALL_PLAN_CONFIRMED=1 bash "$TMP/install-core.sh" "${args[@]}"
else
    bash "$TMP/install-core.sh" "${args[@]}"
fi
