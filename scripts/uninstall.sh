#!/usr/bin/env bash
set -euo pipefail

RAW_COMMON="https://raw.githubusercontent.com/clofour/trellis/main/scripts/common.sh"
COMMON_TMP=""
WORK_TMP=""
cleanup() { local rc=$?; [ -z "$WORK_TMP" ] || rm -rf "$WORK_TMP"; [ -z "$COMMON_TMP" ] || rm -rf "$COMMON_TMP"; return "$rc"; }
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
Remove Trellis from this node.

Usage:
  uninstall.sh [--purge] [-y|--yes]

By default, Trellis gracefully removes this machine from a multi-node cluster
and archives its config, secrets key, and local state together under
/var/lib/trellis/recovery/. This keeps the data recoverable while removing the
active installation.

Options:
  --purge       Permanently delete Trellis config, keys, data, and recovery archives
  -y, --yes     Skip the single confirmation prompt
  -h, --help    Show this help
EOF_USAGE
}

purge=false
assume_yes=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --purge) purge=true; shift ;;
        -y|--yes) assume_yes=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) ui_die "Unknown option: $1" ;;
    esac
done

require_root_linux_amd64
if [ ! -x "${INSTALL_DIR}/trellis" ] && [ ! -f "$SERVICE_FILE" ] && [ ! -d "$CONFIG_DIR" ] && [ ! -d "$STATE_ROOT" ]; then
    ui_title "uninstall"
    ui_step "Trellis is not installed on this node"
    exit 0
fi
load_install_state
load_node_config_paths

ui_title "uninstall"
ui_section "Plan"
ui_detail "Cluster   drain and remove this node when other members exist"
ui_detail "Software  remove Trellis binaries, service, runtime files, and only dependencies recorded as Trellis-owned"
ui_detail "CLI       keep user trellisctl contexts (they belong to the cluster, not this machine)"
if [ "$purge" = true ]; then
    ui_detail "Data      permanently delete config, secrets key, state, volumes, and recovery archives"
else
    ui_detail "Data      archive config + secrets key + local state together under ${STATE_ROOT}/recovery"
fi

if [ "$assume_yes" != true ]; then
    if [ "$purge" = true ]; then
        printf '\n%sPermanently purge this node? [y/N] %s' "$BOLD" "$RESET"
        default_answer=n
    else
        printf '\n%sRemove Trellis from this node? [Y/n] %s' "$BOLD" "$RESET"
        default_answer=y
    fi
    read -r answer </dev/tty
    answer="${answer:-$default_answer}"
    case "$answer" in [Yy]*) ;; *) ui_detail "No changes made."; exit 0 ;; esac
fi

WORK_TMP="$(mktemp -d)"
node_id=""
[ ! -f "${DATA_DIR}/node-id" ] || node_id="$(tr -d '[:space:]' <"${DATA_DIR}/node-id")"
was_running=false
systemctl is-active --quiet trellis 2>/dev/null && was_running=true

if [ "$was_running" = true ] && [ -x "${INSTALL_DIR}/trellisctl" ] && [ -n "$node_id" ]; then
    ui_section "Cluster"
    if ! node_json="$(local_ctl "$WORK_TMP" nodes list --output json 2>/dev/null)"; then
        ui_die "Could not inspect cluster membership. Nothing local has been deleted."
    fi
    node_count="$(printf '%s' "$node_json" | grep -c '"id"' || true)"
    if [ "${node_count:-0}" -gt 1 ]; then
        local_ctl "$WORK_TMP" nodes drain "$node_id" >/dev/null
        ui_step "Drain started"
        if ! wait_for_local_allocations_to_stop; then
            local_ctl "$WORK_TMP" nodes undrain "$node_id" >/dev/null 2>&1 || true
            ui_die "Timed out waiting for allocations to move. The node was undrained and uninstall stopped before deleting anything."
        fi
        ui_step "Allocations moved to healthy replacements"
        local_ctl "$WORK_TMP" nodes transfer-leadership >/dev/null 2>&1 || true
        removed=false
        for _ in $(seq 1 20); do
            if local_ctl "$WORK_TMP" nodes remove "$node_id" >/dev/null 2>&1; then
                removed=true
                break
            fi
            sleep 1
        done
        [ "$removed" = true ] || ui_die "Could not remove the node from cluster membership. Nothing local has been deleted."
        ui_step "Removed node from cluster membership"
    else
        ui_detail "Single-node cluster; there is no remaining member to remove this node from."
    fi
else
    ui_section "Cluster"
    ui_warn "The local daemon is unavailable, so cluster membership cannot be changed from this machine."
    if [ -n "$node_id" ]; then
        ui_detail "Afterward, verify from another operator context that node ${node_id} is no longer a member."
    fi
fi

ui_section "Software"
systemctl stop trellis >/dev/null 2>&1 || true
systemctl disable trellis >/dev/null 2>&1 || true

if command -v ctr >/dev/null 2>&1; then
    for cid in $(ctr -n trellis containers ls -q 2>/dev/null || true); do
        ctr -n trellis tasks kill "$cid" -s SIGKILL >/dev/null 2>&1 || true
        ctr -n trellis tasks delete "$cid" >/dev/null 2>&1 || true
        ctr -n trellis containers rm "$cid" >/dev/null 2>&1 || true
    done
fi
rm -f "$SERVICE_FILE"
systemctl daemon-reload
systemctl reset-failed >/dev/null 2>&1 || true
rm -f "${INSTALL_DIR}/trellis" "${INSTALL_DIR}/trellisctl" "${INSTALL_DIR}/trellis-health-probe"
rm -rf "$RUN_DIR"
ui_step "Removed Trellis service and binaries"
remove_owned_dependencies

if [ "$purge" = true ]; then
    ui_section "Data"
    if [ -n "$DATA_DIR" ] && [ "$DATA_DIR" != "/" ]; then rm -rf "$DATA_DIR"; fi
    if [ -n "$SECRETS_KEY_FILE" ] && [ "$SECRETS_KEY_FILE" != "/" ]; then rm -f "$SECRETS_KEY_FILE"; fi
    rm -rf "$CONFIG_DIR" "$STATE_ROOT"
    ui_step "Permanently removed Trellis node data"
    ui_done "Trellis was purged from this node"
else
    ui_section "Recovery"
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    recovery_root="${STATE_ROOT}/recovery"
    recovery_dir="${recovery_root}/${stamp}"
    # Avoid placing the recovery directory inside the source tree before moving data.
    data_tmp="${STATE_ROOT}/.data-recovery-${stamp}"
    if [ -d "$DATA_DIR" ]; then mv "$DATA_DIR" "$data_tmp"; fi
    install -d -m 0700 "$recovery_dir"
    if [ -d "$data_tmp" ]; then mv "$data_tmp" "${recovery_dir}/data"; fi
    if [ -f "$SECRETS_KEY_FILE" ]; then cp -a "$SECRETS_KEY_FILE" "${recovery_dir}/secrets.key"; fi
    if [ -d "$CONFIG_DIR" ]; then cp -a "$CONFIG_DIR" "${recovery_dir}/config"; rm -rf "$CONFIG_DIR"; fi
    if [ -f "$SECRETS_KEY_FILE" ]; then rm -f "$SECRETS_KEY_FILE"; fi
    if [ -f "$STATE_FILE" ]; then cp -a "$STATE_FILE" "${recovery_dir}/install-state"; fi
    rm -f "$STATE_FILE"
    ui_step "Archived recoverable node state at ${recovery_dir}"
    ui_done "Trellis was removed; node data was preserved"
    ui_detail "Recovery  ${recovery_dir}"
fi
