#!/usr/bin/env bash
set -euo pipefail

RAW_COMMON="https://raw.githubusercontent.com/overfold/trellis/main/scripts/common.sh"
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

uninstall_ctl() {
    env -u TRELLIS_TOKEN TRELLIS_CONFIG="$operator_config" \
        "${INSTALL_DIR}/trellisctl" --context local --server-addr https://127.0.0.1:8128 \
        --ca-cert "${RUN_DIR}/ca.crt" --cert= --key= "$@"
}

usage() {
    cat <<'EOF_USAGE'
Remove Trellis from this node.

Usage:
  uninstall.sh [--force] [--purge] [-y|--yes]

By default, Trellis gracefully removes this machine from a multi-node cluster
and archives its config, secrets key, and local state together under
/var/lib/trellis/recovery/. This keeps the data recoverable while removing the
active installation.

Options:
  --force       Skip cluster operations; stop local workloads without evacuation
                Membership is left unchanged; data is still archived unless --purge
  --purge       Permanently delete Trellis config, keys, data, and recovery archives
  -y, --yes     Skip the single confirmation prompt
  -h, --help    Show this help

Graceful removal uses the invoking user's saved local cluster/write context
(or TRELLIS_CONFIG). For multi-node removal, supply TRELLIS_ADMINISTRATOR_KEY
transiently (a private-key file path or base64 PKCS#8 key); it is never saved.
EOF_USAGE
}

purge=false
force=false
assume_yes=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --force) force=true; shift ;;
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
if [ "$force" = true ]; then
    ui_warn "Cluster   --force skips draining and membership removal; local workloads will stop without evacuation and cluster quorum may be affected"
else
    ui_detail "Cluster   drain and remove this node when other members exist"
fi
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

if [ "$force" = true ]; then
    ui_section "Cluster"
    ui_warn "Skipping cluster operations because --force was specified. Cluster membership is unchanged."
    if [ -n "$node_id" ]; then
        ui_detail "Afterward, remove node ${node_id} from a healthy operator context if other cluster members remain."
    fi
elif [ "$was_running" = true ] && [ -x "${INSTALL_DIR}/trellisctl" ] && [ -n "$node_id" ]; then
    ui_section "Cluster"
    operator_config="${TRELLIS_CONFIG:-}"
    if [ -z "$operator_config" ]; then
        operator_home="$(getent passwd "${SUDO_USER:-root}" | cut -d: -f6 || true)"
        [ -n "$operator_home" ] || ui_die "Could not determine the invoking user's home directory; provide TRELLIS_CONFIG."
        operator_config="${operator_home}/.config/trellis/config.yaml"
    fi
    [ -f "$operator_config" ] || ui_die "Operator config missing at ${operator_config}; provide TRELLIS_CONFIG with a local cluster/write context. Nothing local has been deleted."
    if ! node_json="$(uninstall_ctl nodes list --output json)"; then
        ui_die "Could not inspect cluster membership; check the local context's operator credential and pinned CA. Nothing local has been deleted. To uninstall without draining or changing membership, rerun with --force."
    fi
    if ! node_count="$(printf '%s' "$node_json" | count_nodes_json)"; then
        ui_die "Invalid cluster membership output. Nothing local has been deleted. Use --force only to skip cluster operations explicitly."
    fi
    if [ "${node_count:-0}" -gt 1 ]; then
        [ -n "${TRELLIS_ADMINISTRATOR_KEY:-}" ] || ui_die "Multi-node removal requires explicit TRELLIS_ADMINISTRATOR_KEY (private-key file path or base64 PKCS#8 key). Supply it transiently alongside the local operator context; nothing has been drained or deleted."
        uninstall_ctl nodes drain "$node_id" >/dev/null || ui_die "Could not drain this node; check the local context has cluster/write authority. Nothing local has been deleted."
        ui_step "Drain started"
        if ! wait_for_local_allocations_to_stop; then
            uninstall_ctl nodes undrain "$node_id" >/dev/null 2>&1 || true
            ui_die "Timed out waiting for allocations to move. The node was undrained and uninstall stopped before deleting anything. To uninstall without evacuation, rerun with --force."
        fi
        ui_step "Allocations moved to healthy replacements"
        # A non-leader need not transfer; removal below reports any authority
        # failure and refuses local deletion. Do not hide the CLI diagnostic.
        uninstall_ctl nodes transfer-leadership >/dev/null || true
        removed=false
        for _ in $(seq 1 20); do
            if uninstall_ctl nodes remove "$node_id" >/dev/null; then
                removed=true
                break
            fi
            sleep 1
        done
        [ "$removed" = true ] || ui_die "Could not remove the node from cluster membership; check TRELLIS_ADMINISTRATOR_KEY and cluster quorum. The node remains drained; use 'trellisctl nodes undrain' if abandoning removal. Nothing local has been deleted. To uninstall without changing membership, rerun with --force."
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
if ! systemctl stop trellis; then
    ui_die "Could not stop the Trellis service. Network state and installed files were retained for retry."
fi

if command -v ctr >/dev/null 2>&1; then
    if ! tasks="$(ctr -n trellis tasks ls -q)"; then
        ui_die "Could not inspect Trellis tasks. The containerd error is shown above; installed files and node data were retained for retry."
    fi
    for tid in $tasks; do
        # --force registers an exit waiter, kills all processes, waits for exit,
        # and only then deletes the task. A separate kill/delete races shutdown.
        if ! ctr -n trellis tasks delete --force "$tid"; then
            ui_die "Could not stop and delete Trellis task ${tid}. The containerd error is shown above; installed files and node data were retained for retry."
        fi
    done
    if ! containers="$(ctr -n trellis containers ls -q)"; then
        ui_die "Could not inspect Trellis containers. Network state and installed files were retained for retry."
    fi
    for cid in $containers; do
        if ! ctr -n trellis containers rm "$cid"; then
            ui_die "Could not remove Trellis container ${cid}. The containerd error is shown above; installed files and node data were retained for retry."
        fi
    done
    if ! remaining="$(ctr -n trellis containers ls -q)"; then
        ui_die "Could not verify Trellis container removal. Network state and installed files were retained for retry."
    fi
    if [ -n "$remaining" ]; then
        ui_warn "Remaining containers: ${remaining}"
        ui_die "Could not remove all Trellis containers. Network state and installed files were retained for retry."
    fi
else
    ui_die "ctr is required to verify workloads are stopped before network cleanup. Installed files and state were retained for retry."
fi

cleanup_args=(--data-dir "$DATA_DIR")
if [ -f "$CONFIG_FILE" ]; then
    cleanup_args+=(--config "$CONFIG_FILE")
fi
if ! "${INSTALL_DIR}/trellis" local-cleanup "${cleanup_args[@]}"; then
    ui_die "Could not remove Trellis network resources, volume staging mounts, or delivered secrets. The cleanup error is shown above; node data and installed files were retained for retry."
fi
ui_step "Removed journaled Trellis network resources, volume staging mounts, and delivered secrets"

if [ "$purge" = true ]; then
    ui_section "Data"
    if [ -n "$DATA_DIR" ] && [ "$DATA_DIR" != "/" ]; then
        if ! rm -rf "$DATA_DIR"; then
            ui_die "Could not purge node data at ${DATA_DIR}. Data may be partially deleted; the binary, service, configuration, and dependencies were retained. Fix the error above and rerun uninstall."
        fi
    fi
    if [ -n "$SECRETS_KEY_FILE" ] && [ "$SECRETS_KEY_FILE" != "/" ]; then rm -f "$SECRETS_KEY_FILE"; fi
    if ! rm -rf "$STATE_ROOT" "$CONFIG_DIR"; then
        ui_die "Could not finish purging Trellis state and configuration. The binary, service, and dependencies were retained for retry; data may be partially deleted."
    fi
    ui_step "Permanently removed Trellis node data"
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
fi

ui_section "Software"
remove_owned_dependencies
systemctl disable trellis >/dev/null 2>&1 || true
rm -f "$SERVICE_FILE"
systemctl daemon-reload
systemctl reset-failed >/dev/null 2>&1 || true
rm -f "${INSTALL_DIR}/trellis" "${INSTALL_DIR}/trellisctl" "${INSTALL_DIR}/trellis-health-probe"
rm -rf "$RUN_DIR"
ui_step "Removed Trellis service and binaries"

if [ "$purge" = true ]; then
    ui_done "Trellis was purged from this node"
else
    ui_done "Trellis was removed; node data was preserved"
    ui_detail "Recovery  ${recovery_dir}"
fi
