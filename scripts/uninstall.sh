#!/usr/bin/env bash
set -euo pipefail

RAW_COMMON="https://raw.githubusercontent.com/overfold/trellis/main/scripts/common.sh"
COMMON_TMP=""
WORK_TMP=""
cleanup() { local rc=$?; [ -z "$WORK_TMP" ] || rm -rf "$WORK_TMP"; [ -z "$COMMON_TMP" ] || rm -rf "$COMMON_TMP"; return "$rc"; }
trap cleanup EXIT

load_common() {
    local script_dir
    script_dir=""
    if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
        script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    fi
    if [ -n "$script_dir" ] && [ -f "${script_dir}/common.sh" ]; then
        # shellcheck source=common.sh
        source "${script_dir}/common.sh"
        return
    fi
    command -v curl >/dev/null 2>&1 || { echo "error: curl is required" >&2; exit 1; }
    COMMON_TMP="$(mktemp -d)"
    curl --proto '=https' --proto-redir '=https' -fsSL "$RAW_COMMON" -o "${COMMON_TMP}/common.sh"
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

Graceful multi-node removal refuses this node if it is the leader. Transfer
leadership explicitly and verify the new leader before rerunning uninstall.

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
[ "$STATE_COMPLETE" != true ] || [ -f "$CONFIG_FILE" ] ||
    ui_die "Completed installation configuration is missing; restore its trusted copy before uninstall. No local files were deleted."
load_node_config_paths
# Recovery must be outside the source tree, and purge must not erase active
# installer ownership through the data path before dependency cleanup succeeds.
data_path="$(realpath -m -- "$DATA_DIR")"
state_path="$(realpath -m -- "$STATE_ROOT")"
case "${state_path}/" in
    "${data_path}/"*) ui_die "Node data contains installer state; cannot archive or purge it safely. No local files were deleted." ;;
esac
[ ! -e "$DATA_DIR" ] || [ -d "$DATA_DIR" ] || ui_die "Configured data_dir is not a directory. No local files were deleted."
[ ! -e "$SECRETS_KEY_FILE" ] || [ -f "$SECRETS_KEY_FILE" ] || ui_die "Configured secrets_key is not a file. No local files were deleted."

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
if [ "$was_running" = true ]; then
    [ -f "$CONFIG_FILE" ] || ui_die "Running node configuration is missing; restore it before uninstall. Nothing local has been deleted."
    [ -n "$node_id" ] || ui_die "Running node has no readable, nonempty node-id in ${DATA_DIR}; cannot uninstall safely. Nothing local has been deleted."
    [ "$force" = true ] || [ -x "${INSTALL_DIR}/trellisctl" ] || ui_die "trellisctl is required for graceful removal of a running node. Nothing local has been deleted."
fi

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
        if ! leader_json="$(uninstall_ctl nodes leader --output json)"; then
            ui_die "Could not identify the cluster leader. Nothing has been drained or deleted; verify cluster availability and retry."
        fi
        if ! leader_id="$(printf '%s' "$leader_json" | jq -ers 'select(length == 1) | .[0].leader_id |
            select(type == "string" and length > 0 and . != "00000000-0000-0000-0000-000000000000")')"; then
            ui_die "Invalid cluster leader output. Nothing has been drained or deleted."
        fi
        [ "$leader_id" != "$node_id" ] || ui_die "Node ${node_id} is the cluster leader. Transfer leadership with 'trellisctl nodes transfer-leadership' using the administrator credential, verify the new leader with 'trellisctl nodes leader', then rerun uninstall. Nothing has been drained or deleted."
        [ -n "${TRELLIS_ADMINISTRATOR_KEY:-}" ] || ui_die "Multi-node removal requires explicit TRELLIS_ADMINISTRATOR_KEY (private-key file path or base64 PKCS#8 key). Supply it transiently alongside the local operator context; nothing has been drained or deleted."
        uninstall_ctl nodes drain "$node_id" >/dev/null || ui_die "Could not drain this node; check the local context has cluster/write authority. Nothing local has been deleted."
        ui_step "Drain started"
        if ! wait_for_local_allocations_to_stop; then
            uninstall_ctl nodes undrain "$node_id" >/dev/null 2>&1 || true
            ui_die "Timed out waiting for allocations to move. The node was undrained and uninstall stopped before deleting anything. To uninstall without evacuation, rerun with --force."
        fi
        ui_step "Allocations moved to healthy replacements"
        # Leadership may change after preflight. Removal remains authoritative
        # and refuses a current leader or unsafe quorum; never transfer here.
        removed=false
        for _ in $(seq 1 20); do
            if uninstall_ctl nodes remove "$node_id" >/dev/null; then
                removed=true
                break
            fi
            sleep 1
        done
        [ "$removed" = true ] || ui_die "Could not remove the node from cluster membership; check TRELLIS_ADMINISTRATOR_KEY, cluster quorum, and 'trellisctl nodes leader'. If this node became leader, transfer leadership and retry. The node remains drained; use 'trellisctl nodes undrain' if abandoning removal. Nothing local has been deleted. To uninstall without changing membership, rerun with --force."
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
    if ! tasks="$(ctr --address "${CONTAINERD_SOCKET:-/run/containerd/containerd.sock}" -n trellis tasks ls -q)"; then
        ui_die "Could not inspect Trellis tasks. The containerd error is shown above; installed files and node data were retained for retry."
    fi
    for tid in $tasks; do
        # --force registers an exit waiter, kills all processes, waits for exit,
        # and only then deletes the task. A separate kill/delete races shutdown.
        if ! ctr --address "${CONTAINERD_SOCKET:-/run/containerd/containerd.sock}" -n trellis tasks delete --force "$tid"; then
            ui_die "Could not stop and delete Trellis task ${tid}. The containerd error is shown above; installed files and node data were retained for retry."
        fi
    done
    if ! containers="$(ctr --address "${CONTAINERD_SOCKET:-/run/containerd/containerd.sock}" -n trellis containers ls -q)"; then
        ui_die "Could not inspect Trellis containers. Network state and installed files were retained for retry."
    fi
    for cid in $containers; do
        if ! ctr --address "${CONTAINERD_SOCKET:-/run/containerd/containerd.sock}" -n trellis containers rm "$cid"; then
            ui_die "Could not remove Trellis container ${cid}. The containerd error is shown above; installed files and node data were retained for retry."
        fi
    done
    if ! remaining="$(ctr --address "${CONTAINERD_SOCKET:-/run/containerd/containerd.sock}" -n trellis containers ls -q)"; then
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

# Keep the active config, data and ownership record through fallible dependency
# cleanup. A retry must use recorded ownership, never infer it from host files.
ui_section "Dependencies"
remove_owned_dependencies

if [ "$purge" = true ]; then
    ui_section "Data"
    if [ -n "$DATA_DIR" ] && [ "$DATA_DIR" != "/" ]; then
        if ! rm -rf "$DATA_DIR"; then
            ui_die "Could not purge node data at ${DATA_DIR}. Data may be partially deleted; the binary, service, configuration, and ownership record were retained. Fix the error above and rerun uninstall."
        fi
    fi
    if [ -n "$SECRETS_KEY_FILE" ] && [ "$SECRETS_KEY_FILE" != "/" ]; then rm -f "$SECRETS_KEY_FILE"; fi
    # Purge recovery and other installer state without deleting active ownership
    # or the configuration needed to select the correct runtime on retry.
    (
        shopt -s nullglob dotglob
        for entry in "$STATE_ROOT"/*; do
            [ "$entry" = "$STATE_FILE" ] || rm -rf "$entry" || exit 1
        done
    )
    if ! rm -rf "$CONFIG_DIR"; then
        ui_die "Could not finish purging Trellis configuration. The binary, service, and ownership record were retained for retry; data may be partially deleted."
    fi
    ui_step "Permanently removed Trellis node data"
else
    ui_section "Recovery"
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    recovery_root="${STATE_ROOT}/recovery"
    install -d -m 0700 "$recovery_root"
    recovery_dir="$(mktemp -d "${recovery_root}/${stamp}.XXXXXX")"
    # Finish the whole recoverable set before deleting any source. In particular,
    # a failed key/config copy must not strand the only data copy in an archive.
    if ! (
        if [ -d "$DATA_DIR" ]; then cp -a "$DATA_DIR" "${recovery_dir}/data" || exit 1; fi
        if [ -f "$SECRETS_KEY_FILE" ]; then cp -a "$SECRETS_KEY_FILE" "${recovery_dir}/secrets.key" || exit 1; fi
        if [ -d "$CONFIG_DIR" ]; then cp -a "$CONFIG_DIR" "${recovery_dir}/config" || exit 1; fi
        if [ -f "$STATE_FILE" ]; then cp -a "$STATE_FILE" "${recovery_dir}/install-state" || exit 1; fi
    ); then
        rm -rf "$recovery_dir"
        ui_die "Could not archive recoverable node state. Active data, configuration, key, and ownership were retained for retry."
    fi
    rm -rf "$DATA_DIR" "$CONFIG_DIR"
    if [ -f "$SECRETS_KEY_FILE" ]; then rm -f "$SECRETS_KEY_FILE"; fi
    ui_step "Archived recoverable node state at ${recovery_dir}"
fi

ui_section "Software"
systemctl disable trellis >/dev/null 2>&1 || true
rm -f "$SERVICE_FILE"
systemctl daemon-reload
systemctl reset-failed >/dev/null 2>&1 || true
rm -f "${INSTALL_DIR}/trellis" "${INSTALL_DIR}/trellisctl" "${INSTALL_DIR}/trellis-health-probe"
rm -rf "$RUN_DIR"
rm -f "$STATE_FILE"
if [ "$purge" = true ] && [ -d "$STATE_ROOT" ]; then rmdir "$STATE_ROOT"; fi
ui_step "Removed Trellis service and binaries"

if [ "$purge" = true ]; then
    ui_done "Trellis was purged from this node"
else
    ui_done "Trellis was removed; node data was preserved"
    ui_detail "Recovery  ${recovery_dir}"
fi
