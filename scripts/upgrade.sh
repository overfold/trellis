#!/usr/bin/env bash
set -euo pipefail

RAW_COMMON="https://raw.githubusercontent.com/clofour/trellis/main/scripts/common.sh"
COMMON_TMP=""
WORK_TMP=""
ROLLBACK_NEEDED=false
drained=false
node_id=""
had_health_probe=false

cleanup() {
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        if [ "$ROLLBACK_NEEDED" = true ]; then
            rollback || true
        elif [ "$drained" = true ] && [ -n "$WORK_TMP" ] && [ -n "$node_id" ]; then
            local_ctl "$WORK_TMP" nodes undrain "$node_id" >/dev/null 2>&1 || true
        fi
    fi
    [ -z "$WORK_TMP" ] || rm -rf "$WORK_TMP"
    [ -z "$COMMON_TMP" ] || rm -rf "$COMMON_TMP"
    return "$rc"
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
Upgrade an installed Trellis node to the latest release.

Usage:
  upgrade.sh

The upgrade stages and verifies the release first. On multi-node clusters it
then drains this node, waits for its allocations to move, updates the binaries
and systemd unit, verifies the daemon, refreshes an installer-managed dashboard,
and undrains the node. A failed daemon upgrade rolls back automatically.
EOF_USAGE
}
[ "${1:-}" != "-h" ] && [ "${1:-}" != "--help" ] || { usage; exit 0; }
[ "$#" -eq 0 ] || ui_die "upgrade.sh does not take options"

require_root_linux_amd64
require_commands curl tar systemctl install mktemp ctr
[ -x "${INSTALL_DIR}/trellis" ] || ui_die "Trellis is not installed at ${INSTALL_DIR}/trellis."
[ -x "${INSTALL_DIR}/trellisctl" ] || ui_die "trellisctl is not installed at ${INSTALL_DIR}/trellisctl."
[ ! -e "${INSTALL_DIR}/trellis-health-probe" ] || had_health_probe=true
[ -f "$CONFIG_FILE" ] || ui_die "Node configuration is missing at ${CONFIG_FILE}."
load_node_config_paths

load_install_state
current_version="$("${INSTALL_DIR}/trellis" --version 2>/dev/null | awk '{print $NF}' || true)"
fetch_latest_release

ui_title "upgrade"
if [ "$current_version" = "$RELEASE_TAG" ]; then
    ui_step "Already on ${RELEASE_TAG}"
    exit 0
fi
ui_section "Plan"
ui_detail "Version  ${current_version:-unknown} → ${RELEASE_TAG}"
ui_detail "Safety   stage → drain → swap → verify → undrain"
if [ "$DASHBOARD_INSTALLED" = true ]; then ui_detail "Dashboard  refresh to ${RELEASE_TAG}"; fi

WORK_TMP="$(mktemp -d)"
ui_section "Stage"
download_release "$WORK_TMP"
ui_step "Downloaded and verified ${RELEASE_TAG}"

was_running=false
if systemctl is-active --quiet trellis; then was_running=true; fi
if [ "$was_running" = true ] && [ -f "${DATA_DIR}/node-id" ]; then
    node_id="$(tr -d '[:space:]' <"${DATA_DIR}/node-id")"
fi

if [ "$was_running" = true ] && [ -n "$node_id" ]; then
    ui_section "Drain"
    if ! node_json="$(local_ctl "$WORK_TMP" nodes list --output json 2>/dev/null)"; then
        ui_die "Could not inspect cluster membership; no binaries were changed."
    fi
    node_count="$(printf '%s' "$node_json" | grep -c '"id"' || true)"
    if [ "${node_count:-0}" -gt 1 ]; then
        local_ctl "$WORK_TMP" nodes drain "$node_id" >/dev/null
        drained=true
        ui_step "Drain started"
        if ! wait_for_local_allocations_to_stop; then
            local_ctl "$WORK_TMP" nodes undrain "$node_id" >/dev/null 2>&1 || true
            ui_die "Timed out waiting for allocations to move; the node was undrained and no binaries were changed."
        fi
        ui_step "Allocations moved to healthy replacements"
    else
        ui_detail "Single-node cluster; no evacuation target, skipping drain."
    fi
fi

cp -a "${INSTALL_DIR}/trellis" "${WORK_TMP}/trellis.old"
cp -a "${INSTALL_DIR}/trellisctl" "${WORK_TMP}/trellisctl.old"
if [ "$had_health_probe" = true ]; then cp -a "${INSTALL_DIR}/trellis-health-probe" "${WORK_TMP}/trellis-health-probe.old"; fi
[ ! -f "$SERVICE_FILE" ] || cp -a "$SERVICE_FILE" "${WORK_TMP}/trellis.service.old"

rollback() {
    set +e
    ROLLBACK_NEEDED=false
    ui_warn "Upgrade failed after changing binaries; restoring ${current_version:-the previous version}."
    systemctl stop trellis >/dev/null 2>&1 || true
    install -m 0755 "${WORK_TMP}/trellis.old" "${INSTALL_DIR}/trellis"
    install -m 0755 "${WORK_TMP}/trellisctl.old" "${INSTALL_DIR}/trellisctl"
    if [ "$had_health_probe" = true ]; then
        install -m 0755 "${WORK_TMP}/trellis-health-probe.old" "${INSTALL_DIR}/trellis-health-probe"
    else
        rm -f "${INSTALL_DIR}/trellis-health-probe"
    fi
    if [ -f "${WORK_TMP}/trellis.service.old" ]; then cp -a "${WORK_TMP}/trellis.service.old" "$SERVICE_FILE"; else rm -f "$SERVICE_FILE"; fi
    systemctl daemon-reload
    if [ "$was_running" = true ]; then systemctl start trellis >/dev/null 2>&1 || true; fi
    if [ "$drained" = true ]; then
        for _ in $(seq 1 20); do
            if local_ctl "$WORK_TMP" nodes undrain "$node_id" >/dev/null 2>&1; then break; fi
            sleep 1
        done
    fi
}

ui_section "Install"
ROLLBACK_NEEDED=true
if [ "$was_running" = true ]; then systemctl stop trellis; fi
install -m 0755 "${WORK_TMP}/trellis" "${INSTALL_DIR}/.trellis.new"
install -m 0755 "${WORK_TMP}/trellisctl" "${INSTALL_DIR}/.trellisctl.new"
install -m 0755 "${WORK_TMP}/trellis-health-probe" "${INSTALL_DIR}/.trellis-health-probe.new"
mv "${INSTALL_DIR}/.trellis.new" "${INSTALL_DIR}/trellis"
mv "${INSTALL_DIR}/.trellisctl.new" "${INSTALL_DIR}/trellisctl"
mv "${INSTALL_DIR}/.trellis-health-probe.new" "${INSTALL_DIR}/trellis-health-probe"
write_service
chmod 600 "$CONFIG_FILE"
[ ! -f "$SECRETS_KEY_FILE" ] || chmod 600 "$SECRETS_KEY_FILE"
ui_step "Installed binaries and refreshed the systemd unit"

if [ "$was_running" = true ]; then
    systemctl start trellis
    if ! wait_for_service "$WORK_TMP"; then
        journalctl -u trellis -n 20 --no-pager >&2 || true
        ui_die "New Trellis version did not become healthy; rolling back."
    fi
    ui_step "Trellis ${RELEASE_TAG} is healthy"
    ROLLBACK_NEEDED=false
else
    ROLLBACK_NEEDED=false
    ui_detail "Service was stopped before the upgrade; leaving it stopped."
fi

if [ "$DASHBOARD_INSTALLED" = true ] && [ "$was_running" = true ]; then
    ui_section "Dashboard"
    if deploy_dashboard "$WORK_TMP" "$RELEASE_TAG" "${DASHBOARD_NAMESPACE:-default}" "${DASHBOARD_ACCESS_STATE:-read}"; then
        ui_step "Dashboard refreshed to ${RELEASE_TAG}"
    else
        ui_warn "Core upgrade succeeded, but the dashboard could not be refreshed. Reapply it after checking cluster health."
    fi
fi

if [ "$drained" = true ]; then
    ui_section "Resume"
    local_ctl "$WORK_TMP" nodes undrain "$node_id" >/dev/null
    ui_step "Node is schedulable again"
fi
write_state_version "$RELEASE_TAG"

ui_done "Upgrade complete: ${current_version:-unknown} → ${RELEASE_TAG}"
