#!/usr/bin/env bash
set -euo pipefail

SHARE_DIR="/vagrant/bin"
DATA_DIR="/var/lib/trellis/data"
CONFIG_FILE="/etc/trellis/trellis.yaml"
TOKEN_FILE="${SHARE_DIR}/token"

# Generate a shared bootstrap credential on the first node to write it.
mkdir -p "${SHARE_DIR}"
if [ ! -s "${TOKEN_FILE}" ]; then
    umask 077
    printf 'trls_boot_' > "${TOKEN_FILE}"
    head -c 32 /dev/urandom | base64 | tr -d '=\n' >> "${TOKEN_FILE}"
fi

# Install binaries from the shared folder.
# Build them first on the host: cd orchestrator && CGO_ENABLED=0 go build -o bin/trellis ./cmd/trellis && go build -o bin/trellisctl ./cmd/trellisctl
install -m 0755 "${SHARE_DIR}/trellis"      /usr/local/bin/trellis
install -m 0755 "${SHARE_DIR}/trellisctl"   /usr/local/bin/trellisctl

mkdir -p "${DATA_DIR}" /etc/trellis

HOSTNAME=$(hostname -s)
ADVERTISE_HOST="${HOSTNAME}.local"
cat > "$CONFIG_FILE" <<EOF
cluster: default
bootstrap_token: $(cat "${TOKEN_FILE}")
data_dir: ${DATA_DIR}
agent_advertise: ${ADVERTISE_HOST}:8127
server_advertise: ${ADVERTISE_HOST}:8128
raft_advertise: ${ADVERTISE_HOST}:8129
EOF
if [ "${HOSTNAME}" != "control" ]; then
    printf 'join: control.local:8128\n' >> "$CONFIG_FILE"
fi
chmod 600 "$CONFIG_FILE"

cat > /etc/systemd/system/trellis.service <<EOF
[Unit]
Description=Trellis node
After=containerd.service network-online.target avahi-daemon.service
Wants=containerd.service network-online.target avahi-daemon.service

[Service]
ExecStart=/usr/local/bin/trellis --config ${CONFIG_FILE}
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable trellis
systemctl start trellis
