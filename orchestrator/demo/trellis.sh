#!/usr/bin/env bash
set -euo pipefail

SHARE_DIR="/vagrant/bin"
DATA_DIR="/var/lib/trellis/data"
CONFIG_FILE="/etc/trellis/trellis.yaml"
TOKEN_FILE="${SHARE_DIR}/token"
ENROLLMENT_TOKEN_FILE="${SHARE_DIR}/enrollment-token"
CA_CERT_FILE="${SHARE_DIR}/node-ca.crt"

# Generate separate shared administrator and enrollment credentials.
mkdir -p "${SHARE_DIR}"
if [ ! -s "${TOKEN_FILE}" ]; then
    umask 077
    printf 'trls_admin_' > "${TOKEN_FILE}"
    head -c 32 /dev/urandom | base64 | tr -d '=\n' >> "${TOKEN_FILE}"
fi
if [ ! -s "${ENROLLMENT_TOKEN_FILE}" ]; then
    umask 077
    printf 'trls_enroll_' > "${ENROLLMENT_TOKEN_FILE}"
    head -c 32 /dev/urandom | base64 | tr -d '=\n' >> "${ENROLLMENT_TOKEN_FILE}"
fi

# Install binaries from the shared folder.
# Build them first on the host: cd orchestrator && go build -o bin/trellis ./cmd/trellis && go build -o bin/trellisctl ./cmd/trellisctl && CGO_ENABLED=0 go build -o bin/trellis-health-probe ./cmd/trellis-health-probe
install -m 0755 "${SHARE_DIR}/trellis"      /usr/local/bin/trellis
install -m 0755 "${SHARE_DIR}/trellisctl"   /usr/local/bin/trellisctl
install -m 0755 "${SHARE_DIR}/trellis-health-probe" /usr/local/bin/trellis-health-probe

mkdir -p "${DATA_DIR}" /etc/trellis

HOSTNAME=$(hostname -s)
ADVERTISE_HOST="${HOSTNAME}.local"
cat > "$CONFIG_FILE" <<EOF
cluster: default
admin_token: $(cat "${TOKEN_FILE}")
enrollment_token: $(cat "${ENROLLMENT_TOKEN_FILE}")
node_signing_mode: managed
data_dir: ${DATA_DIR}
agent_advertise: ${ADVERTISE_HOST}:8127
server_advertise: ${ADVERTISE_HOST}:8128
raft_advertise: ${ADVERTISE_HOST}:8129
EOF
if [ "${HOSTNAME}" != "control" ]; then
    for _ in $(seq 1 60); do [ -s "${CA_CERT_FILE}" ] && break; sleep 1; done
    [ -s "${CA_CERT_FILE}" ] || { echo "cluster CA certificate unavailable" >&2; exit 1; }
    install -m 0644 "${CA_CERT_FILE}" /etc/trellis/node-ca.crt
    printf 'join: control.local:8128\n' >> "$CONFIG_FILE"
    printf 'ca_cert: /etc/trellis/node-ca.crt\n' >> "$CONFIG_FILE"
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
if [ "${HOSTNAME}" = "control" ]; then
    for _ in $(seq 1 60); do [ -s "${DATA_DIR}/node-ca.crt" ] && break; sleep 1; done
    install -m 0644 "${DATA_DIR}/node-ca.crt" "${CA_CERT_FILE}"
fi
