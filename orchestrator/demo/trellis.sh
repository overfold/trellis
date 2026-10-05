#!/usr/bin/env bash
set -euo pipefail

SHARE_DIR="/vagrant/bin"
DATA_DIR="/var/lib/trellis/data"
CONFIG_FILE="/etc/trellis/trellis.yaml"
ADMIN_KEY_FILE="${SHARE_DIR}/administrator-key.pem"
ADMIN_PUBLIC_KEY_FILE="${SHARE_DIR}/administrator-public-key"
JOIN_TOKEN_FILE="${SHARE_DIR}/join-token-$(hostname -s)"
CA_CERT_FILE="${SHARE_DIR}/node-ca.crt"

# Generate the administrator signing key. The control node later uses it to
# mint the short-lived join token the workers enroll with.
mkdir -p "${SHARE_DIR}"
if [ ! -s "${ADMIN_KEY_FILE}" ] || [ ! -s "${ADMIN_PUBLIC_KEY_FILE}" ]; then
    umask 077
    openssl genpkey -algorithm ED25519 -out "${ADMIN_KEY_FILE}"
    openssl pkey -in "${ADMIN_KEY_FILE}" -pubout -outform DER | base64 | tr -d '=\n' >"${ADMIN_PUBLIC_KEY_FILE}"
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
node_signing_mode: managed
data_dir: ${DATA_DIR}
agent_advertise: ${ADVERTISE_HOST}:8127
server_advertise: ${ADVERTISE_HOST}:8128
raft_advertise: ${ADVERTISE_HOST}:8129
EOF
if [ "${HOSTNAME}" = "control" ]; then
    printf 'administrator_public_key: %s\n' "$(cat "${ADMIN_PUBLIC_KEY_FILE}")" >> "$CONFIG_FILE"
    rm -f "${CA_CERT_FILE}" "${SHARE_DIR}/join-token-worker-1" "${SHARE_DIR}/join-token-worker-2"
else
    for _ in $(seq 1 60); do [ -s "${CA_CERT_FILE}" ] && [ -s "${JOIN_TOKEN_FILE}" ] && break; sleep 1; done
    [ -s "${CA_CERT_FILE}" ] || { echo "cluster CA certificate unavailable" >&2; exit 1; }
    [ -s "${JOIN_TOKEN_FILE}" ] || { echo "cluster join token unavailable" >&2; exit 1; }
    install -m 0644 "${CA_CERT_FILE}" /etc/trellis/node-ca.crt
    printf 'join: control.local:8128\n' >> "$CONFIG_FILE"
    printf 'join_token: %s\n' "$(cat "${JOIN_TOKEN_FILE}")" >> "$CONFIG_FILE"
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
    # All three demo nodes participate in Raft. Each joining identity needs
    # its own single-use control-plane token.
    umask 077
    for node in worker-1 worker-2; do
        token_file="${SHARE_DIR}/join-token-${node}"
        for _ in $(seq 1 60); do
            trellisctl --server-addr localhost:8128 --ca-cert "${DATA_DIR}/node-ca.crt" --administrator-key "${ADMIN_KEY_FILE}" \
                nodes join-token create --role control-plane --ttl 1h >"${token_file}.tmp" 2>/dev/null && break
            sleep 1
        done
        [ -s "${token_file}.tmp" ] || { echo "could not mint a join token" >&2; exit 1; }
        mv "${token_file}.tmp" "${token_file}"
    done
    install -m 0644 "${DATA_DIR}/node-ca.crt" "${CA_CERT_FILE}"
fi
