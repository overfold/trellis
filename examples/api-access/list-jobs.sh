#!/bin/sh
set -eu
: "${TRELLIS_ADDR:?api_access must inject TRELLIS_ADDR}"
: "${TRELLIS_TOKEN:?api_access must inject TRELLIS_TOKEN}"
: "${TRELLIS_NAMESPACE:?api_access must inject TRELLIS_NAMESPACE}"

case "$TRELLIS_ADDR" in
  https://*) api_url=${TRELLIS_ADDR%/} ;;
  http://*) echo "refusing to send TRELLIS_TOKEN over plaintext HTTP" >&2; exit 1 ;;
  *) api_url="https://${TRELLIS_ADDR%/}" ;;
esac

set -- --fail --silent --show-error --connect-timeout 5 --max-time 15 \
  -H "Authorization: Bearer $TRELLIS_TOKEN" \
  -H "X-Trellis-Namespace: $TRELLIS_NAMESPACE"

ca_file=
if [ -n "${TRELLIS_CA_CERT:-}" ] && [ "${api_url#https://}" != "$api_url" ]; then
  ca_file=$(mktemp)
  trap 'rm -f "$ca_file"' EXIT HUP INT TERM
  printf '%s\n' "$TRELLIS_CA_CERT" > "$ca_file"
  set -- "$@" --cacert "$ca_file"
fi

curl "$@" "$api_url/v1/jobs"
