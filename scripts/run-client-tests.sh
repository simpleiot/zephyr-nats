#!/usr/bin/env bash
# Copyright (c) 2026 Cliff Brake
# SPDX-License-Identifier: Apache-2.0
#
# Build tests/client for native_sim, start nats-server instances on free
# ports, and run the tests against them. The tests run twice: once plain, and
# once with TLS against a server using a CA generated for the run.
#
#   scripts/run-client-tests.sh [build-dir]
#
# Needs west, a Zephyr workspace (ZEPHYR_BASE), nats-server, and openssl.

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
build=${1:-$root/build/client}

logs=$(mktemp -d)
pids=()

cleanup() {
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$logs"
}
trap cleanup EXIT

free_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

wait_port() {
    for _ in $(seq 50); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "nats-server did not start on port $1" >&2
    cat "$logs"/*.log >&2
    return 1
}

start_server() {
    local port=$1
    shift
    nats-server -a 127.0.0.1 -p "$port" "$@" >"$logs/$port.log" 2>&1 &
    pids+=($!)
    wait_port "$port"
}

# A CA and a server certificate for localhost, valid for this run only.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 \
    -subj /CN=nats-test-ca -keyout "$logs/ca.key" -out "$logs/ca.pem" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -subj /CN=localhost -keyout "$logs/server.key" -out "$logs/server.csr" 2>/dev/null
openssl x509 -req -in "$logs/server.csr" -CA "$logs/ca.pem" -CAkey "$logs/ca.key" \
    -days 1 -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1') \
    -out "$logs/server.pem" 2>/dev/null
openssl x509 -in "$logs/ca.pem" -outform der -out "$logs/ca.der"

west build -b native_sim -d "$build" "$root/tests/client"
west build -b native_sim -d "$build-tls" "$root/tests/client" -- \
    -DEXTRA_CONF_FILE=overlay-tls.conf -DNATS_TEST_CA="$logs/ca.der"

open_port=$(free_port)
user_port=$(free_port)
token_port=$(free_port)
tls_port=$(free_port)
proxy_port=$(free_port)

cat >"$logs/tls.conf" <<CONF
tls {
    cert_file: "$logs/server.pem"
    key_file: "$logs/server.key"
    handshake_first: true
}
CONF

start_server "$open_port"
start_server "$user_port" --user test --pass secret
start_server "$token_port" --auth s3cret
start_server "$tls_port" -c "$logs/tls.conf"

export NATS_TEST_URL="nats://127.0.0.1:$open_port"
export NATS_TEST_USER_URL="nats://127.0.0.1:$user_port"
export NATS_TEST_TOKEN_URL="nats://127.0.0.1:$token_port"
export NATS_TEST_TLS_URL="tls://127.0.0.1:$tls_port"
export NATS_TEST_PROXY_PORT="$proxy_port"

status=0
timeout 300 "$build/zephyr/zephyr.exe" || status=$?

# The client starts TLS before reading INFO, which needs a server that honors
# "handshake_first". Older servers send INFO in plaintext instead.
if echo | timeout 5 openssl s_client -connect "127.0.0.1:$tls_port" \
    -CAfile "$logs/ca.pem" >/dev/null 2>&1; then
    timeout 300 "$build-tls/zephyr/zephyr.exe" -test="nats_client::test_tls" || status=$?
else
    echo "SKIP: TLS tests; this nats-server does not support handshake_first" >&2
fi

exit $status
