#!/usr/bin/env bash
# Copyright (c) 2026 Cliff Brake
# SPDX-License-Identifier: Apache-2.0
#
# Check the samples against the nats CLI, which uses the reference Go client.
#
#   scripts/run-interop.sh [build-dir]
#
# Needs west, a Zephyr workspace (ZEPHYR_BASE), and nats-server and nats on PATH.

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
build=${1:-$root/build/interop}

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

fail() {
    echo "FAIL: $*" >&2
    for f in "$logs"/*.log; do
        echo "--- $f" >&2
        tail -20 "$f" >&2
    done
    exit 1
}

wait_for() {
    local pattern=$1 file=$2
    for _ in $(seq 100); do
        if grep -q "$pattern" "$file" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    fail "'$pattern' not found in $file"
}

port=$(python3 -c 'import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])')
url="nats://127.0.0.1:$port"

for sample in pubsub request; do
    west build -b native_sim -d "$build/$sample" "$root/samples/$sample" -- \
        -DEXTRA_CONF_FILE=overlay-host.conf -DCONFIG_SAMPLE_NATS_URL="\"$url\""
done

nats-server -a 127.0.0.1 -p "$port" >"$logs/server.log" 2>&1 &
pids+=($!)
wait_for "Server is ready" "$logs/server.log"

# pubsub: the CLI receives the counter, and its request is answered.
"$build/pubsub/zephyr/zephyr.exe" >"$logs/pubsub.log" 2>&1 &
pids+=($!)
wait_for "connected" "$logs/pubsub.log"

count=$(timeout 10 nats -s "$url" sub zephyr.sample.count --count 3 --raw 2>/dev/null | wc -l)
[ "$count" -eq 3 ] || fail "expected 3 counter messages, got $count"
echo "PASS: nats sub received the counter"

reply=$(nats -s "$url" request zephyr.sample.cmd hello --timeout 5s --raw 2>/dev/null)
[ "$reply" = "ok" ] || fail "expected reply 'ok', got '$reply'"
wait_for "zephyr.sample.cmd: hello" "$logs/pubsub.log"
echo "PASS: nats request was answered"

nats -s "$url" pub zephyr.sample.cmd with-headers -H X-Test:1 >/dev/null 2>&1
wait_for "zephyr.sample.cmd: with-headers" "$logs/pubsub.log"
echo "PASS: message with headers was received"

# request: a CLI responder answers the sample's requests.
nats -s "$url" reply zephyr.sample.echo "pong-from-cli" >"$logs/reply.log" 2>&1 &
pids+=($!)
sleep 0.5

"$build/request/zephyr/zephyr.exe" >"$logs/request.log" 2>&1 &
pids+=($!)
wait_for "reply: pong-from-cli" "$logs/request.log"
echo "PASS: nats reply answered the sample's request"

echo "All interop checks passed"
