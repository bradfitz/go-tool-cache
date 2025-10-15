#!/usr/bin/env bash
#
# benchmark.sh benchmarks go-cacher + go-cacher-server.
#
# It runs three types of benchmarks:
#
# - cold; go-cacher and go-cacher-server both start with a cold cache.
# - warm; go-cacher and go-cacher-server both start with a warm cache.
# - http; go-cacher starts with a cold cache, go-cacher-server starts with a warm cache.
#
# Usage:
#   ./scripts/benchmark.sh [<go cmd>]
#
# Environment variables:
#   LATENCIES: Comma-separated list of latencies (as Go duration strings) to run go-cacher-server with. Default: 0
#   TESTS: Comma-separated list of tests to run. Options are: cold, warm, http. Default: cold,warm,http

set -euo pipefail

GOCMD="${@:-go build ./...}"
TESTS="${TESTS:-cold,warm,http}"
IFS=',' read -ra LATENCY_ARRAY <<< "${LATENCIES:-0}"

which go-cacher > /dev/null 2>&1 || go install github.com/bradfitz/go-tool-cache/cmd/go-cacher@latest
which go-cacher-server > /dev/null 2>&1 || go install github.com/bradfitz/go-tool-cache/cmd/go-cacher-server@latest

export RUN_DIR=$(mktemp -d)

cleanup() {
    stop_server
    rm -rf "$RUN_DIR"
}
trap cleanup EXIT SIGTERM SIGINT

ensure_server() {
    if [ -f "$RUN_DIR/server.pid" ]; then
        return
    fi

    go-cacher-server -verbose -inject-latency="${LATENCY}" -cache-dir="$(mktemp -d -p "$RUN_DIR" srv_cache_XXXX)" > "/tmp/server.log" 2>&1 &
    echo $! > "$RUN_DIR/server.pid"
}

stop_server() {
    if [ -f "$RUN_DIR/server.pid" ]; then
        kill $(cat "$RUN_DIR/server.pid") 2>/dev/null || true
        rm -f "$RUN_DIR/server.pid"
    fi
}

ensure_cold_server() {
    stop_server
    ensure_server
}

for l in "${LATENCY_ARRAY[@]}"; do
    export LATENCY="$l"
    echo "---- LATENCY=${LATENCY} ----"
    echo ""

    if [[ "${TESTS}" == *"cold"* ]]; then
        ensure_cold_server

        echo "Testing with cold cache..."
        start=$(date +%s%N)
        GOCACHEPROG="go-cacher -verbose -cache-dir=$(mktemp -d -p "$RUN_DIR" cmd_cache_XXXX) -cache-server=http://localhost:31364" eval "$GOCMD"
        dur=$(( ($(date +%s%N) - start) / 1000000 ))
        echo "Cold run took ${dur}ms"
        echo ""
    fi

    if [[ "${TESTS}" == *"warm"* ]]; then
        export CMD_CACHE_DIR="$(mktemp -d -p "$RUN_DIR" cmd_cache_XXXX)"
        ensure_server
        GOCACHEPROG="go-cacher -cache-dir=$CMD_CACHE_DIR -cache-server=http://localhost:31364" eval "$GOCMD"

        echo "Testing with warm HTTP and disk cache..."
        start=$(date +%s%N)
        GOCACHEPROG="go-cacher -verbose -cache-dir=$CMD_CACHE_DIR -cache-server=http://localhost:31364" eval "$GOCMD"
        dur=$(( ($(date +%s%N) - start) / 1000000 ))
        echo "Warm run took ${dur}ms"
        echo ""
    fi

    if [[ "${TESTS}" == *"http"* ]]; then
        ensure_server
        GOCACHEPROG="go-cacher -cache-dir=$(mktemp -d -p "$RUN_DIR" cmd_cache_XXXX) -cache-server=http://localhost:31364" eval "$GOCMD"

        echo "Testing with warm HTTP cache only..."
        start=$(date +%s%N)
        GOCACHEPROG="go-cacher -verbose -cache-dir=$(mktemp -d -p "$RUN_DIR" cmd_cache_XXXX) -cache-server=http://localhost:31364" eval "$GOCMD"
        dur=$(( ($(date +%s%N) - start) / 1000000 ))
        echo "HTTP run took ${dur}ms"
        echo ""
    fi
done
