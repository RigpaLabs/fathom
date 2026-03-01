#!/usr/bin/env bash
# Local Docker smoke test — builds the image, runs against real Binance for N seconds,
# verifies parquet files were actually written.
#
# Usage:  ./scripts/docker-smoke.sh [SECONDS]   (default: 60)
#         make docker-smoke                       (same, 60s)
#         make docker-smoke SMOKE_SECS=90         (custom duration)
set -euo pipefail

SECONDS_TO_RUN="${1:-60}"
IMAGE="fathom:smoke-test"
CONTAINER="fathom-smoke"
TMPDIR_HOST=$(mktemp -d)
trap 'rm -rf "$TMPDIR_HOST"' EXIT

echo "=== fathom Docker smoke test ==="
echo "Duration: ${SECONDS_TO_RUN}s | Image: ${IMAGE}"
echo ""

# ── 1. Create minimal config (1 symbol, fast rotation) ──────────────────
cat > "${TMPDIR_HOST}/config.toml" << 'EOF'
data_dir = "data"
raw_rotate_hours = 1

[[connections]]
name     = "spot"
exchange = "binance_spot"
symbols  = ["BTCUSDT"]
depth_ms = 100
EOF

echo "Config: 1 symbol (BTCUSDT spot), 100ms depth"

# ── 2. Build Docker image ────────────────────────────────────────────────
echo ""
echo "Building Docker image..."
docker build -t "${IMAGE}" -f Dockerfile . 2>&1 | tail -5
echo "Build OK"

# ── 3. Cleanup any stale container ──────────────────────────────────────
docker rm -f "${CONTAINER}" 2>/dev/null || true

# ── 4. Run container ────────────────────────────────────────────────────
echo ""
echo "Starting container (${SECONDS_TO_RUN}s against real Binance)..."
docker run -d \
    --name "${CONTAINER}" \
    -e RUST_LOG=fathom=info \
    -v "${TMPDIR_HOST}/config.toml:/app/config.toml:ro" \
    "${IMAGE}"

# ── 5. Wait, show logs ──────────────────────────────────────────────────
echo "Waiting ${SECONDS_TO_RUN}s for data collection..."
echo ""

# Show live logs in background
docker logs -f "${CONTAINER}" 2>&1 &
LOGS_PID=$!

sleep "${SECONDS_TO_RUN}"

# Stop following logs
kill "${LOGS_PID}" 2>/dev/null || true
wait "${LOGS_PID}" 2>/dev/null || true

echo ""
echo "Stopping container (30s grace for writer flush)..."
docker stop -t 30 "${CONTAINER}" > /dev/null

# Show shutdown logs
echo ""
echo "=== Shutdown logs ==="
docker logs "${CONTAINER}" --tail 10 2>&1

# ── 6. Verify data was written ──────────────────────────────────────────
echo ""
echo "=== Verification ==="

# Copy data out of container for inspection
docker cp "${CONTAINER}:/app/data" "${TMPDIR_HOST}/data" 2>/dev/null || {
    echo "FAIL: could not copy data from container"
    docker logs "${CONTAINER}" --tail 30 2>&1
    docker rm "${CONTAINER}" > /dev/null
    exit 1
}

# Check raw parquet files
RAW_FILES=$(find "${TMPDIR_HOST}/data/raw" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
echo "Raw parquet files:   ${RAW_FILES}"

# Check 1s snapshot parquet files
SNAP_FILES=$(find "${TMPDIR_HOST}/data/1s" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
echo "1s snap parquet files: ${SNAP_FILES}"

# Check status.json
if [ -f "${TMPDIR_HOST}/data/metadata/status.json" ]; then
    echo "status.json:         present"
    echo ""
    echo "=== status.json ==="
    cat "${TMPDIR_HOST}/data/metadata/status.json" | python3 -m json.tool 2>/dev/null \
        || cat "${TMPDIR_HOST}/data/metadata/status.json"
else
    echo "status.json:         MISSING"
fi

# List files with sizes
echo ""
echo "=== Files ==="
find "${TMPDIR_HOST}/data" -name "*.parquet" -exec ls -lh {} \;

# Check files have actual data (not just created-but-empty)
EMPTY_FILES=0
for f in $(find "${TMPDIR_HOST}/data" -name "*.parquet" 2>/dev/null); do
    SIZE=$(wc -c < "$f" | tr -d ' ')
    if [ "${SIZE}" -eq 0 ]; then
        echo "WARNING: empty file: $f"
        EMPTY_FILES=$((EMPTY_FILES + 1))
    fi
done

# ── 7. Pass/Fail ────────────────────────────────────────────────────────
echo ""
docker rm "${CONTAINER}" > /dev/null

if [ "${RAW_FILES}" -gt 0 ] && [ "${SNAP_FILES}" -gt 0 ] && [ "${EMPTY_FILES}" -eq 0 ]; then
    echo "=== PASS: Docker smoke test OK ==="
    echo "  Raw files: ${RAW_FILES}, Snap files: ${SNAP_FILES}, all non-empty"
    exit 0
else
    echo "=== FAIL ==="
    echo "  Raw: ${RAW_FILES}, Snap: ${SNAP_FILES}, Empty: ${EMPTY_FILES}"
    exit 1
fi
