#!/usr/bin/env bash
# Local blue-green deploy test — two containers with different DATA_DIRs,
# running simultaneously against real Binance. Mirrors production deploy.yml.
#
# Usage:  ./scripts/docker-blue-green.sh
#         make docker-blue-green
set -euo pipefail

IMAGE="fathom:smoke-test"
OLD="fathom-old"
NEW="fathom-new"
VOLUME="fathom-smoke-data"
TMPDIR_HOST=$(mktemp -d)
trap 'rm -rf "$TMPDIR_HOST"' EXIT

echo "=== fathom blue-green local test ==="
echo ""

# ── Config ───────────────────────────────────────────────────────────────
cat > "${TMPDIR_HOST}/config.toml" << 'EOF'
data_dir = "data"
raw_rotate_hours = 1

[[connections]]
name     = "spot"
exchange = "binance_spot"
symbols  = ["BTCUSDT"]
depth_ms = 100
EOF

# ── Build ────────────────────────────────────────────────────────────────
echo "Building image..."
docker build -t "${IMAGE}" -f Dockerfile . 2>&1 | tail -3
echo ""

# ── Cleanup ──────────────────────────────────────────────────────────────
docker rm -f "${OLD}" "${NEW}" 2>/dev/null || true
docker volume rm "${VOLUME}" 2>/dev/null || true
docker volume create "${VOLUME}" > /dev/null

# ── 1. Start "old" container ────────────────────────────────────────────
echo "▶ Starting OLD container (DATA_DIR=/app/data/v1)..."
docker run -d \
    --name "${OLD}" \
    -e RUST_LOG=fathom=info \
    -e DATA_DIR=/app/data/v1 \
    -v "${VOLUME}:/app/data" \
    -v "${TMPDIR_HOST}/config.toml:/app/config.toml:ro" \
    "${IMAGE}" > /dev/null

echo "  Collecting for 40s..."
sleep 40

echo "  OLD status:"
docker exec "${OLD}" cat /app/data/v1/metadata/status.json 2>/dev/null \
    | python3 -m json.tool 2>/dev/null | head -5 || echo "  (no status yet)"
echo ""

# ── 2. Start "new" container alongside old ──────────────────────────────
echo "▶ Starting NEW container (DATA_DIR=/app/data/v2) — both running..."
docker run -d \
    --name "${NEW}" \
    -e RUST_LOG=fathom=info \
    -e DATA_DIR=/app/data/v2 \
    -v "${VOLUME}:/app/data" \
    -v "${TMPDIR_HOST}/config.toml:/app/config.toml:ro" \
    "${IMAGE}" > /dev/null

echo "  Both containers collecting for 30s..."
sleep 30

echo ""
echo "  Running containers:"
docker ps --filter name=fathom --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'
echo ""

# ── 3. Stop old gracefully (like production) ────────────────────────────
echo "▶ Stopping OLD container (30s grace for flush)..."
docker stop -t 30 "${OLD}" > /dev/null
docker rm "${OLD}" > /dev/null
echo "  OLD stopped and removed"
echo ""

# ── 4. New keeps running — collect more ─────────────────────────────────
echo "▶ NEW still running alone for 20s..."
sleep 20

echo ""
echo "  NEW status:"
docker exec "${NEW}" cat /app/data/v2/metadata/status.json 2>/dev/null \
    | python3 -m json.tool 2>/dev/null || echo "  (no status)"
echo ""

# ── 5. Stop new ─────────────────────────────────────────────────────────
echo "▶ Stopping NEW container..."
docker stop -t 30 "${NEW}" > /dev/null

# ── 6. Verify both data dirs ────────────────────────────────────────────
echo ""
echo "=== Verification ==="

# Copy data from volume via temp container
docker run --rm -v "${VOLUME}:/data" -v "${TMPDIR_HOST}:/out" alpine \
    sh -c "cp -r /data/v1 /out/v1 2>/dev/null; cp -r /data/v2 /out/v2 2>/dev/null" || true

PASS=true

for VER in v1 v2; do
    echo ""
    echo "── ${VER} ──"
    RAW=$(find "${TMPDIR_HOST}/${VER}/raw" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    SNAP=$(find "${TMPDIR_HOST}/${VER}/1s" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
    echo "  Raw files:  ${RAW}"
    echo "  Snap files: ${SNAP}"

    for f in $(find "${TMPDIR_HOST}/${VER}" -name "*.parquet" 2>/dev/null); do
        SIZE=$(wc -c < "$f" | tr -d ' ')
        NAME=$(echo "$f" | sed "s|${TMPDIR_HOST}/||")
        echo "  ${NAME}: ${SIZE} bytes"
        if [ "${SIZE}" -eq 0 ]; then
            echo "  ⚠ EMPTY FILE"
            PASS=false
        fi
    done

    if [ "${RAW}" -eq 0 ] || [ "${SNAP}" -eq 0 ]; then
        PASS=false
    fi
done

# ── 7. Cleanup ──────────────────────────────────────────────────────────
docker rm "${NEW}" > /dev/null 2>&1 || true
docker volume rm "${VOLUME}" > /dev/null 2>&1 || true

echo ""
if [ "${PASS}" = true ]; then
    echo "=== PASS: blue-green test OK ==="
    echo "  Both v1 and v2 have independent, non-empty parquet data"
    exit 0
else
    echo "=== FAIL: missing or empty data ==="
    exit 1
fi
