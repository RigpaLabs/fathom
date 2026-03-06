.PHONY: run run-json build release test smoke lint fmt fmt-check check clean docker-smoke docker-blue-green monitor check-data cov

build:
	cargo build

run:
	RUST_LOG=fathom=info cargo run

run-json:
	FATHOM_JSON_LOG=1 RUST_LOG=fathom=info cargo run

release:
	cargo build --release

test:
	cargo test

smoke:
	cargo test --test smoke_test -- --include-ignored --test-threads 1 --nocapture

lint:
	cargo clippy -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt --check

check: fmt-check lint test

clean:
	cargo clean

# ── Docker smoke test (real Binance) ──────────────────────────────────────

SMOKE_SECS ?= 60
docker-smoke:
	./scripts/docker-smoke.sh $(SMOKE_SECS)

docker-blue-green:
	./scripts/docker-blue-green.sh

# ── Monitoring ─────────────────────────────────────────────────────────────

# Run fathom for N minutes (default 10), then show summary
# Usage: make monitor [MINUTES=10]
MINUTES ?= 10
monitor:
	@echo "=== fathom monitor: $(MINUTES) min run ==="
	@echo "Starting fathom (Ctrl-C to abort early)..."
	@timeout $$(($(MINUTES) * 60)) cargo run 2>&1 || true
	@echo ""
	@echo "=== Data check ==="
	@find data -name "*.parquet" 2>/dev/null | head -20 || echo "No parquet files found"
	@echo ""
	@echo "=== File sizes ==="
	@du -sh data/*/* 2>/dev/null || echo "No data dirs"
	@echo ""
	@echo "=== status.json ==="
	@cat data/metadata/status.json 2>/dev/null | python3 -m json.tool || echo "No status.json"

# Quick data check (no run)
check-data:
	@echo "=== Parquet files ==="
	@find data -name "*.parquet" -exec ls -lh {} \; 2>/dev/null || echo "No files"
	@echo ""
	@echo "=== status.json ==="
	@cat data/metadata/status.json 2>/dev/null | python3 -m json.tool || echo "No status.json"
	@echo ""
	@echo "=== 1s snapshot row counts ==="
	@for f in $$(find data/1s -name "*.parquet" 2>/dev/null); do \
		echo "$$f: $$(python3 -c "import pyarrow.parquet as pq; print(pq.read_table('$$f').num_rows)" 2>/dev/null || echo 'N/A') rows"; \
	done

# Coverage
cov:
	cargo llvm-cov test
	cargo llvm-cov report --show-missing-lines
