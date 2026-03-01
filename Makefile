.PHONY: run build release test smoke lint fmt fmt-check clean

build:
	cargo build

run:
	cargo run

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

clean:
	cargo clean
