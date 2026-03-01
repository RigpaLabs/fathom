# ── Builder ───────────────────────────────────────────────────────────────────
FROM rust:1.84-slim AS builder

WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && echo '' > src/lib.rs
RUN cargo build --release 2>/dev/null; rm -rf src

# Build real source
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/fathom /usr/local/bin/fathom

VOLUME /app/data

CMD ["fathom"]
