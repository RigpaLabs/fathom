FROM rust:1-slim-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends make gcc && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Layer 1: cache dependencies (busts only when Cargo.toml/lock change)
COPY Cargo.toml Cargo.lock ./
COPY crates/fathom-types/Cargo.toml crates/fathom-types/
RUN mkdir src && echo "fn main(){}" > src/main.rs && \
    mkdir -p crates/fathom-types/src && touch crates/fathom-types/src/lib.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src crates/fathom-types/src

# Layer 2: actual source (busts on ANY .rs change)
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --create-home appuser
WORKDIR /app
COPY --from=builder /app/target/release/fathom /usr/local/bin/fathom
RUN mkdir -p /app/data/metadata && chown -R appuser:appuser /app/data
USER appuser

ENV RUST_LOG=fathom=info

ENTRYPOINT ["fathom"]
