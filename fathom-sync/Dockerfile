FROM rust:1.85-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY fathom-sync/ fathom-sync/
RUN cargo build --release -p fathom-sync

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/fathom-sync /usr/local/bin/
ENTRYPOINT ["fathom-sync"]
