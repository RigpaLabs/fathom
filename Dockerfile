FROM rust:1-slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates findutils \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home appuser
WORKDIR /app
COPY --from=builder /app/target/release/fathom /usr/local/bin/fathom
RUN mkdir -p /app/data/metadata && chown -R appuser:appuser /app/data
USER appuser

ENV RUST_LOG=fathom=info

HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD find /app/data -name status.json -mmin -3 2>/dev/null | grep -q . || exit 1

ENTRYPOINT ["fathom"]
