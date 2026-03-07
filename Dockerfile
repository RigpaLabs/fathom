FROM rust:1-slim-bookworm AS builder
WORKDIR /app
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
