# syntax=docker/dockerfile:1.7

# --- ETAPA 1: PLANIFICACION (cache de dependencias) ---
FROM rust:1.96-slim-bookworm AS chef
WORKDIR /app

# Dependencias necesarias para compilar crates del workspace.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# --- ETAPA 2: COMPILACION ---
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release --package datanode --bin datanode

# --- ETAPA 3: RUNTIME ---
FROM debian:bookworm-slim
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssl libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/datanode /usr/local/bin/datanode

RUN mkdir -p /app/data

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/datanode"]
CMD ["--config-path", "/config/datanode.toml", "--listen-address", "[::]:8080", "--data-folder", "/app/data/datanode"]
