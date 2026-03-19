# syntax=docker/dockerfile:1.7

# --- ETAPA 1: PLANIFICACION (cache de dependencias) ---
FROM rust:1.94-slim-bookworm AS chef
WORKDIR /app

# Dependencias necesarias para compilar:
# - protobuf-compiler: requerido por common/build.rs (tonic_build)
# - pkg-config + libssl-dev: requerido por crates que usan OpenSSL (ej. reqwest default-tls)
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
RUN cargo build --release --package master --bin master

# --- ETAPA 3: RUNTIME ---
FROM debian:bookworm-slim
WORKDIR /app

# Runtime:
# - ca-certificates: TLS para llamadas HTTPS (audit/elasticsearch)
# - openssl + libssl3: requerido por binarios enlazados con OpenSSL
RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates openssl libssl3 \
	&& rm -rf /var/lib/apt/lists/*

# Copiamos solo el binario al runtime final.
COPY --from=builder /app/target/release/master /usr/local/bin/master

# Directorio de estado persistente (WAL/SQLite si se sobreescriben por CLI)
RUN mkdir -p /app/data

EXPOSE 9090

# Configuracion externa (volumen): /config/master.toml y /config/master.env.
ENTRYPOINT ["/usr/local/bin/master"]
CMD ["--config-path", "/config/master.toml", "--listen-address", "[::]:9090", "--wal-path", "/app/data/master.wal", "--auth-db-path", "/app/data/users.db", "--env-file-path", "/config/master.env"]

