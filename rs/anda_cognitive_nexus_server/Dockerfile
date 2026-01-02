# base image
FROM rust:slim-bookworm AS builder

RUN apt-get update \
    && apt-get install -y gcc g++ libc6-dev pkg-config libssl-dev wget

WORKDIR /build
COPY rs ./rs
COPY py ./py
COPY Cargo.toml ./
RUN cargo build --release -p anda_cognitive_nexus_server

FROM debian:bookworm-slim AS runtime

# install dependency tools
RUN apt-get update \
    && apt-get install -y net-tools iptables iproute2 ca-certificates tzdata openssl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# working directory
WORKDIR /app

# your custom setup goes here
COPY --from=builder /build/target/release/anda_cognitive_nexus_server ./anda_cognitive_nexus_server

ENV ADDR="0.0.0.0:8080"

EXPOSE 8080
# entry point
ENTRYPOINT [ "/app/anda_cognitive_nexus_server" ]