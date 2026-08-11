# cf-tokenizer — static musl binary, dictionary compiled in (jieba-rs).
# Keep the image minimal: one binary, no shell, no state.
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# --locked: a Cargo.toml/Cargo.lock skew must fail the build, not silently re-resolve deps.
RUN cargo build --release --locked

FROM scratch
COPY --from=build /app/target/release/cf-tokenizer /cf-tokenizer
EXPOSE 8080
ENTRYPOINT ["/cf-tokenizer"]
