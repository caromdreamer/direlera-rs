FROM rust:1.87-slim AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
ARG COMMIT_SHA=unknown
ENV GIT_COMMIT=$COMMIT_SHA
LABEL org.opencontainers.image.revision=$COMMIT_SHA
WORKDIR /app
COPY --from=builder /app/target/release/direlera-rs .
COPY config.toml .
EXPOSE 8080/udp
EXPOSE 27888/udp
CMD ["./direlera-rs"]
