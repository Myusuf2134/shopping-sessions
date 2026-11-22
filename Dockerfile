FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app \
    && mkdir -p /scratch \
    && chown 1000:1000 /scratch
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
ENV HOME=/scratch
WORKDIR /app
CMD ["/app/shopping-sessions"]
