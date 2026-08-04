FROM ubuntu:latest AS builder

RUN apt-get update
RUN apt-get install wget -y

#Install Dependencies
RUN apt-get install -y build-essential
RUN apt-get install -y curl
RUN apt-get install -y software-properties-common
RUN apt-get install -y libnetcdf-dev
RUN apt-get install -y netcdf-bin
RUN apt-get install -y libhdf5-dev
RUN apt-get install -y capnproto
RUN apt-get install -y libclang-dev
RUN apt-get install -y libsqlite3-dev
RUN apt-get install -y cmake
RUN apt-get install -y sqlite3
# protoc: required at build time by the `lance` crate (beacon-lance managed tables)
RUN apt-get install -y protobuf-compiler

#Install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# COPY SOURCE

# Two source trees: the engine and the application that runs on it.
COPY beacon-db/ /beacon-db/
COPY beacon-server/ /beacon-server/
COPY Cargo.toml /
COPY Cargo.lock /
COPY rust-toolchain.toml /

#Build the project (only the server binary the image ships; jemalloc on for prod)
RUN cargo build --release -p beacon-server --features jemalloc

# Build the admin web UI (Vite SPA) from the JS client workspace. The SDK
# (@beacon/client) must be built before the web app, which imports from its dist.
FROM node:20-slim AS webui
WORKDIR /beacon-clients
COPY beacon-clients/package.json beacon-clients/package-lock.json ./
COPY beacon-clients/beacon-ts/ ./beacon-ts/
COPY beacon-clients/beacon-web/ ./beacon-web/
RUN npm ci
RUN npm run build --workspace beacon-ts
RUN npm run build --workspace beacon-web

FROM ubuntu:latest AS runtime
WORKDIR /beacon
COPY --from=builder /target/release/beacon-server /beacon/
# Bundle the built admin UI; beacon-server serves it at /admin (BEACON_WEB_UI_DIR=web).
COPY --from=webui /beacon-clients/beacon-web/dist /beacon/web

#Install Dependencies
RUN apt-get update
RUN apt-get install -y curl
RUN apt-get install -y netcdf-bin
RUN apt-get install -y libnetcdf-dev

# 5001: HTTP API + admin UI. 32011: Arrow Flight SQL (BEACON_FLIGHT_SQL_PORT).
EXPOSE 5001 32011

ENTRYPOINT ["/beacon/beacon-server"]
