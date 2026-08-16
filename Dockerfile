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
# PROJ: required at build time by `ST_Transform`, which the `spatial-proj` feature ships and which
# is on by default. `proj-sys` asks pkg-config for PROJ 9.6.2 or later. It builds PROJ from its own
# vendored source when the distribution carries an older one, so this line is a speed measure, not
# a requirement: it keeps the image off the slow path. The source build needs cmake and sqlite3,
# which this file installs above.
RUN apt-get install -y libproj-dev pkg-config

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

# Build the project. The image ships the server binary only.
#   jemalloc:     the production allocator. It is not a default feature.
#   spatial-proj: `ST_Transform`, which links PROJ. It is a default feature, and this line names
#                 it so the image states what it ships. Drop it and pass `--no-default-features`
#                 to build without PROJ.
RUN cargo build --release -p beacon-server --features jemalloc,spatial-proj

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
# The server links PROJ, so the runtime image carries it too. `proj-data` holds `proj.db`, which
# PROJ reads to resolve an EPSG code. Without that file `ST_Transform` fails, and it fails loudly:
# PROJ reports the missing database instead of wrong coordinates.
#
# `libproj-dev` pulls the headers this stage does not need. It is still the package to name here,
# because the runtime package carries the PROJ version in its name (`libproj25`, `libproj27`), and
# that name changes with the base image. The development package depends on the right one.
RUN apt-get install -y libproj-dev proj-data
# Name the database directory. A source build of PROJ compiles in a path under the builder's
# `target/`, and that path does not exist in this image. `PROJ_DATA` wins over the compiled path,
# so this one line makes both build paths behave the same.
ENV PROJ_DATA=/usr/share/proj

# 5001: HTTP API + admin UI. 32011: Arrow Flight SQL (BEACON_FLIGHT_SQL_PORT).
EXPOSE 5001 32011

ENTRYPOINT ["/beacon/beacon-server"]
