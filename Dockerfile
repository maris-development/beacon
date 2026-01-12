FROM ubuntu:latest as builder

RUN apt-get update
RUN apt-get install wget -y

#Install Dependencies
RUN apt-get install -y build-essential
RUN apt-get install -y curl
RUN apt-get install -y software-properties-common
RUN apt-get install -y libnetcdf-dev
RUN apt-get install -y netcdf-bin
RUN apt-get install -y libnetcdf-dev
RUN apt-get install -y libhdf5-dev
RUN apt-get install -y capnproto
RUN apt-get install -y libclang-dev
RUN apt-get install -y libsqlite3-dev
RUN apt-get install -y cmake
RUN apt-get install -y sqlite3

#Install Rust
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# COPY SOURCE

COPY beacon-api/ /beacon-api/
COPY beacon-arrow-netcdf/ /beacon-arrow-netcdf/
COPY beacon-arrow-netcdf-mpio/ /beacon-arrow-netcdf-mpio/
COPY beacon-arrow-zarr/ /beacon-arrow-zarr/
COPY beacon-arrow-odv/ /beacon-arrow-odv/
COPY beacon-binary-format/ /beacon-binary-format/
COPY beacon-common/ /beacon-common/
COPY beacon-config/ /beacon-config/
COPY beacon-core/ /beacon-core/
COPY beacon-data-lake/ /beacon-data-lake/
COPY beacon-formats/ /beacon-formats/
COPY beacon-functions/ /beacon-functions/
COPY beacon-object-storage/ /beacon-object-storage/
COPY beacon-planner/ /beacon-planner/
COPY beacon-query/ /beacon-query/
COPY Cargo.toml /
COPY Cargo.lock /
COPY rust-toolchain /

#Build the project
RUN cargo build --release

FROM ubuntu:latest AS node
WORKDIR /beacon
COPY --from=builder /target/release/beacon-api /beacon/
COPY --from=builder /target/release/beacon-arrow-netcdf-mpio /beacon/

#Install Dependencies
RUN apt-get update
RUN apt-get install -y curl
RUN apt-get install -y netcdf-bin
RUN apt-get install -y libnetcdf-dev

EXPOSE 5001

ENTRYPOINT ["/beacon/beacon-api"]
