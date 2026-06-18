FROM ubuntu:latest AS builder

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
COPY beacon-file-formats/beacon-arrow-atlas/ /beacon-file-formats/beacon-arrow-atlas/
COPY beacon-file-formats/beacon-arrow-netcdf/ /beacon-file-formats/beacon-arrow-netcdf/
COPY beacon-file-formats/beacon-arrow-tiff/ /beacon-file-formats/beacon-arrow-tiff/
COPY beacon-file-formats/beacon-arrow-zarr/ /beacon-file-formats/beacon-arrow-zarr/
COPY beacon-file-formats/beacon-arrow-odv/ /beacon-file-formats/beacon-arrow-odv/
COPY beacon-file-formats/beacon-binary-format/ /beacon-file-formats/beacon-binary-format/
COPY beacon-common/ /beacon-common/
COPY beacon-config/ /beacon-config/
COPY beacon-core/ /beacon-core/
COPY beacon-data-lake/ /beacon-data-lake/
COPY beacon-datafusion-ext/ /beacon-datafusion-ext/
COPY beacon-file-formats/beacon-arrow-ipc/ /beacon-file-formats/beacon-arrow-ipc/
COPY beacon-file-formats/beacon-arrow-csv/ /beacon-file-formats/beacon-arrow-csv/
COPY beacon-file-formats/beacon-arrow-parquet/ /beacon-file-formats/beacon-arrow-parquet/
COPY beacon-file-formats/beacon-arrow-geoparquet/ /beacon-file-formats/beacon-arrow-geoparquet/
COPY beacon-file-formats/beacon-arrow-bbf/ /beacon-file-formats/beacon-arrow-bbf/
COPY beacon-functions/ /beacon-functions/
COPY beacon-iceberg/ /beacon-iceberg/
COPY beacon-file-formats/beacon-nd-array/ /beacon-file-formats/beacon-nd-array/
COPY beacon-file-formats/beacon-nd-arrow/ /beacon-file-formats/beacon-nd-arrow/
COPY beacon-object-storage/ /beacon-object-storage/
COPY Cargo.toml /
COPY Cargo.lock /
COPY rust-toolchain /

#Build the project
RUN cargo build --release

FROM ubuntu:latest AS node
WORKDIR /beacon
COPY --from=builder /target/release/beacon-api /beacon/

#Install Dependencies
RUN apt-get update
RUN apt-get install -y curl
RUN apt-get install -y netcdf-bin
RUN apt-get install -y libnetcdf-dev

EXPOSE 5001

ENTRYPOINT ["/beacon/beacon-api"]
