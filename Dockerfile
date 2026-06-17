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
COPY file-formats/beacon-arrow-atlas/ /file-formats/beacon-arrow-atlas/
COPY file-formats/beacon-arrow-netcdf/ /file-formats/beacon-arrow-netcdf/
COPY file-formats/beacon-arrow-tiff/ /file-formats/beacon-arrow-tiff/
COPY file-formats/beacon-arrow-zarr/ /file-formats/beacon-arrow-zarr/
COPY file-formats/beacon-arrow-odv/ /file-formats/beacon-arrow-odv/
COPY file-formats/beacon-binary-format/ /file-formats/beacon-binary-format/
COPY beacon-common/ /beacon-common/
COPY beacon-config/ /beacon-config/
COPY beacon-core/ /beacon-core/
COPY beacon-data-lake/ /beacon-data-lake/
COPY beacon-datafusion-ext/ /beacon-datafusion-ext/
COPY file-formats/beacon-formats/ /file-formats/beacon-formats/
COPY beacon-functions/ /beacon-functions/
COPY beacon-iceberg/ /beacon-iceberg/
COPY file-formats/beacon-nd-array/ /file-formats/beacon-nd-array/
COPY file-formats/beacon-nd-arrow/ /file-formats/beacon-nd-arrow/
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
