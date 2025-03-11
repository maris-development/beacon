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
COPY beacon-arrow-odv/ /beacon-arrow-odv/
COPY beacon-common/ /beacon-common/
COPY beacon-config/ /beacon-config/
COPY beacon-core/ /beacon-core/
COPY beacon-functions/ /beacon-functions/
COPY beacon-output/ /beacon-output/
COPY beacon-query/ /beacon-query/
COPY beacon-sources/ /beacon-sources/

COPY Cargo.toml /
COPY Cargo.lock /

#Build the project
RUN cargo build --release

FROM ubuntu:latest AS node
WORKDIR /beacon
COPY --from=builder /target/release/beacon-api /beacon/

#Install Dependencies
RUN apt-get update
RUN apt-get install -y netcdf-bin

EXPOSE 5001

ENTRYPOINT ["/beacon/beacon-api"]
