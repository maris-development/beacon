---
description: Build the BeaconDB wheel from source, including the native toolchain the engine links and the portable static-netcdf build.
---

# Building from source

`beacondb` embeds the whole engine, so building the wheel needs the native toolchain the engine
links, not just a Rust compiler:

- **protoc** (Lance generates protobuf at build time)
- **HDF5 + netCDF** headers/libraries (the netCDF reader/writer)
- a Rust toolchain (pinned by `rust-toolchain`)

```bash
# macOS
brew install protobuf hdf5 netcdf
# Debian/Ubuntu
sudo apt-get install -y protobuf-compiler libhdf5-dev libnetcdf-dev

pip install maturin
maturin develop            # build + install into the current venv (debug)
maturin build --release    # produce a wheel in ./target/wheels (or --out dist)
```

The wheel is **abi3** (`cp310-abi3`), so one wheel per platform covers CPython 3.10+. It ships
`py.typed` and `_beacondb.pyi` type stubs, including the catalog-driven `read_*` readers.

## Portable wheels (`static-netcdf`)

Distributable wheels link netCDF and HDF5 **statically**, compiling them from source, so the wheel
carries them and needs no system libraries, the only way to ship a portable **Windows** wheel:

```bash
maturin build --release --features static-netcdf   # needs protoc + cmake only
```

CI (`.github/workflows/publish-beacondb.yml`, triggered by a `beacondb-v*` tag) builds this way for
Linux (manylinux_2_28 + musllinux, x86_64/aarch64), macOS (arm64 + x86_64), and Windows, then
publishes to PyPI via trusted publishing.

Two honest caveats: the wheel is **large** (it contains a full DataFusion/Lance/netCDF engine), and
there is **no minimal (feature-gated) build** yet.
