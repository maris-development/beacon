---
description: Build the BeaconDB wheel from source, including the native toolchain the engine links and the portable static-netcdf build.
---

# Building from source

`beacondb` embeds the whole engine, so building the wheel needs the native toolchain the engine
links, not just a Rust compiler:

- **protoc** (Lance generates protobuf at build time)
- **HDF5 + netCDF** headers/libraries (the netCDF reader/writer)
- a Rust toolchain — **1.91 or later** (`rust-version` in the workspace `Cargo.toml`); the
  `rust-toolchain.toml` here selects stable for local development

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

CI (`.github/workflows/publish-beacondb.yml`, triggered by the release tag `v*`, or by a
`beacondb-v*` tag for a beacondb-only release) builds this way for Linux (manylinux_2_28,
x86_64/aarch64), macOS (arm64 + x86_64), and Windows (x64), then publishes to PyPI via trusted
publishing.

Two honest caveats: the wheel is **large** (it contains a full DataFusion/Lance/netCDF engine), and
there is **no minimal (feature-gated) build** yet.

## The source distribution

An **sdist** is published alongside the wheels, so `pip install beacondb` works on a platform with
no wheel by compiling instead of downloading. maturin walks the path dependencies and emits a
self-contained tree — the beacon-db crates beacondb needs, a trimmed workspace `Cargo.toml`, and
`Cargo.lock` — so the build resolves offline against pinned versions.

### Rust and protoc install themselves

Two of the build prerequisites are provisioned automatically, so a source build does not fail
merely because they are absent:

- **Rust** — maturin (>= 1.8.4) installs a toolchain into a temporary directory when `cargo` is not
  on `PATH`. Set `MATURIN_NO_INSTALL_RUST=1` to forbid that and fail instead. This works on musl
  too, but only *after* a C toolchain is present — see the Alpine note below.
- **protoc** — required by `prost-build` (reached through Lance), which does not bundle it. It is
  listed in `[build-system] requires` as `protoc-wheel-0`, so pip installs it into the build
  environment like any other build dependency.

Both land in pip's isolated build environment, not on your system. The protoc requirement carries
a `platform_machine` marker: wheels exist for x86_64, aarch64, arm64 and AMD64 (manylinux,
musllinux, macOS, Windows), and on anything else — ppc64le, s390x — it is skipped and the build
uses your system `protoc` instead.

Where your package manager offers them, installing Rust and protoc yourself is still faster than
re-provisioning on every build.

### What you still need

A source build uses the crate's **default** features, which link netCDF and HDF5 **dynamically**.
So it needs a C/C++ toolchain and system HDF5/netCDF, unlike the published wheels. To build the
fully static, self-contained variant instead, pass the feature through maturin's PEP 517 hook —
then you need `cmake` rather than the HDF5/netCDF dev packages:

```bash
MATURIN_PEP517_ARGS="--features static-netcdf" pip install beacondb
```

`pip install beacondb --no-binary beacondb` forces the source path on a platform that has a wheel.

## Building on Alpine (musl)

No musllinux wheel is published at the moment, so on Alpine pip falls through to the sdist and
compiles the engine. The supported fix is a glibc-based image (`python:3.12-slim`); building from
source is the fallback if you must stay on musl.

Rust and protoc provision themselves as described above, so Alpine only needs the C toolchain and
the netCDF/HDF5 libraries:

```bash
apk add --no-cache build-base linux-headers hdf5-dev netcdf-dev
pip install beacondb
```

::: warning Install `build-base` before pip, not after
The order matters on Alpine. rustup's musl toolchain links against `libgcc_s.so.1`, which the base
image does not carry — it arrives with `build-base`. Run `pip install` on a bare
`python:3.12-alpine` and the Rust bootstrap downloads a toolchain and then dies with:

```text
Error loading shared library libgcc_s.so.1: No such file or directory (needed by .../bin/cargo)
Error relocating .../bin/cargo: _Unwind_Resume: symbol not found
```

which names neither Rust nor the package you are missing. With `build-base` installed first the
bootstrap succeeds normally.
:::

Adding `rust cargo protobuf-dev` is optional. It is worth it if you build more than once — a
system toolchain skips re-downloading ~200 MB of rustup on every build, and it sidesteps the
ordering trap above entirely.

To skip `hdf5-dev`/`netcdf-dev` and have the build compile netCDF/HDF5 from source into the
extension instead, swap them for `cmake perl` and use the `MATURIN_PEP517_ARGS` form above — more
self-contained, but a considerably longer build.

Either way, expect a long compile: this builds a full DataFusion/Lance/netCDF engine from scratch.
The `static-netcdf` route is the same one CI uses for musllinux wheels, and the workflow still
carries its `apk` branch, so restoring those wheels is a matrix change only.
