---
description: Build the BeaconDB wheel from source. This page covers the native toolchain that the engine links, and the portable static-netcdf build.
---

# Building from source

`beacondb` holds the whole engine. A build of the wheel therefore needs the native toolchain that the
engine links. A Rust compiler alone is not enough:

- **protoc**. Lance generates protobuf at build time.
- **HDF5 and netCDF** headers and libraries. The netCDF reader and writer need them.
- **PROJ, 9.6.2 or later**, and pkg-config. The `ST_Transform` spatial function links it. A
  machine without it still builds: the build compiles PROJ from source instead, which needs
  **cmake** and the **sqlite3** program. `--no-default-features` drops `ST_Transform` and PROJ
  with it. The other 122 spatial functions stay.
- A Rust toolchain, **1.94 or later**. See `rust-version` in the workspace `Cargo.toml`. The
  `rust-toolchain.toml` file here selects stable for local development.

```bash
# macOS
brew install protobuf hdf5 netcdf proj pkg-config
# Debian/Ubuntu
sudo apt-get install -y protobuf-compiler libhdf5-dev libnetcdf-dev libproj-dev pkg-config

pip install maturin
maturin develop            # build + install into the current venv (debug)
maturin build --release    # produce a wheel in ./target/wheels (or --out dist)
```

The wheel is **abi3** (`cp310-abi3`). One wheel per platform therefore covers CPython 3.10 and later.
The wheel ships `py.typed` and the `_beacondb.pyi` type stubs. The stubs include the `read_*` readers
from the catalog.

## Portable wheels (`static-netcdf`, `spatial-proj-bundled`)

A wheel for distribution links every native library **statically**. The build compiles netCDF, HDF5
and PROJ from source. The wheel then carries them and needs no system library. This is the only way
to ship a portable **Windows** wheel:

```bash
# needs protoc, cmake and the sqlite3 program
maturin build --release --features static-netcdf,spatial-proj-bundled
```

PROJ keeps its CRS database, `proj.db`, in a file beside the library. A static build embeds that
database in the library instead, so the wheel holds it and needs no `PROJ_DATA`. The build writes
that database from SQL with the `sqlite3` program, which is why the toolchain lists it. The SQLite
*library* comes from the build itself, on every platform but Windows. Windows takes it from vcpkg,
because `proj-sys` passes the built one on under a name that MSVC does not write. The release
workflow does that step.

CI builds this way for Linux (manylinux_2_34, x86_64 and aarch64), macOS (arm64) and Windows (x64).
The Linux wheels add `vendored-openssl`, which the MySQL and PostgreSQL drivers need there. CI then
publishes to PyPI with trusted publishing. The workflow is
`.github/workflows/publish-beacondb.yml`. The release tag `v*` starts it. A `beacondb-v*` tag starts
a beacondb-only release. A manual run with `dry_run` builds and tests every wheel and publishes
none.

There are two drawbacks. The wheel is **large**, because it holds a full DataFusion, Lance and netCDF
engine. There is also **no minimal build** with feature gates yet.

## The source distribution

Beacon publishes an **sdist** next to the wheels. `pip install beacondb` therefore works on a
platform without a wheel. Pip compiles the package instead of a download. maturin walks the path
dependencies and writes a self-contained tree. The tree holds the beacon-db crates that beacondb
needs, a reduced workspace `Cargo.toml` and `Cargo.lock`. The build therefore resolves offline
against pinned versions.

### Rust and protoc install themselves

The build provides two prerequisites automatically. A source build therefore does not fail because
they are absent:

- **Rust**. maturin 1.8.4 and later installs a toolchain into a temporary directory when `cargo` is
  not on `PATH`. Set `MATURIN_NO_INSTALL_RUST=1` to forbid this. The build then fails instead. This
  also works on musl, but only *after* a C toolchain is present. See the Alpine note below.
- **protoc**. `prost-build` needs it, through Lance. `prost-build` does not include it. The
  `[build-system] requires` section lists it as `protoc-wheel-0`. Pip therefore installs it into the
  build environment, like any other build dependency.

Both land in the isolated build environment of pip, not on your system. The protoc requirement
carries a `platform_machine` marker. Wheels exist for x86_64, aarch64, arm64 and AMD64, on
manylinux, musllinux, macOS and Windows. On any other architecture, such as ppc64le and s390x, pip
skips it. The build then uses your system `protoc`.

Does your package manager offer Rust and protoc? Then install them yourself. This is faster than a
new download on every build.

### What you still need

A source build uses the **default** features of the crate. Those features link netCDF and HDF5
**dynamically**. The build therefore needs a C and C++ toolchain and the system HDF5 and netCDF
packages. It also needs PROJ, or cmake and sqlite3 to build PROJ from source. The published wheels
need none of this. To build the fully static variant, pass the features through the PEP 517 hook of
maturin. You then need `cmake` instead of the HDF5 and netCDF development packages:

```bash
MATURIN_PEP517_ARGS="--features static-netcdf,spatial-proj-bundled" pip install beacondb
```

`pip install beacondb --no-binary beacondb` forces a source build on a platform with a wheel.

## Building on Alpine (musl)

Beacon publishes no musllinux wheel at this moment. On Alpine, pip therefore uses the sdist and
compiles the engine. The supported fix is a glibc image such as `python:3.12-slim`. A build from
source is the fallback if you must stay on musl.

Rust and protoc install themselves, as described above. Alpine therefore needs only the C toolchain,
the netCDF and HDF5 libraries, and what a source build of PROJ uses:

```bash
apk add --no-cache build-base linux-headers hdf5-dev netcdf-dev cmake sqlite
pip install beacondb
```

The PROJ package of Alpine can be older than the 9.6.2 floor. The build then compiles PROJ itself,
and `cmake` and `sqlite` are for that step. Add `proj-dev` where the package is new enough. The
build links that copy and skips the source build.

::: warning Install `build-base` before pip, not after
The order matters on Alpine. The musl toolchain of rustup links against `libgcc_s.so.1`. The base
image does not hold that library. `build-base` brings it. Run `pip install` on a bare
`python:3.12-alpine` and the Rust bootstrap downloads a toolchain. It then fails with:

```text
Error loading shared library libgcc_s.so.1: No such file or directory (needed by .../bin/cargo)
Error relocating .../bin/cargo: _Unwind_Resume: symbol not found
```

The message names neither Rust nor the missing package. Install `build-base` first and the bootstrap
succeeds.
:::

You can also add `rust cargo protobuf-dev`. This is optional. It helps if you build more than once. A
system toolchain avoids a download of about 200 MB of rustup on every build. It also avoids the order
problem above.

You can skip `hdf5-dev` and `netcdf-dev`. The build then compiles netCDF and HDF5 from source into
the extension. Replace those two packages with `cmake perl` and use the `MATURIN_PEP517_ARGS` form
above. The result is more self-contained, but the build takes much longer.

Both routes need a long compile. The build makes a full DataFusion, Lance and netCDF engine from
scratch. CI uses the same `static-netcdf` route for the musllinux wheels. The workflow still holds
its `apk` branch. A change to the matrix therefore restores those wheels.
