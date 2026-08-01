#!/usr/bin/env bash
# Reproduce the CI beacondb wheel build locally, in the same container CI uses.
#
# The Linux wheel jobs in `.github/workflows/publish-beacondb.yml` only run on a
# `beacondb-v*` tag, and they build inside manylinux/musllinux containers whose toolchain
# is nothing like a dev machine's — an AlmaLinux 8 base with a 2017-era protoc, or Alpine
# with musl. Toolchain breakage there is invisible until a release is already tagged. This
# script runs that same build on demand, in the same container images, installing the toolchain
# by reading the `before-script-linux:` block straight out of publish-beacondb.yml — so a
# failure here is the failure CI would hit, not the failure of a stale copy.
#
# Usage:
#   scripts/build-wheel-docker.sh                       # manylinux, host arch, full wheel
#   scripts/build-wheel-docker.sh --libc musllinux      # Alpine/musl wheel
#   scripts/build-wheel-docker.sh --arch x86_64         # cross-arch via emulation (slow)
#   scripts/build-wheel-docker.sh --deps-only           # toolchain check only (~1 min)
#   scripts/build-wheel-docker.sh --features static-netcdf   # override the cargo features
#
# `--deps-only` stops after installing the build toolchain and printing protoc/cmake
# versions. That is the cheap pre-flight: it catches the whole class of "the container's
# protoc/cmake is too old" failures without waiting for netCDF and HDF5 to compile from
# source, which the full build does and which takes a long while the first time.
#
# Wheels land in target/docker-wheels/. Cargo's registry and target dir live in named
# Docker volumes, so repeat runs reuse the compiled dependencies.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

libc=manylinux
deps_only=0
features=""
case "$(uname -m)" in
    arm64 | aarch64) arch=aarch64 ;;
    *) arch=x86_64 ;;
esac

while [[ $# -gt 0 ]]; do
    case "$1" in
        --libc) libc="$2"; shift 2 ;;
        --arch) arch="$2"; shift 2 ;;
        --features) features="$2"; shift 2 ;;
        --deps-only) deps_only=1; shift ;;
        # The header comment is the help text: everything from line 2 up to the first
        # non-comment line.
        -h | --help) sed -n '2,${/^[^#]/q;p;}' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown argument: $1 (try --help)" >&2; exit 1 ;;
    esac
done

case "$libc" in
    manylinux) image="quay.io/pypa/manylinux_2_28_${arch}"; rust_host="${arch}-unknown-linux-gnu" ;;
    musllinux) image="quay.io/pypa/musllinux_1_2_${arch}"; rust_host="${arch}-unknown-linux-musl" ;;
    *) echo "--libc must be manylinux or musllinux" >&2; exit 1 ;;
esac

case "$arch" in
    aarch64) platform=linux/arm64 ;;
    x86_64) platform=linux/amd64 ;;
    *) echo "--arch must be aarch64 or x86_64" >&2; exit 1 ;;
esac

# beacon-binary-format is a submodule; without it the workspace does not build at all, and
# inside the container the failure looks like an unrelated missing-crate error.
if [[ ! -e "$repo_root/beacon-db/beacon-file-formats/beacon-binary-format/Cargo.toml" ]]; then
    echo "error: the beacon-binary-format submodule is not checked out. Run:" >&2
    echo "  git submodule update --init beacon-db/beacon-file-formats/beacon-binary-format" >&2
    exit 1
fi

# Match the feature set the release workflow uses. `vendored-openssl` exists on some branches
# and not others; picking it up from the manifest keeps this build identical to CI's on
# whichever branch is checked out, instead of silently testing a different configuration.
if [[ -z "$features" ]]; then
    features=static-netcdf
    if grep -q '^vendored-openssl *=' "$repo_root/beacon-db/beacon-db-py/Cargo.toml"; then
        features="$features,vendored-openssl"
    fi
fi

rust_version=$(tr -d '[:space:]' < "$repo_root/rust-toolchain")
# Pinned exactly, like the workspace's other build-tool pins. maturin-action resolves its own
# maturin release, so this only has to be new enough to speak the same CLI.
maturin_version=1.14.1
builder_image="beacon-wheel-builder:${libc}-${arch}"
out_dir="$repo_root/target/docker-wheels"

echo "==> image      $image ($platform)"
echo "==> toolchain  rust $rust_version, host $rust_host"
echo "==> features   $features"
echo "==> mode       $([[ $deps_only == 1 ]] && echo 'deps-only' || echo 'full wheel build')"

# The toolchain steps come out of publish-beacondb.yml itself, so this builds against exactly
# what the release job installs. A tiny build context (rather than the repo, which carries
# target/ and the test datasets) keeps the daemon from copying gigabytes on every run.
build_ctx=$(mktemp -d)
trap 'rm -rf "$build_ctx"' EXIT
"$repo_root/scripts/wheel-build-deps.sh" > "$build_ctx/before-build-linux.sh"

# The builder layer holds the container toolchain plus rust/maturin, so only the first run
# pays for it. It is invalidated when the workflow's toolchain block or rust-toolchain
# changes — exactly the inputs whose breakage this script exists to catch.
docker build --platform "$platform" -t "$builder_image" -f - "$build_ctx" <<DOCKERFILE
FROM $image

COPY before-build-linux.sh /tmp/before-build-linux.sh
RUN sh /tmp/before-build-linux.sh

ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo
# The cp312 interpreter is what maturin builds the abi3 wheel against; it comes first on
# PATH so \`maturin\` and \`python\` both resolve there.
ENV PATH=/opt/cargo/bin:/opt/python/cp312-cp312/bin:\$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain $rust_version --target $rust_host
RUN /opt/python/cp312-cp312/bin/pip install --no-cache-dir maturin==$maturin_version
DOCKERFILE

if [[ $deps_only == 1 ]]; then
    echo
    echo "==> toolchain check passed for ${libc}/${arch}"
    exit 0
fi

mkdir -p "$out_dir"

# The repo is mounted read-write (maturin writes Cargo.lock timestamps and the version
# bump is not applied here), but the target dir is redirected into a volume: host build
# artifacts are a different libc and arch and must not be mixed in.
compat=$([[ $libc == manylinux ]] && echo manylinux_2_28 || echo musllinux_1_2)

# Drop the cdylib before building. maturin's auditwheel step rewrites the module's DT_NEEDED
# entries *in place*, to the hashed names of the libraries it vendors into the wheel
# (libssl-3f64e418.so.1.1 and friends). Those names exist only inside the wheel, so a second
# run — where cargo has nothing to recompile and hands back the patched file — fails with
# "Cannot repair wheel, because required library ... could not be located". CI never sees
# this because it always starts from an empty target dir; here the target volume is the whole
# point. Deleting the module forces a relink from the cached rlibs, which costs seconds.
docker run --rm --platform "$platform" \
    -v "$repo_root:/work" \
    -v "beacon-wheel-cargo-${libc}-${arch}:/opt/cargo/registry" \
    -v "beacon-wheel-target-${libc}-${arch}:/target" \
    -e CARGO_TARGET_DIR=/target \
    -w /work \
    "$builder_image" \
    sh -c "
        set -eu
        rm -rf /target/maturin
        find /target -name 'lib_beacondb*.so' -delete
        maturin build --release \
            --manifest-path beacon-db/beacon-db-py/Cargo.toml \
            --features '$features' \
            --target '$rust_host' \
            --compatibility '$compat' \
            --out /work/target/docker-wheels
    "

echo
echo "==> wheels in $out_dir"
ls -la "$out_dir"
