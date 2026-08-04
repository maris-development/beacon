# Licensing

| Path | Licence | Licence file |
|---|---|---|
| Everything, unless listed below | AGPL-3.0 | [`LICENSE`](LICENSE) |
| `beacon-clients/**` | Apache-2.0 | [`beacon-clients/LICENSE`](beacon-clients/LICENSE) |

## The Rust workspace

The root [`LICENSE`](LICENSE) covers it. There is no `license` field in
`[workspace.package]`, and the `beacon-db` crates declare none of their own, so
the root file is the single statement for all of them.

The three `beacon-server` crates restate it as `license = "AGPL-3.0-only"` in
their manifests. They are the crates that could plausibly be published, and a
published crate needs the field.

`beacon-db/beacon-db-py` sets `publish = false` and carries
`Private :: Do Not Upload`. It is an in-tree binding for local builds, not a
distributable — see [Not published](#not-published) below.

## The clients

`beacon-clients/**` is Apache-2.0, declared in each manifest:

- `beacon-clients/beacon-datalake-cli/pyproject.toml`
- `beacon-clients/beacon-ts/package.json`
- `beacon-clients/beacon-web/package.json`

These speak HTTP and Arrow Flight SQL to a server. They link no engine code, so a
permissive licence costs nothing and removes a blocker for institutional users.

## The direction rule

`beacon-server` may depend on `beacon-db`. `beacon-db` must never depend on
`beacon-server`. That is a layering rule, not a licence one now that both sides
are AGPL, but it still holds: the engine is the lower layer.

`scripts/check-layer-boundary.sh` enforces it, and CI runs it on every push.

## Not published

The `beacondb` wheel is no longer built or released. Its GitHub workflow, the
manylinux build scripts and the `make wheel` targets have been removed, and
`scripts/bump-version.py` and `scripts/check-version.py` no longer track its
version.

The crate stays in the workspace and still builds with maturin for local use. It
is simply not a product.

This also resolves a conflict that existed while the engine was Apache-2.0: the
BBF submodule (`beacon-db/beacon-file-formats/beacon-binary-format`) is AGPL-3.0
and is linked by `beacon-core`, so an Apache-2.0 wheel built from that graph would
have been misdeclared. With the engine under the root AGPL and the wheel
unpublished, there is nothing left to reconcile.
