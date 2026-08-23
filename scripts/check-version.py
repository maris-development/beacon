#!/usr/bin/env python3
"""Verify every release manifest declares the same version.

    python3 scripts/check-version.py                 # manifests must agree with each other
    python3 scripts/check-version.py v2.0.0-rc.1     # ...and with this tag / expected version

The counterpart to bump-version.py: that one writes the version, this one proves nothing drifted.
Every publish workflow calls it, so a release fails *before* anything is uploaded rather than after
— PyPI and npm do not let a version be replaced.

Checking every manifest (not just the one being published) is deliberate: they all release off the
same tag, so bumping beacondb and forgetting beacon-ts should stop the release, not produce a
half-published version pair.

An empty argument means "no expected version" — a manual run with the version field blank just
confirms the manifests agree with each other and publishes what is committed.
"""

import json
import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _toml(rel: str, *keys: str) -> str:
    data = tomllib.loads((REPO / rel).read_text(encoding="utf-8"))
    for k in keys:
        data = data[k]
    return data


def _json(rel: str, key: str) -> str:
    return json.loads((REPO / rel).read_text(encoding="utf-8"))[key]


# label -> (declared version, spelling). Cargo/npm use SemVer (2.0.0-rc.1), Python uses
# PEP 440 (2.0.0rc1) — the same version, so they are compared parsed rather than as strings.
def declared() -> dict[str, str]:
    return {
        "Cargo.toml [workspace.package]": _toml("Cargo.toml", "workspace", "package", "version"),
        "beacon-db-py/pyproject.toml": _toml(
            "beacon-db/beacon-db-py/pyproject.toml", "project", "version"
        ),
        "beacon-datalake-cli/pyproject.toml": _toml(
            "beacon-clients/beacon-datalake-cli/pyproject.toml", "project", "version"
        ),
        "beacon-ts/package.json": _json(
            "beacon-clients/beacon-ts/package.json", "version"
        ),
    }


def main(expected: str | None) -> None:
    try:
        from packaging.version import InvalidVersion, Version
    except ImportError:
        raise SystemExit("This script needs `packaging` — run: python3 -m pip install packaging")

    found = declared()
    width = max(len(k) for k in found)
    for name, v in found.items():
        print(f"  {name:{width}}  {v}")

    parsed = {name: Version(v) for name, v in found.items()}
    if len(set(parsed.values())) != 1:
        raise SystemExit(
            "::error::Release manifests declare different versions. "
            "Run scripts/bump-version.py to set them all."
        )
    release = next(iter(parsed.values()))

    if expected:
        # Tags may be `v2.0.0-rc.1` or `beacondb-v2.0.0-rc.1`.
        wanted = expected.removeprefix("beacondb-").removeprefix("v")
        try:
            want = Version(wanted)
        except InvalidVersion:
            raise SystemExit(f"::error::{expected!r} is not a valid version")
        if want != release:
            raise SystemExit(
                f"::error::Tag/input says {wanted}, but the manifests declare {release}. "
                "Run scripts/bump-version.py, commit, then tag."
            )

    print(f"publishing {release}")


if __name__ == "__main__":
    if len(sys.argv) > 2:
        raise SystemExit("usage: check-version.py [expected-version]")
    main(sys.argv[1] if len(sys.argv) == 2 else None)
