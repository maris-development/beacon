#!/usr/bin/env python3
"""Set the release version across every manifest that declares one.

    python3 scripts/bump-version.py 2.0.0-rc.2

Then commit and tag. CI does not rewrite versions — it verifies the tag matches what is
committed — so this script is the only thing that moves a version, and a release fails loudly
if it was not run.

Four ecosystems, three spellings of the same version:

  Cargo.toml [workspace.package]   SemVer     2.0.0-rc.2   (inherited by all 26 release crates)
  */pyproject.toml                 PEP 440    2.0.0rc2     (canonical Python form)
  beacon-ts/package.json           SemVer     2.0.0-rc.2   (+ its lockfiles, via `npm version`)

`beacon-binary-format` (a git submodule) and `beacon-binary-format-toolbox` are deliberately
untouched: they version the binary format, not the beacon release.
"""

import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# Every file whose `version = "..."` line is the beacon release version.
CARGO_WORKSPACE = REPO / "Cargo.toml"
PYPROJECTS = (
    REPO / "beacon-db/beacon-db-py/pyproject.toml",
    REPO / "beacon-clients/beacon-datalake-cli/pyproject.toml",
)
NPM_PACKAGE = REPO / "beacon-clients/beacon-ts"


def pep440(version: str) -> str:
    """SemVer -> canonical PEP 440 (2.0.0-rc.2 -> 2.0.0rc2). Same version, Python spelling."""
    try:
        from packaging.version import Version
    except ImportError:
        raise SystemExit("This script needs `packaging` — run: python3 -m pip install packaging")
    return str(Version(version))


def replace_version(path: Path, new: str, *, first_only: bool = True) -> None:
    """Rewrite the first top-level `version = "..."` line, or fail loudly."""
    text = path.read_text(encoding="utf-8")
    out, n = re.subn(
        r'(?m)^version = "[^"]*"', f'version = "{new}"', text, count=1 if first_only else 0
    )
    if n != 1:
        raise SystemExit(f'{path}: expected one `version = "..."` line, replaced {n}')
    path.write_text(out, encoding="utf-8")
    print(f"  {path.relative_to(REPO).as_posix():54} {new}")


def main(version: str) -> None:
    semver = version.removeprefix("v")
    py = pep440(semver)

    print(f"setting release version to {semver} (Python: {py})")

    # One line covers all 26 crates — they inherit via `version.workspace = true`.
    replace_version(CARGO_WORKSPACE, semver)
    for p in PYPROJECTS:
        replace_version(p, py)

    # npm owns package.json *and* two lockfiles; let it do the writing.
    npm = shutil.which("npm")
    if npm is None:
        raise SystemExit("npm not found — needed to update package.json and its lockfiles")
    subprocess.run(
        [npm, "version", semver, "--no-git-tag-version", "--allow-same-version"],
        cwd=NPM_PACKAGE,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    print(f"  {NPM_PACKAGE.relative_to(REPO).as_posix() + '/package.json':54} {semver}")

    # Refresh Cargo.lock so the workspace member versions inside it match.
    cargo = shutil.which("cargo")
    if cargo is not None:
        subprocess.run(
            [cargo, "metadata", "--format-version", "1", "--offline"],
            cwd=REPO,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print("  Cargo.lock refreshed")
    else:
        print("  cargo not found - run `cargo metadata` to refresh Cargo.lock")

    print("\nnow: review `git diff`, commit, then tag v" + semver)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: bump-version.py <version>   e.g. 2.0.0-rc.2")
    main(sys.argv[1])
