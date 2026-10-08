"""The CLI reports the version of the installed package."""

from __future__ import annotations

from importlib.metadata import version

from typer.testing import CliRunner

from beacon_datalake_cli import __version__
from beacon_datalake_cli.cli import app


def test_version_matches_the_package_metadata() -> None:
    assert __version__ == version("beacon-datalake-cli")


def test_version_option_prints_the_package_version() -> None:
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    assert f"beacon-datalake-cli {version('beacon-datalake-cli')}" in result.output
