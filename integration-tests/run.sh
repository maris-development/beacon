#!/usr/bin/env bash
# Convenience runner for the Beacon Docker integration tests (POSIX shells).
#
# Usage:
#   ./run.sh                                  # build the local Dockerfile and run the suite
#   BEACON_IMAGE=ghcr.io/maris-development/beacon:latest ./run.sh   # reuse an image
#
# Any extra arguments are passed through to pytest, e.g.:
#   ./run.sh -k external -v
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d .venv ]; then
    python3 -m venv .venv
fi
# shellcheck disable=SC1091
. .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
pytest "$@"
