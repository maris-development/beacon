#!/usr/bin/env bash
# The engine may not depend on the application.
#
# `beacon-db/` is beacon's embedded SQL database; `beacon-datalake/` is the
# application built on it. Dependencies run one way only. This check makes that
# rule mechanical instead of conventional — a path dependency pointing from the
# engine into the application is a layering violation, whatever it is called.
set -euo pipefail

violations=$(grep -rn --include=Cargo.toml 'path *= *"[^"]*beacon-datalake' beacon-db/ || true)

if [[ -n "$violations" ]]; then
    echo "error: beacon-db depends on beacon-datalake — the engine must not depend on the application:" >&2
    echo "$violations" >&2
    exit 1
fi

echo "layer boundary ok: beacon-db has no dependency on beacon-datalake"
