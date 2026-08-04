#!/usr/bin/env bash
# The engine may not depend on the application.
#
# `beacon-db/` is beacon's embedded SQL database; `beacon-server/` is the
# application built on it. Dependencies run one way only. This check makes that
# rule mechanical instead of conventional — a path dependency pointing from the
# engine into the application is a layering violation, whatever it is called.
#
# It is also the licence boundary: beacon-db is Apache-2.0 and beacon-server is
# AGPL-3.0-only. Apache-2.0 combines into AGPL-3.0 but not the reverse, so an edge
# in this direction would relicense the engine by accident. See LICENSING.md.
set -euo pipefail

violations=$(grep -rn --include=Cargo.toml 'path *= *"[^"]*beacon-server' beacon-db/ || true)

if [[ -n "$violations" ]]; then
    echo "error: beacon-db depends on beacon-server — the engine must not depend on the application:" >&2
    echo "$violations" >&2
    exit 1
fi

echo "layer boundary ok: beacon-db has no dependency on beacon-server"
