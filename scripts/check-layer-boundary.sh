#!/usr/bin/env bash
# The engine may not depend on the application.
#
# `beacon-db/` is beacon's embedded SQL database; `beacon-server/` is the
# application built on it. Dependencies run one way only. This check makes that
# rule mechanical instead of conventional — a path dependency pointing from the
# engine into the application is a layering violation, whatever it is called.
#
# It was the licence boundary as well, while the engine was Apache-2.0 and the
# server AGPL-3.0. Both sides are AGPL-3.0 now, so this is a layering rule alone.
# It still holds: the engine is the lower layer.
set -euo pipefail

violations=$(grep -rn --include=Cargo.toml 'path *= *"[^"]*beacon-server' beacon-db/ || true)

if [[ -n "$violations" ]]; then
    echo "error: beacon-db depends on beacon-server — the engine must not depend on the application:" >&2
    echo "$violations" >&2
    exit 1
fi

echo "layer boundary ok: beacon-db has no dependency on beacon-server"
