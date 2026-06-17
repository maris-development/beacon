#!/usr/bin/env bash
# Copy generated ./data into the native Docker volume `<project>_benchdata`.
#
# Why: on Docker Desktop (Windows/Mac) a bind-mounted host dir is read through a slow
# gRPC-FUSE/virtiofs layer. Engines reading Parquet through that would be measuring the
# mount overhead, not the engine. A native volume (ext4 in the Linux VM) gives real I/O.
#
# Run AFTER generating data and BEFORE `docker compose up` (or restart the engines after,
# so they re-scan). Re-run whenever you regenerate the dataset.
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${COMPOSE_PROJECT_NAME:-beaconbench}"
VOL="${PROJECT}_benchdata"

docker volume create "$VOL" >/dev/null
echo "Streaming ./data -> volume $VOL ..."
# Stream a tar over stdin rather than bind-mounting the host dir: avoids Docker Desktop's
# slow FUSE mount AND Windows/MSYS path-translation issues. tar reads the host FS directly.
tar -C data -cf - . | docker run --rm -i -v "$VOL:/dst" alpine \
  sh -c "rm -rf /dst/* && tar -C /dst -xf - && echo 'volume contents:' && ls -la /dst"
echo "Done. Volume $VOL is ready for the engines."
