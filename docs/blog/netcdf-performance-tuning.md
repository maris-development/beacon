
# NetCDF Performance Tuning in Beacon 1.5

If NetCDF performance has been the bottleneck in your stack, Beacon 1.5 gives you a simple, high‑impact fix. The new NetCDF multiplexer (MPIO) path makes reads faster and more predictable when many people are querying data at the same time. This post explains what it is, how it works, and why you’ll want it turned on.

Beacon Arrow NetCDF MPIO is Beacon’s multi‑process NetCDF reader. It is not MPI‑IO. Instead, Beacon runs several small helper programs (called “workers”) that open NetCDF files and read data in parallel. That means better isolation from the NetCDF/HDF5 libraries and noticeably higher throughput under real‑world load.

## The core idea

Beacon keeps the query engine responsive by moving heavy NetCDF reads into separate processes. Each worker opens NetCDF files, converts the data into Arrow batches (a standard columnar format), and sends them back to the main process using a lightweight protocol. You get the speed of parallelism without changing your datasets or client code.

At a high level, Beacon does four things:

1. **Enables the multiplexer**: Beacon routes schema reads and batch reads through the MPIO worker pool.
2. **Dispatches requests**: a dispatcher queues read requests and assigns them to idle workers.
3. **Reads in workers**: each worker reads NetCDF data and encodes it as Arrow IPC.
4. **Returns Arrow batches**: the main process receives the Arrow payload and continues query execution.

## How it works in Beacon

The multiplexer is implemented as a small client + worker protocol:

- The **client** lives inside Beacon. It sends read requests through an internal queue.
- A **dispatcher** assigns each request to an idle worker (first in, first out). Each worker processes one request at a time.
- The **worker process** (`beacon-arrow-netcdf-mpio`) opens NetCDF files and returns Arrow data back to Beacon.

The protocol is intentionally simple:

- Control messages are **length-prefixed JSON frames** (small messages that say what to read).
- For batch reads, the response is followed by the **raw Arrow data**.

This approach keeps the main runtime non‑blocking while still letting NetCDF/HDF5 do its work in isolated processes. The result: smoother dashboards, faster queries, and fewer latency spikes.

## Why this helps performance

Beacon Arrow NetCDF MPIO improves performance in two ways:

- **Parallel reads**: multiple NetCDF files can be read at the same time across worker processes.
- **Isolation**: heavy native library work is kept out of the main server loop, so other queries stay responsive.

In practice, this means higher throughput and more predictable latency when many queries hit NetCDF datasets. If you’ve ever watched NetCDF reads slow everything else down, this is the change that fixes it.

## How to enable it

The multiplexer is controlled by environment variables (simple settings you can add to your deployment). Flip a switch, size the worker pool, and you’re done:

- `BEACON_ENABLE_MULTIPLEXER_NETCDF=true`
- `BEACON_NETCDF_MULTIPLEXER_PROCESSES` (defaults to half of CPU cores)
- `BEACON_NETCDF_MPIO_WORKER` (optional path to the worker binary)
- `BEACON_NETCDF_MPIO_REQUEST_TIMEOUT_MS` (optional per-request timeout)

## Example results (illustrative)

The table below shows typical improvements for concurrent NetCDF reads when enabling Beacon Arrow NetCDF MPIO. These numbers are illustrative.

| Scenario | Serial NetCDF reads (MB/s) | Beacon Arrow NetCDF MPIO (MB/s) | Speedup |
| --- | --- | --- | --- |
| 4 workers, 20 concurrent queries | 220 | 640 | 2.9× |
| 8 workers, 50 concurrent queries | 260 | 1,050 | 4.0× |
| 16 workers, 100 concurrent queries | 240 | 1,900 | 7.9× |

## Takeaway

Beacon 1.5 gives you a straightforward performance lever for NetCDF: enable the MPIO multiplexer, size the worker pool, and let Beacon handle the rest. If NetCDF reads are a bottleneck in your deployment, this is the fastest way to unlock real performance gains and keep users happy.
