# Local ERDDAP for the ERDDAP tests

This directory holds a local ERDDAP server for the ignored tests in `beacon-db/beacon-core/tests/erddap_tables.rs`.
The server gives the tabledap dataset `beaconTest`.
The data is in `data/beacon_test.csv`. Each row tests one edge case of the filter pushdown.

## Start the server

Run this command from the repository root:

```sh
docker compose -f beacon-db/beacon-file-formats/beacon-erddap/docker/docker-compose.yml up -d
```

The server uses host port 8089 on 127.0.0.1 only.

## Wait for the server

The server is ready when the dataset info returns HTTP 200.
Startup usually takes less than one minute. This loop stops after 5 minutes:

```sh
for i in $(seq 1 60); do
  curl -sf -o /dev/null http://localhost:8089/erddap/info/beaconTest/index.json && break
  sleep 5
done
```

## Run the tests

Run the pushdown test:

```sh
BEACON_ERDDAP_URL=http://localhost:8089/erddap \
  cargo test -p beacon-core --no-default-features --test erddap_tables -- --ignored local_erddap
```

Run the live test on the same server:

```sh
BEACON_ERDDAP_URL=http://localhost:8089/erddap BEACON_ERDDAP_TABLEDAP_ID=beaconTest \
  cargo test -p beacon-core --no-default-features --test erddap_tables -- --ignored live
```

Add `--nocapture` to see each query, its counts and its request URL.

## Stop the server

```sh
docker compose -f beacon-db/beacon-file-formats/beacon-erddap/docker/docker-compose.yml down
```

## Notes

- `datasets.xml` holds one `dataset` element. The image puts it into its `datasets.xml` at startup (datasets.d mode).
- After you change the CSV, stop and start the server again.
- ERDDAP does not read a CSV field that holds a line break. It makes a new row from the second line.
