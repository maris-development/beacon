# ERDDAP over Beacon DAP2

This test shows that ERDDAP reads a Beacon table through the dataset type
`EDDTableFromDapSequence`. ERDDAP sends value constraints to Beacon, and Beacon runs them.

## Run the test

1. Start a Beacon server that has the DAP2 endpoint. Use port 5001 and anonymous read (the default).
2. Start ERDDAP:

   ```bash
   docker compose -f integration-tests/erddap-dap2/docker-compose.yml up -d
   ```

3. Run the test:

   ```bash
   BEACON_ERDDAP_E2E=1 pytest integration-tests/erddap-dap2/test_erddap_dap2.py -v
   ```

   The test creates the table `erddap_obs`. ERDDAP loads the dataset in 1 to 2 minutes.

4. Stop ERDDAP:

   ```bash
   docker compose -f integration-tests/erddap-dap2/docker-compose.yml down
   ```

## Environment

| Variable | Default |
| --- | --- |
| `BEACON_URL` | `http://localhost:5001` |
| `ERDDAP_URL` | `http://localhost:8090/erddap` |
| `BEACON_ADMIN_USERNAME` | `admin` |
| `BEACON_ADMIN_PASSWORD` | `securepassword` |

If Beacon uses another port, change `sourceUrl` in `datasets.xml` too.

## Record the results

Record these facts in the pull request or the issue:

- The constraint text that ERDDAP sends. Start Beacon with `RUST_LOG=info,beacon_dap=debug`.
  Read the text from the Beacon log, or from the ERDDAP `log.txt`.
- Whether `sourceUrl` with `user:password@` works against a Beacon table without anonymous read.
