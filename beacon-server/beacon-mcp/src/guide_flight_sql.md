
## Join with local files: beacondb

This server accepts anonymous Arrow Flight SQL (gRPC) on port {flight_port}. `beacondb` is an
embedded Python engine. It can query a table of this server as a remote table, next to local
files:

```python
# pip install beacondb
import beacondb

con = beacondb.connect()  # in memory
con.execute(
    "CREATE EXTERNAL TABLE remote_obs STORED AS REMOTE "
    "LOCATION 'beacon://{flight_host}:{flight_port}/<table>'"
)
df = con.sql("SELECT ... FROM remote_obs WHERE ...").df()
```

Beacon sends the filters, the columns and the `LIMIT` to this server. Only the smaller result
travels. Add `OPTIONS ('tls' 'true')` when a TLS proxy serves the Flight SQL port.
