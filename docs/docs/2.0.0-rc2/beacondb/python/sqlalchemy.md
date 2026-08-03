---
description: The beacondb:// SQLAlchemy dialect. Use pandas.read_sql, reflection, notebooks and BI tools against an embedded beacondb.
---

# SQLAlchemy

The package includes a `beacondb://` dialect. Install it with
`pip install "beacondb[sqlalchemy]"`. The SQLAlchemy ecosystem then works at once. This covers
`pandas.read_sql`, reflection, notebooks and BI tools:

```python
from sqlalchemy import create_engine
engine = create_engine("beacondb:///beacon.db")     # or "beacondb://" for in-memory
# auth and options ride on the URL query:
#   beacondb:///beacon.db?auth=true&username=u&password=p&datasets=/data

import pandas as pd
pd.read_sql("SELECT platform, avg(temperature) AS t FROM obs GROUP BY platform", engine)
```

Reflection uses the `information_schema` of Beacon. It covers
`inspect(engine).get_table_names()`, `get_columns(...)` and `has_table(...)`. The engine commits
automatically. `commit()` and `rollback()` do nothing, because Beacon has no transaction over
several statements.
