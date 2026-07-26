---
description: The BeaconDB:// SQLAlchemy dialect, use pandas.read_sql, reflection, notebooks and BI tools against an embedded beacondb.
---

# SQLAlchemy

A `beacondb://` dialect ships with the package (`pip install "beacondb[sqlalchemy]"`), so the
SQLAlchemy ecosystem, `pandas.read_sql`, reflection, notebooks, BI tools, works out of the box:

```python
from sqlalchemy import create_engine
engine = create_engine("beacondb:///beacon.db")     # or "beacondb://" for in-memory
# auth and options ride on the URL query:
#   beacondb:///beacon.db?auth=true&username=u&password=p&datasets=/data

import pandas as pd
pd.read_sql("SELECT platform, avg(temperature) AS t FROM obs GROUP BY platform", engine)
```

Reflection (`inspect(engine).get_table_names()`, `get_columns(...)`, `has_table(...)`) is answered from
Beacon's `information_schema`. The engine is autocommit, `commit()`/`rollback()` are no-ops, since
Beacon has no multi-statement transactions.
