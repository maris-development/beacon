---
layout: home

hero:
  name: "Beacon"
  text: "One query engine for scientific data — embed it or serve it"
  tagline: "Query NetCDF, Zarr, Parquet, GeoTIFF and more in place, from local files or S3, with SQL. Embed the engine in Python with beacondb, or run it as a server with beacon-datalake."
  image:
    src: /hero.png
    alt: Beacon
  actions:
    - theme: brand
      text: Get Started
      link: /docs/2.0.0/introduction
    - theme: alt
      text: Explore Public Nodes
      link: /available-nodes/available-nodes

features:
  - title: beacondb — embed it
    icon: 🧪
    details: "pip install beacondb — the whole engine in-process, DuckDB-class, backed by one portable beacon.db file. Query files from a notebook or ship it inside an app."
    link: /docs/2.0.0/beacondb/
    linkText: beacondb docs

  - title: beacon-datalake — serve it
    icon: 🛰️
    details: The same engine as a server — HTTP + Arrow Flight SQL, a datasets store, crawlers, RBAC, a web admin UI, and client SDKs. Serve datasets to many clients.
    link: /docs/2.0.0/getting-started
    linkText: Run the server

  - title: Fast analytical queries
    icon: 🚀
    details: Built in Rust with Apache Arrow and DataFusion, Beacon is designed for efficient filtering, projection, and retrieval across large scientific datasets.

  - title: Works with existing formats
    icon: 🧩
    details: Query NetCDF, Zarr, Parquet, GeoParquet, CSV, ODV, HDF5, Arrow, TIFF, Atlas, Delta and BBF datasets in place — no converting everything into a proprietary warehouse.

  - title: One SQL, everywhere
    icon: 🔌
    details: The same SQL dialect and formats in both products. Create external and managed tables, views, secrets, and ATTACH remote Beacons — embedded or served.

  - title: Open source and self-hosted
    icon: 🤝
    details: Beacon is available under the AGPL-3.0 license, giving teams full control over deployment, data access, and infrastructure.

---
