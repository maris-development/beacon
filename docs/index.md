---
layout: home

hero:
  name: "Beacon"
  text: "A modern open-source data lakehouse for scientific data"
  tagline: "Manage, query, and serve climate and scientific datasets from S3 Buckets, or local storage through SQL and JSON APIs."
  image:
    src: /hero.png
    alt: VitePress
  actions:
    - theme: brand
      text: Get Started
      link: /docs/1.6.1/getting-started
    - theme: alt
      text: Read the Docs
      link: /docs/1.6.1/introduction
    - theme: alt
      text: Explore Public Nodes 
      link: /available-nodes/available-nodes

features:
  - title: Fast analytical queries
    details: Built in Rust with Apache Arrow and DataFusion, Beacon is designed for efficient filtering, projection, and retrieval across large scientific datasets.
    icon: 🚀

  - title: Works with existing formats
    details: Query NetCDF, Zarr, Parquet, CSV, ODV, Arrow, TIFF, Atlas and BBF datasets without converting everything into a proprietary warehouse.
    icon: 🧩

  - title: Create tables and views
    details: Define external tables over existing files, create reusable SQL views, and expose curated datasets to downstream users and applications.
    icon: 🧱

  - title: SQL and JSON APIs
    details: Serve scientific data through queryable SQL and JSON interfaces for portals, dashboards, notebooks, services, and data products.
    icon: 🔌

  - title: Query from your favorite tools
    details: Use Arrow Flight SQL to connect Beacon to JDBC-compatible SQL clients such as DataGrip and DBeaver, or query datasets directly through JSON APIs.
    icon: 🧭

  - title: Open source and self-hosted
    details: Beacon is available under the AGPL-3.0 license, giving teams full control over deployment, data access, and infrastructure.
    icon: 🤝

---