---
layout: home

hero:
  name: "Beacon"
  text: "One SQL engine for scientific data, embed it or serve it"
  tagline: "Query NetCDF, Zarr, Parquet, GeoTIFF and more in place, from local files or S3, with one SQL dialect. Embed the engine in Python with BeaconDB, or serve it to your whole team with Beacon Data Lake."
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
  - title: Fast analytical queries
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 14a1 1 0 0 1-.78-1.63l9.9-10.2a.5.5 0 0 1 .86.46l-1.92 6.02A1 1 0 0 0 13 10h7a1 1 0 0 1 .78 1.63l-9.9 10.2a.5.5 0 0 1-.86-.46l1.92-6.02A1 1 0 0 0 11 14z"/></svg>'
    details: Built in Rust on Apache Arrow and DataFusion, with filter and projection pushdown that scans only the bytes a query needs across large scientific datasets.

  - title: Works with existing formats
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 22V7a1 1 0 0 0-1-1H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-5a1 1 0 0 0-1-1H2"/><rect x="14" y="2" width="8" height="8" rx="1"/></svg>'
    details: Query NetCDF, Zarr, Parquet, GeoParquet, CSV, ODV, HDF5, Arrow, GeoTIFF, Delta and BBF datasets in place. No need to convert everything into a proprietary warehouse first.

  - title: One SQL, everywhere
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>'
    details: The exact same SQL dialect and formats either way. Create external and managed tables, views and secrets, and ATTACH remote Beacons, whether embedded or served.

  - title: Open source and self-hosted
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19.414 14.414C21 12.828 22 11.5 22 9.5a5.5 5.5 0 0 0-9.591-3.676.6.6 0 0 1-.818.001A5.5 5.5 0 0 0 2 9.5c0 2.3 1.5 4 3 5.5l5.535 5.362a2 2 0 0 0 2.879.052 2.12 2.12 0 0 0-.004-3 2.124 2.124 0 1 0 3-3 2.124 2.124 0 0 0 3.004 0 2 2 0 0 0 0-2.828l-1.881-1.882a2.41 2.41 0 0 0-3.409 0l-1.71 1.71a2 2 0 0 1-2.828 0 2 2 0 0 1 0-2.828l2.823-2.762"/></svg>'
    details: Released under AGPL-3.0. Run it on your laptop, your servers, or your own cloud, so you keep full control over deployment, data access, and infrastructure.

---
