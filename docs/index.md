---
layout: home

hero:
  name: "Beacon"
  text: "Query millions of files with one SQL statement"
  tagline: "Beacon is a data lake query engine for scientific data. One node serves your whole community over SQL, reading NetCDF, Zarr, Parquet and other formats where the files already are. No download. No conversion. No ETL."
  image:
    src: /hero.png
    alt: Beacon
  actions:
    - theme: brand
      text: Get Started
      # Points at 2.0.0-rc3, not the 1.8.0 stable tree. 2.0 is the release that
      # drops the two-product split, so the 1.8.0 pages contradict this page:
      # they still describe "Beacon Data Lake" and know nothing of the current
      # naming. The pre-release banner on those pages carries the caveat.
      # Revisit when 2.0.0 goes GA and LATEST_VERSION moves (theme/version.js).
      link: /docs/2.0.0-rc3/quickstart
    # Was "Explore Public Nodes", pointing at a page whose first line said the
    # nodes were not public. beacon-wod.maris.nl is genuinely open, so this now
    # lands on the section that queries it.
    - theme: alt
      text: Query a live node
      link: /available-nodes/available-nodes

features:
  - title: Any data shape, gridded or ragged
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v16a2 2 0 0 0 2 2h16"/><path d="m19 9-5 5-4-4-3 3"/></svg>'
    details: Argo floats, CTD casts, gliders and moorings give ragged profiles, one schema per file and hundreds of thousands of files. Beacon reads that shape directly.

  - title: Filters skip whole files
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 3H2l8 9.46V19l4 2v-8.54L22 3z"/></svg>'
    details: Beacon keeps per-file statistics for your archive. A filter on time, depth or position drops files before Beacon opens any array. Large archives stay fast.

  - title: One node, every client
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>'
    details: Stand up one server over your archive and let notebooks, portals, dashboards and BI tools query it concurrently over HTTP or Arrow Flight SQL. Role-based grants decide who reads what.

  - title: Query across institutions
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="2"/><path d="M4.93 19.07a10 10 0 0 1 0-14.14"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/><path d="M7.76 16.24a6 6 0 0 1 0-8.48"/><path d="M16.24 7.76a6 6 0 0 1 0 8.48"/></svg>'
    details: ATTACH another node and join its tables against your own in one statement. One query reaches EMODnet and the World Ocean Database, and neither side moves its data.

  - title: Reads your existing formats
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 22V7a1 1 0 0 0-1-1H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-5a1 1 0 0 0-1-1H2"/><rect x="14" y="2" width="8" height="8" rx="1"/></svg>'
    details: NetCDF, Zarr, Parquet, GeoParquet, CSV, ODV, HDF5, Arrow, GeoTIFF, Delta and BBF. Beacon reads each one in place, on local disk or in S3.

  - title: Open source and self-hosted
    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19.414 14.414C21 12.828 22 11.5 22 9.5a5.5 5.5 0 0 0-9.591-3.676.6.6 0 0 1-.818.001A5.5 5.5 0 0 0 2 9.5c0 2.3 1.5 4 3 5.5l5.535 5.362a2 2 0 0 0 2.879.052 2.12 2.12 0 0 0-.004-3 2.124 2.124 0 1 0 3-3 2.124 2.124 0 0 0 3.004 0 2 2 0 0 0 0-2.828l-1.881-1.882a2.41 2.41 0 0 0-3.409 0l-1.71 1.71a2 2 0 0 1-2.828 0 2 2 0 0 1 0-2.828l2.823-2.762"/></svg>'
    details: Beacon is AGPL-3.0, and the clients are Apache-2.0. Run it on your own hardware or your own cloud. Your data never leaves your control.

---
