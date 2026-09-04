---
# The documentation entry point, and the whole page: no theme slots, and one
# shared component. `layout: home` with neither `hero:` nor `features:` renders
# only the markdown below, inside the same `vp-doc` container every documentation
# page uses, so this page reads in the type and spacing of the rest of the site.
#
# The product story lives on beacon-datalake.org, which links straight into
# /docs/latest. Keep this page navigational and let that site do the pitch.
#
# Raw HTML is not base-rewritten by VitePress (only markdown links are), so
# every `href` here is relative. It resolves against `base` on its own.
#
# <SystemDiagram /> is the same component the 2.0 introduction page uses under
# "How it fits together". It is registered globally in .vitepress/theme/index.js
# and takes no props, so both pages show one overview and there is one copy of it.
#
# The copy follows ASD-STE100, like the rest of the 2.0 documentation: active
# voice, simple present, one thought per sentence, no em-dashes, and one word
# for one meaning. "Server" is the thing you run. Do not write "node" for it.
layout: home
title: Documentation
description: Beacon documentation. Point a server at your archive. Connect a client. Write queries. Read every version, format and setting.
---

<div class="dhead">

# Beacon documentation

Point a server at your archive. Connect a client. Write queries.

</div>

<SystemDiagram />

## Choose a version

<div class="dgrid">
  <a class="dcard" href="docs/2.0.0-rc5/introduction">
    <span class="dcard-head">
      <span class="dcard-title mono">2.0.0-rc5</span>
      <span class="dcard-tag">Pre-release</span>
    </span>
    <span class="dcard-body">This is the upcoming 2.0 version release.</span>
    <span class="dcard-cta is-primary">Read the docs <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg></span>
  </a>
  <a class="dcard" href="docs/1.8.0/introduction">
    <span class="dcard-head">
      <span class="dcard-title mono">1.8.0</span>
      <span class="dcard-tag is-stable">Stable</span>
    </span>
    <span class="dcard-body">Read this for a 1.8.0 server.</span>
    <span class="dcard-cta">Read the docs <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg></span>
  </a>
  <a class="dcard" href="docs/1.7.3/introduction">
    <span class="dcard-head">
      <span class="dcard-title mono">1.7.3</span>
      <span class="dcard-tag">Older</span>
    </span>
    <span class="dcard-body">Read this for a 1.7.3 server.</span>
    <span class="dcard-cta">Read the docs <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg></span>
  </a>
</div>

## Common tasks

<div class="dgrid">
  <a class="dcard" href="docs/2.0.0-rc5/getting-started">
    <span class="dcard-icon"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><path d="M6 6h.01"/><path d="M6 18h.01"/></svg></span>
    <span class="dcard-title">Deploy a server</span>
    <span class="dcard-body">Point Beacon at a directory or a bucket. Set the ports, the storage and the limits.</span>
  </a>
  <a class="dcard" href="docs/2.0.0-rc5/connect/python">
    <span class="dcard-icon"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 18l6-6-6-6"/><path d="M4 4v16"/><path d="M20 4v16"/></svg></span>
    <span class="dcard-title">Connect a client</span>
    <span class="dcard-body">Use Python, TypeScript or the terminal client. DataGrip and DBeaver use Arrow Flight SQL.</span>
  </a>
  <a class="dcard" href="docs/2.0.0-rc5/sql/">
    <span class="dcard-icon"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0 0 18 0V5"/><path d="M3 12a9 3 0 0 0 18 0"/></svg></span>
    <span class="dcard-title">Write SQL</span>
    <span class="dcard-body">Query your files with SELECT, JOIN and UNION BY NAME. Beacon adds 123 spatial functions.</span>
  </a>
  <a class="dcard" href="docs/2.0.0-rc5/api/">
    <span class="dcard-icon"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg></span>
    <span class="dcard-title">Call the API</span>
    <span class="dcard-body">One endpoint accepts SQL or a JSON query. It answers in Arrow, Parquet, CSV, NetCDF or ODV.</span>
  </a>
  <a class="dcard" href="docs/2.0.0-rc5/formats/">
    <span class="dcard-icon"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 22V7a1 1 0 0 0-1-1H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-5a1 1 0 0 0-1-1H2"/><rect x="14" y="2" width="8" height="8" rx="1"/></svg></span>
    <span class="dcard-title">Read your formats</span>
    <span class="dcard-body">Beacon reads NetCDF, Zarr, Parquet, GeoParquet, CSV, ODV, HDF5, Arrow, GeoTIFF, Iceberg, Delta and BBF.</span>
  </a>
  <a class="dcard" href="docs/2.0.0-rc5/server/configuration">
    <span class="dcard-icon"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 7h-9"/><path d="M14 17H5"/><circle cx="17" cy="17" r="3"/><circle cx="7" cy="7" r="3"/></svg></span>
    <span class="dcard-title">Tune and fix</span>
    <span class="dcard-body">Set every BEACON_* variable. Find the answer for a common error.</span>
  </a>
</div>
