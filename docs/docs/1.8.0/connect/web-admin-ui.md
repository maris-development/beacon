---
description: Beacon ships a bundled admin web UI — an Athena-style query workbench plus pages to manage tables, datasets, crawlers and external tables, served at /admin straight from the Beacon server and Docker image.
---

# Admin Web UI

Beacon ships with a bundled **admin web interface** — an Athena-style query
workbench and data-lake admin console. It is built into the Beacon server and the
official Docker image, so there is nothing extra to deploy: when a build is
present, the server serves it at **`/admin`**.

```
http://localhost:5001/admin
```

The UI is a React single-page app (Vite, Tailwind CSS, shadcn/ui) that talks to
Beacon exclusively through the [`@beacon/client`](/docs/1.8.0/connect/beacon-typescript-sdk)
TypeScript SDK.

## Logging in

The UI is **admin-only**: a login screen gates the whole app. Sign in with the
Beacon server URL and the admin Basic-auth credentials configured on the server:

```bash
BEACON_ADMIN_USERNAME=beacon-admin
BEACON_ADMIN_PASSWORD=beacon-password
```

Credentials are verified against `GET /api/admin/check` and stored in the
browser's `localStorage`, then sent on every request.

:::warning
This is browser-side gating over Beacon's HTTP Basic admin auth — it restricts
who uses the UI, it is not a secret-keeping mechanism. Serve Beacon over HTTPS
and expose `/admin` only to trusted operators.
:::

## Features

- **Query editor** — an Athena-style workbench: a searchable table/column data
  panel, a CodeMirror SQL editor (run with <kbd>⌘</kbd>/<kbd>Ctrl</kbd> +
  <kbd>Enter</kbd>), a results grid, and CSV / Parquet download. **Explain**
  renders the query's logical plan as a collapsible tree, and queries can be
  **saved** to the browser and reloaded.
- **Tables** — browse registered tables, their Arrow schemas and table
  configuration, register an [external table](/docs/1.8.0/data-lake/external-tables)
  over files in the datasets store, and drop a table (`DROP TABLE`, leaving the
  underlying files in place).
- **Datasets** — explore discovered dataset files and inspect each file's schema.
- **Crawlers** — list, [create, run, and delete crawlers](/docs/1.8.0/data-lake/crawlers).
- **Users & roles** — manage [role-based access control](/docs/1.8.0/security/access-control):
  users, roles, and privileges.
- **Server** — runtime info, health, and the available scalar / table functions.
- **Light / dark / system theme** — switch from the top bar; the choice persists
  in the browser.

## Running it standalone

The bundled copy is enough for most deployments. To run the UI from source (for
development) it lives in the `clients/` npm workspace and depends on the SDK,
which must be built first:

```bash
# from clients/
npm install                       # installs the JS workspace (beacon-ts + beacon-web)
npm run build -w @beacon/client   # build the SDK so beacon-web can import it
npm run dev -w @beacon/web        # start the Vite dev server
```

Point it at any running Beacon server; the default CORS policy permits the dev
server to call the API directly. The app source is in
[`clients/beacon-web`](https://github.com/maris-development/beacon/tree/main/clients/beacon-web).
