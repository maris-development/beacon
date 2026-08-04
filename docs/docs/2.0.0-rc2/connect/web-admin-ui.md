---
description: Beacon includes an admin web UI at /admin. It gives a query workbench and pages for tables, datasets, crawlers and external tables.
---

# Admin Web UI

Beacon includes an **admin web interface**. It is a query workbench and a data
lake admin console. The Beacon server and the official Docker image hold it. You
deploy nothing extra. The server serves the UI at **`/admin`**.

```
http://localhost:5001/admin
```

The UI is a React single-page application. It uses Vite, Tailwind CSS and
shadcn/ui. It reaches Beacon through the
[`@beacon/client`](/docs/2.0.0-rc2/connect/typescript) TypeScript SDK
only.

## Log in

The UI is **for an admin only**. A login screen protects the whole application.
Sign in with the URL of the Beacon server. Also give the admin Basic auth
credentials of that server:

```bash
BEACON_ADMIN_USERNAME=beacon-admin
BEACON_ADMIN_PASSWORD=beacon-password
```

The UI checks the credentials with `GET /api/admin/check`. It stores them in the
`localStorage` of the browser. It then sends them on every request.

:::warning
This login runs in the browser, over the HTTP Basic admin auth of Beacon. It
controls who uses the UI. It keeps no secret. Serve Beacon over HTTPS. Give
`/admin` only to operators that you trust.
:::

## Features

- **Query editor**: a workbench with a data panel for tables and columns. The
  panel has a search. It also has a CodeMirror SQL editor. Run a query with
  <kbd>⌘</kbd>/<kbd>Ctrl</kbd> + <kbd>Enter</kbd>. The editor shows a results
  grid. You can download CSV or Parquet. **Explain** shows the logical plan as a
  tree. You can also **save** a query in the browser and load it again.
- **Tables**: browse the registered tables, their Arrow schemas and their
  configuration. Register an
  [external table](/docs/2.0.0-rc2/data-sources/external-tables) over the
  files in the datasets store. Drop a table with `DROP TABLE`. Beacon keeps the
  files.
- **Datasets**: explore the dataset files that Beacon finds. Inspect the schema of
  each file.
- **Crawlers**: list, [create, run and delete crawlers](/docs/2.0.0-rc2/server/crawlers).
- **Users and roles**: manage the
  [role-based access control](/docs/2.0.0-rc2/security/access-control). This covers
  users, roles and privileges.
- **Server**: the runtime information, the health and the available scalar and
  table functions.
- **Light, dark and system theme**: change it in the top bar. The browser keeps
  your choice.

## Run it standalone

The included copy is enough for most deployments. You can also run the UI from
source, for development. The source lives in the `beacon-clients/` npm
workspace. It depends on the SDK. Build the SDK first:

```bash
# from beacon-clients/
npm install                       # installs the JS workspace (beacon-ts + beacon-web)
npm run build -w @beacon/client   # build the SDK so beacon-web can import it
npm run dev -w @beacon/web        # start the Vite dev server
```

Point the UI at any Beacon server. The default CORS policy lets the development
server call the API directly. The application source is in
[`beacon-clients/beacon-web`](https://github.com/maris-development/beacon/tree/main/beacon-clients/beacon-web).
