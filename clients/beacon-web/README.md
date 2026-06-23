# Beacon Admin Web UI

An Athena-style admin web interface for a Beacon instance, built with React, Vite, Tailwind
CSS and shadcn/ui. It talks to Beacon exclusively through the [`@beacon/client`](../beacon-ts)
TypeScript SDK.

The app is **admin-only**: a login screen gates the whole UI. You sign in with a Beacon
server URL and the admin Basic-auth credentials (`BEACON_ADMIN_USERNAME` /
`BEACON_ADMIN_PASSWORD` on the server), which are verified against `GET /api/admin/check`.

> ⚠️ This is browser-side gating over Beacon's HTTP Basic admin auth. Credentials are stored in
> `localStorage` and sent on every request. It restricts who uses the UI; it is not a
> secret-keeping mechanism. Serve it over HTTPS to trusted operators only.

## Features

- **Query editor** — Athena-style workbench: searchable table/column data panel, a CodeMirror
  SQL editor (run with ⌘/Ctrl + Enter), a results grid, and CSV/Parquet download. **Explain**
  renders the query's logical plan as a collapsible tree, and queries can be **saved** to the
  browser and reloaded.
- **Tables** — browse registered tables, their Arrow schemas, and table configuration, and
  drop a table (`DROP TABLE`, leaving the underlying files in place).
- **Datasets** — explore discovered dataset files and inspect each file's schema.
- **Crawlers** — list, create, run, and delete crawlers.
- **External tables** — register an external table over files in the datasets store.
- **Server** — runtime info, health, and the available scalar/table functions.
- **Light / dark / system theme** — switch from the top bar; the choice persists in the browser.

## Prerequisites

This package lives in the `clients/` npm workspace and depends on `@beacon/client`, which must
be built first (it resolves through its `dist/` output):

```bash
# from clients/
npm install                       # installs the whole JS workspace (beacon-ts + beacon-web)
npm run build -w @beacon/client   # build the SDK so beacon-web can import it
```

You also need a running Beacon server to point at. With defaults:

```bash
# from the repo root
BEACON_ADMIN_USERNAME=beacon-admin BEACON_ADMIN_PASSWORD=beacon-password \
  cargo run -p beacon-api          # serves http://localhost:5001
```

Beacon's default CORS policy (`*`, with the `Authorization` header allowed) permits the dev
server to call it directly.

## Develop

```bash
# from clients/beacon-web
npm run dev        # Vite dev server on http://localhost:5173
npm run typecheck  # tsc --noEmit
npm run build      # production build to dist/
npm run preview    # preview the production build
```

Open the dev URL, sign in with the server URL (default `http://localhost:5001`) and your admin
credentials, and you're in.

## Bundled with the Beacon server (Docker)

The production build is shipped inside the Beacon Docker image and served by `beacon-api`
itself, so no separate web host is needed. The Dockerfile builds the SDK and this app in a
Node stage and copies `dist/` into `/beacon/web`; `beacon-api` mounts it at **`/admin`** and
redirects `/` there. Just run the image and open `http://localhost:5001/admin`:

```bash
docker run -p 5001:5001 beacon
```

For local development there are `make` shortcuts at the repo root:

```bash
make run      # build the SPA, then serve API + UI on http://localhost:5001/admin
make dev-api  # API only (terminal 1)  ┐ UI with hot-reload
make dev-ui   # Vite dev server (terminal 2)  ┘ at http://localhost:5173
```

When served by the Beacon server the UI talks to its own origin automatically — the login screen only asks
for admin username/password (no server URL). The directory is configurable via
`BEACON_WEB_UI_DIR` (default `web`, relative to the working directory); if it is absent — as in a
bare `cargo run` — the `/admin` route is simply not mounted and `/` keeps redirecting to the API
docs.
