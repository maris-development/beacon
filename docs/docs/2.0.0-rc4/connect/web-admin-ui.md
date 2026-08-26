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
[`@beacon/client`](/docs/2.0.0-rc4/connect/typescript) TypeScript SDK
only.

## Log in

The UI is **for an admin only**. A login screen protects the whole application.
Sign in with the URL of the Beacon server. Also give the admin Basic auth
credentials of that server:

```bash
BEACON_ADMIN_USERNAME=beacon-admin
BEACON_ADMIN_PASSWORD=beacon-password
```

The UI checks the credentials with `GET /admin/api/admin/check`. It stores them in
the `localStorage` of the browser. It then sends them on every request.

## A gateway can sign you in

A gateway in front of Beacon can supply the credentials. nginx and oauth2-proxy
do this with an injected `Authorization` header. The UI then shows no login
screen.

On the first load the UI calls `GET /admin/api/admin/check` with no credentials.
Beacon answers `401` to a request without super-user credentials. A `200`
therefore proves that a gateway adds them. The UI starts a **proxy session**:

- It stores no credentials in the browser.
- It sends no `Authorization` header. The header of the gateway stays intact.
- The user menu shows `Proxy session`.

The check goes to the origin that serves the UI. It fails safe: `401`, `403` or a
network error gives the login screen.

**Sign out** stops the detection for that browser tab. A new tab starts a proxy
session again.

An example for nginx:

```nginx
location /admin/ {
    proxy_set_header Authorization "Basic YmVhY29uLWFkbWluOmJlYWNvbi1wYXNzd29yZA==";
    proxy_pass http://beacon:5001;
}
```

:::warning
That header makes every caller behind the gateway a super-user. It also replaces
the header that a browser sends, so the login screen accepts each password. Put
your own authentication in front of it.
:::

## Every call sits below `/admin`

The UI calls the [admin path alias](/docs/2.0.0-rc4/api/#admin-path-alias) of the
API. `/admin/api/query` runs the same handler as `/api/query`. Each request of the
UI therefore uses the same prefix that serves the UI.

Put your own security in front of `/api/*` with this. The UI keeps its function,
because it sends no request to `/api/*`. Give your proxy these rules:

- Keep your own rules on `/api/*`.
- Send `/admin` and each path below it to Beacon.

The alias needs the admin credentials, which the UI holds after the login.

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
  [external table](/docs/2.0.0-rc4/data-sources/external-tables) over the
  files in the datasets store. Drop a table with `DROP TABLE`. Beacon keeps the
  files.
- **Datasets**: explore the dataset files that Beacon finds. Inspect the schema of
  each file.
- **Crawlers**: list, [create, run and delete crawlers](/docs/2.0.0-rc4/server/crawlers).
- **Users and roles**: manage the
  [role-based access control](/docs/2.0.0-rc4/security/access-control). This covers
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
