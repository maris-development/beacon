---
description: Deploy Beacon Studio next to a Beacon server with Docker Compose. Studio is a web app to explore Beacon data on maps, in tables and in charts.
---

# Beacon Studio

[Beacon Studio](https://github.com/maris-development/beacon-studio) is a web app for the data of
Beacon nodes. Build a query without SQL. Show the results on a map, in a table or in a chart. Studio
also connects to more than one node, for example your own node and the public nodes.

Studio is a separate project. It has its own repository and the Apache 2.0 license. It is a static
web app. Each browser calls the Beacon API directly over HTTP.

This chapter deploys Beacon and Studio together with Docker Compose.

## Get the Studio source

The source of Studio is on GitHub:
[github.com/maris-development/beacon-studio](https://github.com/maris-development/beacon-studio).
The Compose example below clones it for you. To build or change Studio yourself, clone it:

```bash
git clone https://github.com/maris-development/beacon-studio.git
```

The [Studio README](https://github.com/maris-development/beacon-studio#readme) gives the
development and build steps.

## Studio and the admin UI

Beacon includes the [admin web UI](/docs/2.1.0-rc2/connect/web-admin-ui) at `/admin`. Studio does
not replace it. Use the table to select a tool:

| | Admin web UI | Beacon Studio |
| - | - | - |
| **Users** | Operators of one node | Operators and data users |
| **Deploy** | Included in Beacon | Separate container or desktop app |
| **Nodes** | The node that serves it | Each node that you add |
| **Queries** | SQL editor | Query builder and SQL editor |
| **Results** | Results grid, download | Map, table, chart, download |
| **Login** | Admin credentials only | Anonymous, bearer token or admin |

## Deploy Beacon and Studio together

The Beacon repository holds a ready example in
[`examples/beacon-studio`](https://github.com/maris-development/beacon/tree/main/examples/beacon-studio).
It runs two services:

- **`beacon`**: the Beacon server, on port `5001`.
- **`studio`**: Studio, on port `8080`. Studio has no published image. Compose builds it from the
  Studio Git repository and serves it with nginx.

### 1. Get the files

Clone the Beacon repository and open the example folder:

```bash
git clone https://github.com/maris-development/beacon.git
cd beacon/examples/beacon-studio
```

Or copy the three files below into one folder:

```
beacon-studio/
├── docker-compose.yml
└── studio/
    ├── Dockerfile
    └── nginx.conf
```

::: code-group

<<< ../../../examples/beacon-studio/docker-compose.yml [docker-compose.yml]

<<< ../../../examples/beacon-studio/studio/Dockerfile{dockerfile} [studio/Dockerfile]

<<< ../../../examples/beacon-studio/studio/nginx.conf{nginx} [studio/nginx.conf]

:::

### 2. Start the services

Open the folder. Then run:

```bash
docker compose up -d --build
```

The first build of Studio takes some minutes. Docker then keeps the image. A second start is fast.

Copy supported files into `./datasets`, as in [Getting Started](/docs/2.1.0-rc2/getting-started#local).

### 3. Add your node in Studio

1. Open Studio at <http://localhost:8080>.
2. Go to **Beacon Nodes**. Click **Add node**.
3. Type a name, for example `Local Beacon`.
4. Type the URL `http://localhost:5001`.
5. Optional: type the admin user name and password from `docker-compose.yml`. Studio then shows the
   admin pages, for example **Crawlers** and **Users & Roles**.
6. Click **Test connection**. Then click **Add node**.
7. Select the node in the node picker at the top of a page.

::: warning Give the URL that the browser can reach
Studio runs in the browser, not in the `studio` container. Thus the browser calls Beacon. Give the
URL of Beacon that the browser can reach, for example `http://localhost:5001`. The Docker service
name `http://beacon:5001` does not work.
:::

## Settings

Set these variables in the shell or in a `.env` file next to `docker-compose.yml`:

| Variable | Default | Description |
| - | - | - |
| `STUDIO_REF` | `main` | The Git branch, tag or commit of Studio to build. |
| `STUDIO_BRAND` | `default` | The [brand](https://github.com/maris-development/beacon-studio/blob/main/src/brands/default/README.md) of Studio: fonts, colors, logo, text and links. |

Run `docker compose up -d --build` again after a change to `STUDIO_REF` or `STUDIO_BRAND`. These
values go into the Studio build.

The browser calls Beacon from the Studio origin. Beacon allows calls from each origin by default, so
the example needs no CORS setting.

## Production

The example serves plain HTTP on `localhost`. For a public deployment:

- **Use HTTPS.** Put a reverse proxy, for example nginx or Traefik, in front of both services. Give
  each service its own host name, for example `beacon.example.org` and `studio.example.org`.
- **Limit the CORS origins.** Set
  [`BEACON_CORS_ALLOWED_ORIGINS`](/docs/2.1.0-rc2/server/configuration#cors) to the public origin of
  Studio, for example `https://studio.example.org`. Then other web sites cannot call Beacon from
  the browser of a user. Separate two origins with a comma.
- **Add the public URL** of Beacon as the node in Studio, for example `https://beacon.example.org`.
- **Change the admin credentials** in `docker-compose.yml`. See
  [Secure your instance](/docs/2.1.0-rc2/getting-started#local).
- **Pin `STUDIO_REF`** to a commit, so that a new build gives the same Studio.

### Beacon below a path

Beacon can run below a path, for example `https://example.org/beacon-api`. Studio supports this.
Add the full URL with the path as the node. Studio adds the path to each request.

Use one of these two setups:

- **Beacon serves the path.** Set
  [`BEACON_BASE_PATH`](/docs/2.1.0-rc2/server/configuration#server) to the path, for example
  `BEACON_BASE_PATH=/beacon-api`. The proxy sends the path to Beacon without a change.
- **The proxy removes the path.** Keep `BEACON_BASE_PATH` empty. The proxy removes `/beacon-api`
  before it sends the request to Beacon.

For the first setup, add this line to the `environment` list of the `beacon` service:

```yaml
- BEACON_BASE_PATH=/beacon-api
```

The node URL in Studio is then `http://localhost:5001/beacon-api`.

### Studio below a path

To serve Studio below a path, for example `/studio`, build it with `BASE_PATH`. See the
[Studio README](https://github.com/maris-development/beacon-studio#building). The nginx configuration
of this example serves Studio at the root path only.

## Data that Studio sends

- **Public nodes**: a new user sees a list of public Beacon nodes. Studio gets the list from
  `beacon-datalake.org`. Studio also gets the examples of the home page from that site.
- **Telemetry**: off. The example build does not set `STUDIO_TELEMETRY=on`, so Studio sends no usage
  events.
- **Credentials**: Studio keeps the node list in the browser. It keeps the admin password only when
  you select **Remember on this device**.

## Desktop app

Studio is also a desktop app, built with Tauri. The desktop app needs no web server. Build it from
the [Studio repository](https://github.com/maris-development/beacon-studio#building) with
`npm run tauri:build`.

## Troubleshooting

**Studio shows the node as offline.** Open `http://localhost:5001/api/health` in the browser. If the
page does not open, Beacon does not run, or the port is wrong. Run `docker compose logs beacon`.

**The browser console shows a CORS error.** The origin of Studio is not in
`BEACON_CORS_ALLOWED_ORIGINS`. This occurs only when you set that variable. Add the origin that
opens Studio, for example `http://127.0.0.1:8080`. Then run `docker compose up -d`.

**`npm ci` fails in the Studio build.** The Studio lock file needs npm 11. Keep the `node:24-alpine`
base image in `studio/Dockerfile`.

**Port `5001` or `8080` is in use.** Change the left port of the mapping, for example
`"9080:80"`. For a new Beacon port, also change the node URL in Studio.
