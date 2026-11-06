# Beacon and Beacon Studio

This example runs a Beacon server and [Beacon Studio](https://github.com/maris-development/beacon-studio)
with Docker Compose. Compose builds Studio from its Git repository.

```bash
docker compose up -d --build
```

1. Copy data files into `./datasets`.
2. Open Studio at <http://localhost:8080>.
3. Go to **Beacon Nodes** and add the node `http://localhost:5001`.

Change the admin credentials in `docker-compose.yml` before you expose Beacon.

The Studio source is on GitHub:
<https://github.com/maris-development/beacon-studio>. To build or change Studio
yourself, clone it:

```bash
git clone https://github.com/maris-development/beacon-studio.git
```

For the settings, production use and troubleshooting, see the
[Beacon Studio chapter](https://maris-development.github.io/beacon/docs/2.1.0-rc2/beacon-studio)
of the documentation.
