# Beacon developer convenience targets.
#
# The Rust server (beacon-datalake) and the admin web UI (beacon-datalake-clients/beacon-web) build
# independently. These targets wire them together so you can serve both from a
# single process, or run the UI with hot-reload while iterating.
#
# Quick start:
#   make run     # build the SPA, then serve API + UI on http://localhost:5001/admin
#   make dev-api # run just the API (terminal 1)   } UI dev with hot-reload
#   make dev-ui  # run the Vite dev server (terminal 2)

# Admin credentials the server starts with (also what you log in with).
BEACON_ADMIN_USERNAME ?= beacon-admin
BEACON_ADMIN_PASSWORD ?= beacon-password
# Where the built SPA lives; beacon-datalake serves it at /admin when present.
WEB_DIR ?= beacon-datalake-clients/beacon-web/dist

export BEACON_ADMIN_USERNAME
export BEACON_ADMIN_PASSWORD

.PHONY: help ui-deps ui run serve dev-api dev-ui clean-ui wheel-check wheel

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

ui-deps: ## Install JS workspace dependencies
	cd beacon-datalake-clients && npm install

# Depends on ui-deps because the workspace symlinks in node_modules encode the
# absolute path of each workspace. Moving or renaming a client directory leaves
# them dangling, and the SPA then fails to resolve `@beacon/client` — which
# surfaces as a wall of unrelated-looking implicit-any errors from tsc. `npm
# install` is idempotent and relinks them.
ui: ui-deps ## Build the admin web UI (SDK first, then the SPA) into $(WEB_DIR)
	cd beacon-datalake-clients && npm run build --workspace beacon-ts
	cd beacon-datalake-clients && npm run build --workspace beacon-web

run: ui ## Build the SPA, then serve API + UI on http://localhost:5001/admin
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run -p beacon-datalake

run-release: ui ## Build the SPA, then serve API + UI on http://localhost:5001/admin
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run --release -p beacon-datalake

serve: ## Serve API + UI without rebuilding the SPA (expects $(WEB_DIR) to exist)
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run -p beacon-datalake

dev-api: ## Run only the API (no bundled UI); pair with `make dev-ui`
	cargo run -p beacon-datalake

dev-ui: ## Run the Vite dev server with hot-reload on http://localhost:5173
	cd beacon-datalake-clients/beacon-web && npm run dev

clean-ui: ## Remove the built SPA
	rm -rf $(WEB_DIR) beacon-datalake-clients/beacon-ts/dist

# The beacondb wheel is built in a manylinux container whose toolchain is nothing like a dev
# machine's, and CI only builds it on a release tag. These run that same build locally, so
# container breakage is found before a tag is cut. musllinux is checked too: it is not in the
# release matrix today, but publish-beacondb.yml keeps its apk branch so the entries can be
# restored, and this is what keeps that branch honest.
wheel-check: ## Verify the beacondb wheel build toolchain in the CI containers (fast)
	./scripts/build-wheel-docker.sh --deps-only
	./scripts/build-wheel-docker.sh --libc musllinux --deps-only

wheel: ## Build the beacondb wheel in the CI manylinux container (slow; wheel in target/docker-wheels)
	./scripts/build-wheel-docker.sh
