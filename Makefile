# Beacon developer convenience targets.
#
# The Rust server (beacon-server) and the admin web UI (beacon-clients/beacon-web) build
# independently. These targets wire them together so you can serve both from a
# single process, or run the UI with hot-reload while iterating.
#
# Quick start:
#   make run     # build the SPA, then serve API + UI on http://localhost:5001/admin
#   make dev-api # run just the API (terminal 1)   } UI dev with hot-reload
#   make dev-ui  # run the Vite dev server (terminal 2)
#
# A standard build links PROJ, a native C++ library. Use the *-no-proj targets on a
# machine without it. See the PROJ note above `run-no-proj`.

# Admin credentials the server starts with (also what you log in with).
BEACON_ADMIN_USERNAME ?= beacon-admin
BEACON_ADMIN_PASSWORD ?= beacon-password
# Where the built SPA lives; beacon-server serves it at /admin when present.
WEB_DIR ?= beacon-clients/beacon-web/dist

export BEACON_ADMIN_USERNAME
export BEACON_ADMIN_PASSWORD

.PHONY: help ui-deps ui run run-release run-no-proj run-release-no-proj serve dev-api dev-api-no-proj dev-ui clean-ui wheel-check wheel

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

ui-deps: ## Install JS workspace dependencies
	cd beacon-clients && npm install

# Depends on ui-deps because the workspace symlinks in node_modules encode the
# absolute path of each workspace. Moving or renaming a client directory leaves
# them dangling, and the SPA then fails to resolve `@beacon/client` — which
# surfaces as a wall of unrelated-looking implicit-any errors from tsc. `npm
# install` is idempotent and relinks them.
ui: ui-deps ## Build the admin web UI (SDK first, then the SPA) into $(WEB_DIR)
	cd beacon-clients && npm run build --workspace beacon-ts
	cd beacon-clients && npm run build --workspace beacon-web

run: ui ## Build the SPA, then serve API + UI on http://localhost:5001/admin
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run -p beacon-server

run-release: ui ## Build the SPA, then serve API + UI on http://localhost:5001/admin
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run --release -p beacon-server

# `spatial-proj` is the only default feature of beacon-server, so `--no-default-features`
# drops PROJ and nothing else. The server then loses `ST_Transform`, the one function that
# reprojects coordinates. Every other spatial function stays. Use this where the machine
# carries no PROJ 9.6.2 or later, or no pkg-config to find it. The alternative is
# `--features spatial-proj-bundled`, which builds PROJ from source and needs a C++
# toolchain, CMake, sqlite3 and libtiff.
run-no-proj: ui ## Same as `run`, but without PROJ (drops ST_Transform)
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run -p beacon-server --no-default-features

run-release-no-proj: ui ## Same as `run-release`, but without PROJ (drops ST_Transform)
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run --release -p beacon-server --no-default-features

serve: ## Serve API + UI without rebuilding the SPA (expects $(WEB_DIR) to exist)
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run -p beacon-server

dev-api: ## Run only the API (no bundled UI); pair with `make dev-ui`
	cargo run -p beacon-server

# See the PROJ note above `run-no-proj`.
dev-api-no-proj: ## Same as `dev-api`, but without PROJ (drops ST_Transform)
	cargo run -p beacon-server --no-default-features

dev-ui: ## Run the Vite dev server with hot-reload on http://localhost:5173
	cd beacon-clients/beacon-web && npm run dev

clean-ui: ## Remove the built SPA
	rm -rf $(WEB_DIR) beacon-clients/beacon-ts/dist

# The beacondb wheel is built in the same container the release workflow uses, so a toolchain
# break shows up here rather than on a tag. `wheel-check` stops after the toolchain, which is
# the cheap part; `wheel` compiles the engine, netCDF, HDF5, OpenSSL and PROJ from source.
wheel-check: ## Verify the beacondb wheel build toolchain in the CI containers (fast)
	./scripts/build-wheel-docker.sh --deps-only
	./scripts/build-wheel-docker.sh --libc musllinux --deps-only

wheel: ## Build the beacondb wheel in the CI manylinux container (slow; wheel in target/docker-wheels)
	./scripts/build-wheel-docker.sh
