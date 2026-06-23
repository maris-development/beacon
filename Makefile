# Beacon developer convenience targets.
#
# The Rust server (beacon-api) and the admin web UI (clients/beacon-web) build
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
# Where the built SPA lives; beacon-api serves it at /admin when present.
WEB_DIR ?= clients/beacon-web/dist

export BEACON_ADMIN_USERNAME
export BEACON_ADMIN_PASSWORD

.PHONY: help ui-deps ui run serve dev-api dev-ui clean-ui

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

ui-deps: ## Install JS workspace dependencies
	cd clients && npm install

ui: ## Build the admin web UI (SDK first, then the SPA) into $(WEB_DIR)
	cd clients && npm run build --workspace beacon-ts
	cd clients && npm run build --workspace beacon-web

run: ui ## Build the SPA, then serve API + UI on http://localhost:5001/admin
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run -p beacon-api

serve: ## Serve API + UI without rebuilding the SPA (expects $(WEB_DIR) to exist)
	BEACON_WEB_UI_DIR=$(WEB_DIR) cargo run -p beacon-api

dev-api: ## Run only the API (no bundled UI); pair with `make dev-ui`
	cargo run -p beacon-api

dev-ui: ## Run the Vite dev server with hot-reload on http://localhost:5173
	cd clients/beacon-web && npm run dev

clean-ui: ## Remove the built SPA
	rm -rf $(WEB_DIR) clients/beacon-ts/dist
