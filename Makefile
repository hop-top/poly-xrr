# xrr — top-level Makefile
#
# Per-language: test-{go,py,ts,rs,php}, e2e-{go,py,ts,rs,php},
# lint-{go,py,ts,rs,php}. Aggregated: test-all, e2e-all, lint-all,
# check-all. Bare `make test` / `make e2e` / `make lint` stay Go-only
# (canonical implementation).
#
# Hop worktrees split the git dir from the working tree; suppress VCS
# stamp errors (same pattern as the other poly- repos).
export GOFLAGS := -buildvcs=false

.PHONY: test e2e lint check \
        test-go test-py test-ts test-rs test-php test-all \
        e2e-go e2e-py e2e-ts e2e-rs e2e-php e2e-all \
        lint-go lint-py lint-ts lint-rs lint-php lint-all \
        install-ts install-php check-all dist \
        dev-up dev-down dev-exec dev-rebuild dev-status dev-logs

# --- Go (canonical) ---

test test-go:
	cd go && go test ./...

e2e e2e-go:
	cd go && go test -v -run TestE2E .

lint lint-go:
	cd go && go vet ./...
	@if command -v golangci-lint >/dev/null 2>&1; then \
		cd go && golangci-lint run; \
	else \
		echo "lint-go: golangci-lint not installed; 'mise install' to enable"; \
	fi

# --- Python (uv + pytest + ruff) ---

test-py:
	cd py && uv run pytest

e2e-py:
	cd py && uv run pytest tests/test_e2e.py -v

lint-py:
	cd py && uv run ruff check .

# --- TypeScript (pnpm + vitest + eslint) ---

install-ts:
	@[ -d ts/node_modules ] || ( cd ts && pnpm install --frozen-lockfile )

test-ts: install-ts
	cd ts && pnpm test

e2e-ts: install-ts
	cd ts && pnpm test tests/e2e.test.ts

lint-ts: install-ts
	cd ts && pnpm lint

# --- Rust (cargo) ---

test-rs:
	cd rs && cargo test

e2e-rs:
	cd rs && cargo test --test e2e

lint-rs:
	cd rs && cargo clippy -- -D warnings

# --- PHP (phpunit + phpstan) ---

install-php:
	cd php && composer install --no-interaction --no-progress

test-php:
	@[ -d php/vendor ] || $(MAKE) install-php
	cd php && vendor/bin/phpunit

e2e-php:
	@[ -d php/vendor ] || $(MAKE) install-php
	cd php && vendor/bin/phpunit tests/E2eTest.php --testdox

lint-php:
	@[ -d php/vendor ] || $(MAKE) install-php
	cd php && vendor/bin/phpstan analyse src --level 9 --no-progress

# --- Release packaging ---

# Copy root legal/community files into each lang subdir for release
# packaging. py/ts/rs reference ../ directly in their manifests
# (pyproject.toml / package.json / Cargo.toml).
dist:
	cp LICENSE CODE_OF_CONDUCT.md CONTRIBUTING.md SECURITY.md README.md go/
	cp LICENSE CODE_OF_CONDUCT.md CONTRIBUTING.md SECURITY.md README.md php/

# --- Aggregates ---

test-all: test-go test-py test-ts test-rs test-php

e2e-all: e2e-go e2e-py e2e-ts e2e-rs e2e-php

lint-all: lint-go lint-py lint-ts lint-rs lint-php

# check stays Go-only (cheap pre-commit gate).
# check-all gates on every language: lint + test + e2e (was `task check`).
check: lint test

check-all: lint-all test-all e2e-all

# --- Devcontainer lifecycle ---
#
# Uses @devcontainers/cli (Microsoft's reference implementation).
# Auto-installed on first use via npx; no global install required.
# Requires Docker.

DEVCONTAINER := npx -y @devcontainers/cli@latest
DEVCONTAINER_CONFIG := .devcontainer/devcontainer.json

# dev-up builds (if needed) and starts the container. Idempotent —
# subsequent invocations re-use the running container.
dev-up:
	@command -v docker >/dev/null 2>&1 || { echo "dev-up: docker is required"; exit 1; }
	$(DEVCONTAINER) up --workspace-folder . --config $(DEVCONTAINER_CONFIG)

# dev-exec opens an interactive shell inside the running container.
# Use `make dev-exec CMD="make test-all"` to run a specific command.
dev-exec:
	@command -v docker >/dev/null 2>&1 || { echo "dev-exec: docker is required"; exit 1; }
	$(DEVCONTAINER) exec --workspace-folder . --config $(DEVCONTAINER_CONFIG) $(if $(CMD),$(CMD),bash)

# dev-down stops + removes the container. Image cache is preserved;
# next dev-up reuses it for fast restart.
dev-down:
	@command -v docker >/dev/null 2>&1 || { echo "dev-down: docker is required"; exit 1; }
	@cid=$$(docker ps -aq --filter "label=devcontainer.local_folder=$$PWD"); \
	if [ -n "$$cid" ]; then docker rm -f $$cid; else echo "dev-down: no container for $$PWD"; fi

# dev-rebuild forces a fresh container build. Use after editing
# .devcontainer/devcontainer.json (features, versions).
dev-rebuild:
	@command -v docker >/dev/null 2>&1 || { echo "dev-rebuild: docker is required"; exit 1; }
	$(MAKE) dev-down
	$(DEVCONTAINER) up --workspace-folder . --config $(DEVCONTAINER_CONFIG) --remove-existing-container

dev-status:
	@docker ps -a --filter "label=devcontainer.local_folder=$$PWD" \
		--format "table {{.Names}}\t{{.Status}}\t{{.Image}}" \
		2>/dev/null || echo "dev-status: docker not available"

dev-logs:
	@cid=$$(docker ps -aq --filter "label=devcontainer.local_folder=$$PWD"); \
	if [ -n "$$cid" ]; then docker logs --tail 100 $$cid; else echo "dev-logs: no container for $$PWD"; fi
