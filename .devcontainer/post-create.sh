#!/usr/bin/env bash
# post-create.sh — runs after devcontainer create. Mirrors the Makefile's
# per-language install steps so `make check-all` works out of the box
# inside the container.
set -euo pipefail

echo "==> Enabling corepack (pnpm)"
corepack enable

echo "==> Installing uv (Python runner used by make test-py)"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="$HOME/.local/bin:$PATH"

echo "==> Warming Go module cache"
(cd go && go mod download)

echo "==> Installing Python dependencies"
(cd py && uv sync)

echo "==> Installing TypeScript dependencies"
# CI=true: pnpm refuses to purge a pre-existing node_modules without a
# TTY, and postCreateCommand has none. Scoped to this call so
# interactive shells in the container are unaffected.
(cd ts && CI=true pnpm install --frozen-lockfile --ignore-scripts)

echo "==> Installing PHP dependencies"
(cd php && composer install --no-interaction --no-progress)

echo "==> Warming Rust toolchain"
(cd rs && cargo fetch)

echo "Dev environment ready. Run: make check-all"
