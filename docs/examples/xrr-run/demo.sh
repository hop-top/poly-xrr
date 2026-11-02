#!/usr/bin/env bash
# Record then replay one example suite.
#   demo.sh bats|pytest|vitest
# Needs `xrr` on PATH (cd go && go build -o /usr/local/bin/xrr ./cmd/xrr).
# For vitest, run from a checkout where `npx vitest` works (ts/ has it).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
case "${1:?usage: demo.sh bats|pytest|vitest}" in
  bats)   suite=(bats "$here/bats") ;;
  pytest) suite=(pytest -q "$here/pytest") ;;
  vitest) suite=(npx vitest run --root "$here/vitest") ;;
  *) echo "unknown suite $1" >&2; exit 2 ;;
esac
cass=$(mktemp -d)/cassettes
trap 'rm -rf "$(dirname "$cass")"' EXIT

echo "== record (real az on PATH) =="
PATH="$here/fake-bin:$PATH" xrr run --mode record --cassettes "$cass" --shim az -- "${suite[@]}"

echo "== replay (az not on PATH) =="
xrr run --mode replay --cassettes "$cass" --shim az -- "${suite[@]}"
