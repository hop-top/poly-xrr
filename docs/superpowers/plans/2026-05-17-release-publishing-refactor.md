# Release Publishing Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `hop-top/xrr-poly` onto the hop-top release/publish standard (release-please + `publish-on-tag.yml@v0` + tag-driven mirror), ship `0.1.0-alpha.4` across 5 components, and rename the repo to `hop-top/poly-xrr`.

**Architecture:** Mirror `hop-top/poly-uri`'s shape exactly: per-component release-please config + manifest, two thin workflows (`release-please.yml`, `publish.yml`) that delegate to `hop-top/.github`'s reusable workflows, replacing the existing `split-publish.yml` + `nightly-publish.yml` pair. Pre-seed manifests at `0.1.0-alpha.3` so the next conventional commit advances each component to `0.1.0-alpha.4`. Suppress the `xrr-poly` umbrella to avoid umbrella-tag parsing failures.

**Tech Stack:** GitHub Actions (workflows), `googleapis/release-please-action@v4`, `hop-top/.github/.github/workflows/publish-on-tag.yml@v0` (reusable), `gh` CLI, `npx release-please` (for dry-run validation).

**Spec:** `docs/superpowers/specs/2026-05-17-release-publishing-refactor-design.md`

**Track:** `release-conv` (tlc)

---

## File Structure

Files this plan creates, modifies, or deletes:

| File | Action | Responsibility |
|---|---|---|
| `.github/release-please-config.json` | Create | Per-component release-please config (5 components, no umbrella) |
| `.github/.release-please-manifest.json` | Create | Current version of each component (seeded at `0.1.0-alpha.3`) |
| `.github/workflows/release-please.yml` | Create | Opens standing release PRs on push to `main` |
| `.github/workflows/publish.yml` | Create | Tag-triggered publish + mirror via reusable workflow |
| `.github/workflows/split-publish.yml` | Delete | Superseded by `publish.yml` |
| `.github/workflows/nightly-publish.yml` | Delete | Superseded by tag-driven mirror |
| `ts/package.json` | Modify | Bump `version` `0.1.0 → 0.1.0-alpha.3`; add `ci:build` script |
| `py/pyproject.toml` | Modify | Bump `[project].version` `0.1.0 → 0.1.0-alpha.3` |
| `rs/Cargo.toml` | Modify | Bump `[package].version` `0.1.0 → 0.1.0-alpha.3` |

PHP (`php/composer.json`) and Go (`go/go.mod`) carry no embedded version field — versions are tag-driven on Packagist and proxy.golang.org. Not touched.

---

## Phase 1 — Pre-flight Verification

### Task 1: Verify org/repo secrets exist

**Files:** none (read-only checks)

- [ ] **Step 1: List repo secrets**

```sh
gh secret list --repo hop-top/xrr-poly
```

Expected: at minimum `SPLIT_TOKEN` (the old token). Note presence/absence of:
- `GH_RELEASE_PLEASE_PAT`
- `GH_MIRROR_PAT`
- `NPM_REGISTRY_TOKEN`
- `CARGO_REGISTRY_TOKEN`
- `PYPI_REGISTRY_TOKEN`

- [ ] **Step 2: List org secrets** (only if access permits)

```sh
gh secret list --org hop-top 2>&1 || echo "no org-secret access from this token; rely on repo-level"
```

Expected: the 5 secrets above likely live at the org level (used by `poly-uri`/`poly-kit`). Document which are present.

- [ ] **Step 3: Record findings**

Write a short note in the PR draft (kept locally for now) listing each secret + scope where found, or "MISSING — must provision before tag push." If any of `GH_RELEASE_PLEASE_PAT`, `GH_MIRROR_PAT`, `NPM_REGISTRY_TOKEN`, `CARGO_REGISTRY_TOKEN`, `PYPI_REGISTRY_TOKEN` are missing, **STOP** and surface to the user before continuing — they must create the secret before Phase 5 dry-run will succeed.

- [ ] **Step 4: No commit**

This step produces information only.

---

### Task 2: Verify registry name availability + mirror repos

**Files:** none (read-only checks)

- [ ] **Step 1: Re-confirm registry names are still free**

```sh
for url in \
  "https://registry.npmjs.org/@hop-top/xrr" \
  "https://pypi.org/pypi/hop-top-xrr/json" \
  "https://crates.io/api/v1/crates/hop-top-xrr" \
  "https://repo.packagist.org/p2/hop-top/xrr.json"; do
  printf '%-60s %s\n' "$url" "$(curl -sf -o /dev/null -w '%{http_code}' "$url" || echo 404)"
done
```

Expected: all four print `404` (available). If any returns `200`: **STOP** — someone claimed the name; surface to user before continuing.

- [ ] **Step 2: List existing xrr-* mirror repos**

```sh
for r in xrr xrr-ts xrr-py xrr-rs xrr-php; do
  printf '%-15s ' "hop-top/$r"
  gh repo view "hop-top/$r" --json name 2>&1 | grep -q '"name"' && echo "EXISTS" || echo "MISSING (auto-created on first publish)"
done
```

Expected (from earlier brainstorm): `xrr` and `xrr-ts` exist; `xrr-py`, `xrr-rs`, `xrr-php` missing.

- [ ] **Step 3: Verify rename target is free**

```sh
gh repo view hop-top/poly-xrr --json name 2>&1
```

Expected: `Could not resolve to a Repository` (free).

- [ ] **Step 4: No commit**

Information only.

---

### Task 3: Probe poly-uri's umbrella-tag handling

**Files:** none (read-only checks)

- [ ] **Step 1: Check if poly-uri has ever tagged its umbrella**

```sh
gh api repos/hop-top/poly-uri/git/refs/tags --jq '.[].ref' 2>&1 | grep '^refs/tags/uri-poly/' || echo "no uri-poly tags — umbrella never published"
```

Expected: no `uri-poly/*` tags. Confirms umbrella suppression (decision Y) matches poly-uri's actual behavior.

- [ ] **Step 2: If umbrella tags DO exist** (unexpected):

Look at the workflow run for the most recent umbrella tag:

```sh
gh run list --repo hop-top/poly-uri --workflow publish.yml --limit 5
```

If `publish.yml` succeeded on an umbrella tag, our **Y** decision needs revisiting. Surface to user. Otherwise proceed.

- [ ] **Step 3: No commit**

Information only.

---

## Phase 2 — Land Conventions on `main`

### Task 4: Pre-seed source manifests to `0.1.0-alpha.3`

**Files:**
- Modify: `ts/package.json` (line with `"version": "0.1.0"`)
- Modify: `py/pyproject.toml` (line with `version = "0.1.0"`)
- Modify: `rs/Cargo.toml` (line with `version = "0.1.0"`)

- [ ] **Step 1: Bump ts/package.json**

Edit `ts/package.json`, change:

```json
  "version": "0.1.0",
```

to:

```json
  "version": "0.1.0-alpha.3",
```

- [ ] **Step 2: Bump py/pyproject.toml**

Edit `py/pyproject.toml`, change:

```toml
version = "0.1.0"
```

to:

```toml
version = "0.1.0-alpha.3"
```

- [ ] **Step 3: Bump rs/Cargo.toml**

Edit `rs/Cargo.toml`, change:

```toml
version = "0.1.0"
```

to:

```toml
version = "0.1.0-alpha.3"
```

- [ ] **Step 4: Verify all three bumps**

```sh
grep -H 'version.*0.1.0' ts/package.json py/pyproject.toml rs/Cargo.toml | grep alpha.3
```

Expected: 3 lines, each showing `0.1.0-alpha.3`.

- [ ] **Step 5: Run a sanity test in each ecosystem to confirm no breakage**

```sh
(cd ts && pnpm install --ignore-scripts >/dev/null && pnpm vitest run) && \
(cd py && uv run pytest -q) && \
(cd rs && cargo test --quiet)
```

Expected: all three pass. Version string changes shouldn't break tests; this confirms.

- [ ] **Step 6: Commit**

```sh
git add ts/package.json py/pyproject.toml rs/Cargo.toml
git commit -m "chore(release): pre-seed manifest versions to 0.1.0-alpha.3"
```

---

### Task 5: Add `ci:build` script to ts/package.json

**Files:**
- Modify: `ts/package.json` (`scripts` block)

- [ ] **Step 1: Inspect current scripts block**

```sh
grep -A 4 '"scripts"' ts/package.json
```

Expected output:

```
  "scripts": {
    "test": "vitest run",
    "lint": "eslint src tests"
  },
```

- [ ] **Step 2: Add `ci:build` script**

Edit `ts/package.json`. Change:

```json
  "scripts": {
    "test": "vitest run",
    "lint": "eslint src tests"
  },
```

to:

```json
  "scripts": {
    "test": "vitest run",
    "lint": "eslint src tests",
    "ci:build": "tsc --build"
  },
```

- [ ] **Step 3: Confirm tsconfig is present**

```sh
ls ts/tsconfig.json
```

Expected: file exists. (Confirmed during brainstorm.)

- [ ] **Step 4: Run the new script to verify it works**

```sh
cd ts && pnpm install --ignore-scripts >/dev/null && pnpm ci:build && cd ..
```

Expected: builds without error. If `tsc --build` fails because of missing `composite: true` or output config in `tsconfig.json`, fall back to `"ci:build": "tsc --noEmit"` (typecheck-only — still useful as a publish gate, no emit needed since `dist/` is gitignored / pre-built by publisher).

- [ ] **Step 5: Commit**

```sh
git add ts/package.json
git commit -m "chore(ts): add ci:build script for publish workflow"
```

---

### Task 6: Create `.github/release-please-config.json`

**Files:**
- Create: `.github/release-please-config.json`

- [ ] **Step 1: Write the config**

Create `.github/release-please-config.json` with this exact content:

```json
{
  "$schema": "https://raw.githubusercontent.com/googleapis/release-please/main/schemas/config.json",
  "separate-pull-requests": true,
  "pull-request-title-pattern": "chore(release):${component} ${version}",
  "include-component-in-tag": true,
  "tag-separator": "/",
  "changelog-sections": [
    {"type": "feat", "section": "Features"},
    {"type": "fix", "section": "Bug Fixes"},
    {"type": "perf", "section": "Performance"},
    {"type": "refactor", "section": "Refactoring"},
    {"type": "chore", "section": "Miscellaneous", "hidden": true},
    {"type": "docs", "section": "Documentation", "hidden": true},
    {"type": "test", "section": "Tests", "hidden": true},
    {"type": "ci", "section": "CI", "hidden": true},
    {"type": "build", "section": "Build", "hidden": true}
  ],
  "packages": {
    "go": {
      "release-type": "go",
      "component": "xrr",
      "changelog-path": "CHANGELOG.md",
      "bump-minor-pre-major": true,
      "prerelease": true,
      "prerelease-type": "alpha.0",
      "versioning": "prerelease"
    },
    "ts": {
      "release-type": "node",
      "component": "xrr-ts",
      "changelog-path": "CHANGELOG.md",
      "bump-minor-pre-major": true,
      "prerelease": true,
      "prerelease-type": "alpha.0",
      "versioning": "prerelease"
    },
    "py": {
      "release-type": "python",
      "component": "xrr-py",
      "package-name": "hop-top-xrr",
      "changelog-path": "CHANGELOG.md",
      "bump-minor-pre-major": true,
      "extra-files": [
        {
          "type": "toml",
          "path": "pyproject.toml",
          "jsonpath": "$.project.version"
        }
      ],
      "prerelease": true,
      "prerelease-type": "alpha.0",
      "versioning": "prerelease"
    },
    "rs": {
      "release-type": "rust",
      "component": "xrr-rs",
      "changelog-path": "CHANGELOG.md",
      "bump-minor-pre-major": true,
      "prerelease": true,
      "prerelease-type": "alpha.0",
      "versioning": "prerelease"
    },
    "php": {
      "release-type": "php",
      "component": "xrr-php",
      "changelog-path": "CHANGELOG.md",
      "bump-minor-pre-major": true,
      "prerelease": true,
      "prerelease-type": "alpha.0",
      "versioning": "prerelease"
    }
  }
}
```

- [ ] **Step 2: Validate JSON syntax**

```sh
python3 -c 'import json; json.load(open(".github/release-please-config.json"))' && echo OK
```

Expected: `OK`.

- [ ] **Step 3: No commit yet**

The config, manifest, and workflows go in one commit (Task 9) so the repo never observes a partial state.

---

### Task 7: Create `.github/.release-please-manifest.json`

**Files:**
- Create: `.github/.release-please-manifest.json`

- [ ] **Step 1: Write the manifest**

Create `.github/.release-please-manifest.json` with this exact content:

```json
{
  "go":  "0.1.0-alpha.3",
  "ts":  "0.1.0-alpha.3",
  "py":  "0.1.0-alpha.3",
  "rs":  "0.1.0-alpha.3",
  "php": "0.1.0-alpha.3"
}
```

- [ ] **Step 2: Validate JSON syntax**

```sh
python3 -c 'import json; json.load(open(".github/.release-please-manifest.json"))' && echo OK
```

Expected: `OK`.

- [ ] **Step 3: No commit yet**

Bundled with Task 9.

---

### Task 8: Create `.github/workflows/release-please.yml` and `publish.yml`

**Files:**
- Create: `.github/workflows/release-please.yml`
- Create: `.github/workflows/publish.yml`

- [ ] **Step 1: Write release-please.yml**

Create `.github/workflows/release-please.yml` with this exact content:

```yaml
name: release-please

on:
  push:
    branches: [main]

permissions:
  contents: write
  pull-requests: write

jobs:
  release-please:
    runs-on: ubuntu-latest
    steps:
      - uses: googleapis/release-please-action@v4
        with:
          config-file: .github/release-please-config.json
          manifest-file: .github/.release-please-manifest.json
          token: ${{ secrets.GH_RELEASE_PLEASE_PAT }}
```

- [ ] **Step 2: Write publish.yml**

Create `.github/workflows/publish.yml` with this exact content:

```yaml
name: publish

on:
  push:
    tags: ['*/v*']

jobs:
  publish:
    permissions:
      contents: read
      id-token: write
    uses: hop-top/.github/.github/workflows/publish-on-tag.yml@v0
    secrets:
      NPM_REGISTRY_TOKEN:   ${{ secrets.NPM_REGISTRY_TOKEN }}
      CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
      PYPI_REGISTRY_TOKEN:  ${{ secrets.PYPI_REGISTRY_TOKEN }}
      GH_MIRROR_PAT:        ${{ secrets.GH_MIRROR_PAT }}
    with:
      homepage: https://hop.top/xrr
      description-prefix: "READ-ONLY MIRROR"
      ecosystems: |
        xrr-ts:
          dir: ts
          ecosystem: ts
          package: "@hop-top/xrr"
          mirror: hop-top/xrr-ts
          test-command: pnpm dlx --config.ignore-scripts=true vitest@3 run
          build-command: pnpm ci:build
        xrr-py:
          dir: py
          ecosystem: py
          package: hop-top-xrr
          mirror: hop-top/xrr-py
          pypi-auth: token
          test-command: pip install -e . && pytest
        xrr-rs:
          dir: rs
          ecosystem: rs
          package: hop-top-xrr
          mirror: hop-top/xrr-rs
        xrr-php:
          dir: php
          ecosystem: php
          package: hop-top/xrr
          mirror: hop-top/xrr-php
        xrr:
          dir: go
          ecosystem: go
          mirror: hop-top/xrr
```

- [ ] **Step 3: Validate YAML syntax**

```sh
python3 -c '
import yaml
for f in [".github/workflows/release-please.yml", ".github/workflows/publish.yml"]:
    yaml.safe_load(open(f))
    print(f, "OK")
'
```

Expected: two `OK` lines.

- [ ] **Step 4: Lint with actionlint (if installed)**

```sh
command -v actionlint && actionlint .github/workflows/release-please.yml .github/workflows/publish.yml || echo "actionlint not installed — skip (CI will catch)"
```

Expected: clean (no output) or skip message. If actionlint reports errors, fix inline; the most common is `${{ inputs.X }}` in `run:` (we have none).

- [ ] **Step 5: No commit yet**

Bundled with Task 9.

---

### Task 9: Delete obsolete workflows + commit Phase 2

**Files:**
- Delete: `.github/workflows/split-publish.yml`
- Delete: `.github/workflows/nightly-publish.yml`

- [ ] **Step 1: Delete the old workflows**

```sh
git rm .github/workflows/split-publish.yml .github/workflows/nightly-publish.yml
```

Expected: two `rm` lines.

- [ ] **Step 2: Stage the new files**

```sh
git add .github/release-please-config.json \
        .github/.release-please-manifest.json \
        .github/workflows/release-please.yml \
        .github/workflows/publish.yml
```

- [ ] **Step 3: Review the staged diff**

```sh
git diff --cached --stat
```

Expected (6 file changes total):
- `+` 4 new files (config, manifest, 2 workflows)
- `-` 2 deleted workflows

- [ ] **Step 4: Commit**

```sh
git commit -m "$(cat <<'EOF'
ci: adopt hop-top release-please + publish-on-tag conventions

Replaces split-publish.yml + nightly-publish.yml (subtree-push with
SPLIT_TOKEN) with the hop-top standard:

- release-please.yml opens standing PRs on every conventional commit
- publish.yml triggers on <component>/v* tags, calls
  hop-top/.github/.github/workflows/publish-on-tag.yml@v0 which
  dispatches per-ecosystem publish + mirror

Five components configured: xrr (go), xrr-ts (npm), xrr-py (PyPI/token),
xrr-rs (crates.io), xrr-php (Packagist). Umbrella suppressed — root
commits do not trigger releases.

Manifests seeded at 0.1.0-alpha.3 so the next conventional commit
proposes 0.1.0-alpha.4 for each component.

Mirrors poly-uri's shape exactly. Source manifests pre-bumped in a
prior commit so release-please drift detection passes.
EOF
)"
```

- [ ] **Step 5: Verify the workflow set**

```sh
ls .github/workflows/
```

Expected:

```
ci.yml
publish.yml
release-please.yml
```

(Three files; no `split-publish.yml` or `nightly-publish.yml`.)

---

## Phase 3 — Dry-Run Gate

### Task 10: Run `release-please release-pr --dry-run` and validate against 5 criteria

**Files:** none

- [ ] **Step 1: Ensure GH_RELEASE_PLEASE_PAT is available locally**

```sh
test -n "${GH_RELEASE_PLEASE_PAT:-}" && echo "PAT set" || echo "PAT MISSING — export it before running"
```

If missing: prompt user to export `GH_RELEASE_PLEASE_PAT=ghp_...` then continue. (Alternative: use `gh auth token` if the gh CLI's token has the required scopes — but a dedicated PAT is recommended to match what the workflow will use.)

- [ ] **Step 2: Run the dry-run**

```sh
npx -y release-please@latest release-pr \
  --token="$GH_RELEASE_PLEASE_PAT" \
  --repo-url=hop-top/xrr-poly \
  --config-file=.github/release-please-config.json \
  --manifest-file=.github/.release-please-manifest.json \
  --dry-run 2>&1 | tee /tmp/release-please-dryrun.log
```

Expected exit code: `0`.

- [ ] **Step 3: Validate against the 5 pass criteria**

```sh
echo "Criterion 1 — exit 0:"; grep -c 'fatal' /tmp/release-please-dryrun.log | xargs -I{} test {} = 0 && echo PASS || echo FAIL

echo "Criterion 2 — 5 components present:"
for c in xrr xrr-ts xrr-py xrr-rs xrr-php; do
  grep -q "$c" /tmp/release-please-dryrun.log && echo "  $c PASS" || echo "  $c FAIL"
done

echo "Criterion 3 — all proposed versions are 0.1.0-alpha.4:"
grep -oE '0\.1\.0-alpha\.[0-9]+' /tmp/release-please-dryrun.log | sort -u

echo "Criterion 4 — tag format <component>/v0.1.0-alpha.4:"
grep -oE '(xrr|xrr-(ts|py|rs|php))/v0\.1\.0-alpha\.4' /tmp/release-please-dryrun.log | sort -u

echo "Criterion 5 — no umbrella xrr-poly PR:"
grep -q 'xrr-poly' /tmp/release-please-dryrun.log && echo FAIL || echo PASS
```

Expected:
- Criterion 1: `PASS`
- Criterion 2: 5 × `PASS`
- Criterion 3: single line `0.1.0-alpha.4`
- Criterion 4: 5 unique tag strings
- Criterion 5: `PASS`

- [ ] **Step 4: If ANY criterion fails — STOP**

Do not proceed to Phase 4. Diagnose by reading `/tmp/release-please-dryrun.log` fully. Common causes:

| Symptom | Likely cause | Fix |
|---|---|---|
| `fatal: no such file` for manifest | Wrong `--manifest-file` path | Re-check `.github/.release-please-manifest.json` exists at that path |
| Versions propose `0.2.0-alpha.0` instead of `0.1.0-alpha.4` | `bump-minor-pre-major` mis-triggering | Verify config; usually means a `feat:` was detected; re-seed manifest if needed |
| Some components missing | Path mismatch between `packages` key and `dir` | Reconcile config with on-disk paths |
| Umbrella `xrr-poly` PR proposed | An umbrella `packages` entry crept in | Remove from config; only 5 keys (`go`, `ts`, `py`, `rs`, `php`) allowed |

Re-run Step 2 after each fix.

- [ ] **Step 5: If all 5 PASS — proceed**

Document the dry-run output in `/tmp/release-please-dryrun.log` for PR body inclusion later. No commit.

---

## Phase 4 — Trigger `0.1.0-alpha.4` Release

### Task 11: Create per-component trigger commits with `Release-As` footers

**Files:** none (empty commits)

> NOTE: This task runs **after** the Phase 2 PR has merged to `origin/main`. The trigger commits also land on `main` directly (small, conventional). If empty commits don't route per-component as expected, fall back to one no-op file per dir (Step 4b).

- [ ] **Step 1: Create 5 empty trigger commits**

```sh
for c in go ts py rs php; do
  comp=$(case $c in
    go) echo xrr ;;
    ts) echo xrr-ts ;;
    py) echo xrr-py ;;
    rs) echo xrr-rs ;;
    php) echo xrr-php ;;
  esac)
  git commit --allow-empty -m "$(cat <<EOF
chore($c): bootstrap 0.1.0-alpha.4 release

First release on the hop-top conventions standard.

Release-As: 0.1.0-alpha.4
EOF
)"
done
```

Expected: 5 commits, each titled `chore(<lang>): bootstrap 0.1.0-alpha.4 release`.

- [ ] **Step 2: Verify the 5 commits**

```sh
git log -5 --format='%h %s'
```

Expected: 5 lines with the bootstrap subjects (one per component, most recent first).

- [ ] **Step 3: Re-run dry-run with these commits in place**

```sh
npx -y release-please@latest release-pr \
  --token="$GH_RELEASE_PLEASE_PAT" \
  --repo-url=hop-top/xrr-poly \
  --config-file=.github/release-please-config.json \
  --manifest-file=.github/.release-please-manifest.json \
  --dry-run 2>&1 | tee /tmp/release-please-dryrun-2.log
```

Expected: 5 PR proposals, one per component, each at version `0.1.0-alpha.4`.

- [ ] **Step 4a: If dry-run shows 5 PRs at `0.1.0-alpha.4` — proceed to Task 12**

- [ ] **Step 4b: If dry-run shows fewer than 5 PRs (empty-commit path attribution failed)**

The empty commits didn't route per-component because release-please attributes by file path. Fall back: undo the 5 empty commits and replace with 5 path-touching commits.

```sh
# Reset the 5 empty commits
git reset --soft HEAD~5
git reset .   # unstage anything (nothing to unstage from empty commits)
git stash drop 2>/dev/null || true

# Create per-component no-op trigger files
for c in go ts py rs php; do
  comp=$(case $c in
    go) echo xrr ;;
    ts) echo xrr-ts ;;
    py) echo xrr-py ;;
    rs) echo xrr-rs ;;
    php) echo xrr-php ;;
  esac)
  echo "trigger: 0.1.0-alpha.4" > "$c/.release-trigger"
  git add "$c/.release-trigger"
  git commit -m "$(cat <<EOF
chore($c): bootstrap 0.1.0-alpha.4 release

Release-As: 0.1.0-alpha.4
EOF
)"
done
```

Then re-run Step 3. Expected: 5 PRs. (The `.release-trigger` files can be deleted in a follow-up `chore` commit after the release lands — leave them for now.)

---

## Phase 5 — Re-Validate Dry-Run (Strict Pass)

### Task 12: Final dry-run validation before push

**Files:** none

- [ ] **Step 1: Re-run dry-run and apply the strict 5-criteria check from Task 10 Step 3**

```sh
npx -y release-please@latest release-pr \
  --token="$GH_RELEASE_PLEASE_PAT" \
  --repo-url=hop-top/xrr-poly \
  --config-file=.github/release-please-config.json \
  --manifest-file=.github/.release-please-manifest.json \
  --dry-run 2>&1 | tee /tmp/release-please-dryrun-final.log
```

Then re-run all 5 checks from Task 10 Step 3 against `/tmp/release-please-dryrun-final.log`.

Expected: all 5 PASS.

- [ ] **Step 2: If any criterion fails — STOP**

Do not proceed to rename or PR. Diagnose using Task 10 Step 4's table. Re-run.

- [ ] **Step 3: All PASS — proceed**

Capture the dry-run output for the PR body:

```sh
cat /tmp/release-please-dryrun-final.log > /tmp/pr-dryrun-evidence.txt
```

---

## Phase 6 — Repo Rename

### Task 13: Rename `hop-top/xrr-poly → hop-top/poly-xrr`

**Files:** none (remote operation + local remote URL update)

- [ ] **Step 1: Perform the rename via gh**

```sh
gh repo rename poly-xrr --repo hop-top/xrr-poly --yes
```

Expected: output like `https://github.com/hop-top/poly-xrr`.

- [ ] **Step 2: Update local remote URL**

```sh
git remote set-url origin git@github.com:hop-top/poly-xrr.git
git remote -v
```

Expected: both fetch + push lines show `git@github.com:hop-top/poly-xrr.git`.

- [ ] **Step 3: Verify the rename works**

```sh
git fetch origin --dry-run
```

Expected: no error.

- [ ] **Step 4: Scan for stale references to `xrr-poly` in repo content**

```sh
grep -rIn 'xrr-poly' . --exclude-dir=.git --exclude-dir=node_modules 2>&1 | grep -v '^\.xray_' || echo "no remaining xrr-poly references"
```

If matches appear in `README.md` or `docs/`: update them in Step 5. Workflow/config files should NOT reference `xrr-poly` directly (the publish workflow uses `hop.top/xrr` for homepage and per-component mirror names, not the source repo name).

- [ ] **Step 5: Fix any stale references found**

If grep returned content-file hits, edit each one to swap `xrr-poly → poly-xrr`. Then:

```sh
git add -u
git commit -m "docs: update references from xrr-poly to poly-xrr after rename"
```

If grep returned nothing, skip this step.

- [ ] **Step 6: No release-please re-run needed**

GitHub auto-redirects the old repo URL indefinitely, so the `--repo-url=hop-top/xrr-poly` invocation above will continue to resolve. Future runs should use `hop-top/poly-xrr` for clarity but the redirect is durable.

---

## Phase 7 — Open the PR

### Task 14: Push branch + create PR

**Files:** none (git operations)

- [ ] **Step 1: Verify local main state**

```sh
git log origin/main..main --format='%h %s'
```

Expected (in order, oldest to newest):
1. `23f6c2e ci: bump actions/checkout v4→v5 and actions/setup-node v4→v5` (the PR #7 squash)
2. `7724596 docs(spec): release publishing refactor design`
3. `<sha> chore(release): pre-seed manifest versions to 0.1.0-alpha.3`
4. `<sha> chore(ts): add ci:build script for publish workflow`
5. `<sha> ci: adopt hop-top release-please + publish-on-tag conventions`
6. 5 × `chore(<lang>): bootstrap 0.1.0-alpha.4 release` (Task 11)
7. (optional) `docs: update references from xrr-poly to poly-xrr after rename` (Task 13 Step 5)

Total: 10–11 commits ahead of `origin/main`.

- [ ] **Step 2: Push a topic branch** (don't push directly to main)

```sh
git checkout -b release-conv
git push -u origin release-conv
```

Expected: branch created on origin (`hop-top/poly-xrr`).

- [ ] **Step 3: Create the PR**

```sh
gh pr create \
  --base main \
  --head release-conv \
  --title "ci: adopt hop-top release/publish conventions; rename to poly-xrr" \
  --body "$(cat <<'EOF'
## Summary

Brings this repo onto the hop-top release/publish standard already in
production on `hop-top/poly-uri` (and `hop-top/poly-kit`):

- Replaces `split-publish.yml` + `nightly-publish.yml` with
  release-please + `publish-on-tag.yml@v0`.
- Adds `release-please-config.json` + manifest seeded at
  `0.1.0-alpha.3`.
- Adds 5 per-component bootstrap commits with
  `Release-As: 0.1.0-alpha.4` footers.
- Renames the source repo from `xrr-poly` to `poly-xrr` (hop-top
  polyglot convention is `poly-*`).

Spec: `docs/superpowers/specs/2026-05-17-release-publishing-refactor-design.md`
Plan: `docs/superpowers/plans/2026-05-17-release-publishing-refactor.md`

## Dry-run evidence

The plan gates on `release-please --dry-run` showing exactly 5
proposed PRs at `0.1.0-alpha.4` for components
`xrr`/`xrr-ts`/`xrr-py`/`xrr-rs`/`xrr-php`, no `xrr-poly` umbrella PR.
Final dry-run captured in `/tmp/pr-dryrun-evidence.txt`; paste below
after this PR's CI green:

```
<paste contents of /tmp/pr-dryrun-evidence.txt here>
```

## Secrets verified

(From Task 1 pre-flight — paste actual findings here)

- `GH_RELEASE_PLEASE_PAT`: ✓ / MISSING
- `GH_MIRROR_PAT`:         ✓ / MISSING
- `NPM_REGISTRY_TOKEN`:    ✓ / MISSING
- `CARGO_REGISTRY_TOKEN`:  ✓ / MISSING
- `PYPI_REGISTRY_TOKEN`:   ✓ / MISSING

## Mirror repos

Will be auto-created on first publish: `hop-top/xrr-py`,
`hop-top/xrr-rs`, `hop-top/xrr-php`.

Already exist: `hop-top/xrr`, `hop-top/xrr-ts`.

## Out of scope (parking lot)

- Nightly publish lane (`-nightly.YYYYMMDD`)
- LTS branch cut (`release/0.1`) — only after stable `0.1.0`
- `release-bot` GitHub App token (poly-kit pattern)
- Branch protection updates for `main`
- Revoke old `SPLIT_TOKEN` after merge

## Test plan

- [ ] CI green on this PR (`Go` / `TypeScript` / `Python` / `PHP` / `Rust`)
- [ ] After merge: release-please opens 5 standing PRs (one per component) at `0.1.0-alpha.4`
- [ ] Merge each standing PR → 5 tags created (`<component>/v0.1.0-alpha.4`)
- [ ] `publish.yml` runs successfully on each tag — registry pushes succeed, mirrors auto-create and receive content
EOF
)"
```

Expected: PR URL printed.

- [ ] **Step 4: Verify PR opened cleanly**

```sh
gh pr view --json url,state,title
```

Expected: `OPEN`, matching title.

---

### Task 15: Close superseded PR #7

**Files:** none

- [ ] **Step 1: Get the new PR URL**

```sh
NEW_PR=$(gh pr view --json url --jq .url)
echo "$NEW_PR"
```

Expected: the URL printed by Task 14 Step 3.

- [ ] **Step 2: Close PR #7 with back-reference**

```sh
gh pr close 7 --comment "Superseded by $NEW_PR. The actions v5 bump was already squash-merged locally as commit \`23f6c2e\` and is included in the supersession PR's base."
```

Expected: PR #7 transitions to `CLOSED`.

- [ ] **Step 3: Verify**

```sh
gh pr view 7 --json state,closed
```

Expected: `"state": "CLOSED"`.

---

## Phase 8 — Post-Merge (out of scope for this PR but documented)

After the PR in Task 14 merges:

1. **release-please opens 5 standing PRs** (titles like `chore(release):xrr v0.1.0-alpha.4`).
2. **Merge each standing PR** → release-please creates 5 tags: `xrr/v0.1.0-alpha.4`, `xrr-ts/v0.1.0-alpha.4`, etc.
3. **`publish.yml` triggers per tag** → per-language publish + mirror push.
4. **Verify on each registry**: npm `@hop-top/xrr@0.1.0-alpha.4`, PyPI `hop-top-xrr 0.1.0-alpha.4`, crates.io `hop-top-xrr 0.1.0-alpha.4`, Packagist `hop-top/xrr 0.1.0-alpha.4`, `go install hop.top/xrr@v0.1.0-alpha.4`.
5. **Verify mirror repos** (`hop-top/xrr-py`, `xrr-rs`, `xrr-php`) auto-created with subtree content.
6. **Revoke old `SPLIT_TOKEN`** (no longer used).
7. **Mark tlc track `release-conv` as completed.**

---

## Plan Self-Review

**Spec coverage check** (each spec section → task):

| Spec section | Covered by |
|---|---|
| Components table | Task 6 (config) + Task 8 (publish.yml ecosystems) |
| Umbrella suppression (Y) | Task 6 (no umbrella key in `packages`) |
| File layout | File Structure section + Tasks 6, 7, 8, 9 |
| `release-please-config.json` | Task 6 |
| Manifest bootstrap | Task 7 + Task 4 (source pre-bump) |
| `release-please.yml` | Task 8 |
| `publish.yml` | Task 8 |
| Secrets required | Task 1 |
| Mirror repos | Task 2 + Phase 8 note 5 |
| Phase 1 Pre-flight | Tasks 1, 2, 3 |
| Phase 2 Land conventions | Tasks 4–9 |
| Phase 3 Dry-run gate | Task 10 |
| Phase 4 Trigger commits | Task 11 |
| Phase 5 Re-dry-run | Task 12 |
| Phase 6 Repo rename | Task 13 |
| Phase 7 Open PR | Task 14 |
| Phase 8 PR #7 disposition | Task 15 |
| Risks & mitigations | Embedded throughout (Task 10 Step 4 fix-table, Task 11 Step 4b fallback) |

All spec sections covered.

**Placeholder scan:** no "TBD", no "implement later", every command/code block shown verbatim.

**Type consistency:** component names (`xrr`, `xrr-ts`, `xrr-py`, `xrr-rs`, `xrr-php`) used identically in Tasks 6, 8, 10, 11, 12, 14. No drift.
