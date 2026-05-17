# Release Publishing Refactor — adopt hop-top conventions

**Date:** 2026-05-17
**Track:** `release-conv` (tlc)
**Owner:** Noor (`jad+noor@ideacrafters.com`)
**Status:** Approved, ready for implementation plan

## Problem

This repo (`hop-top/xrr-poly`) currently ships only a pair of subtree-push
workflows (`split-publish.yml`, `nightly-publish.yml`) using `SPLIT_TOKEN`.
It has no release-please, no per-language registry publishing, no
conventional versioning, no changelog automation. It also diverges from
the hop-top naming convention: polyglot source repos are prefixed
`poly-*`, not suffixed `*-poly`.

Goal: bring this repo onto the hop-top release/publish standard already in
production on `hop-top/poly-uri` (and `hop-top/poly-kit`), then ship
`0.1.0-alpha.4` of every component to its registry. Final state: source
repo renamed to `hop-top/poly-xrr`, sibling mirrors auto-managed from
tags, release-please opens standing PRs on every conventional commit.

## Scope

In:
- release-please configuration (5 publishable components + 1 suppressed umbrella)
- `publish.yml` calling `hop-top/.github/.../publish-on-tag.yml@v0`
- `release-please.yml` calling `googleapis/release-please-action@v4`
- Deletion of `split-publish.yml` + `nightly-publish.yml`
- Pre-seeding source manifests to `0.1.0-alpha.3`
- `--dry-run` gate before any tag is created
- Repo rename `xrr-poly → poly-xrr`
- Disposition of open PR #7

Out (parking lot):
- Nightly publish lane (`-nightly.YYYYMMDD`)
- LTS branch cut (`release/0.1`) — only after stable `0.1.0`
- `release-bot` GitHub App token (poly-kit pattern) — not needed pre-CODEOWNERS
- Branch protection updates for `main`

## Architecture

```
commit lands on main
       ↓
release-please.yml (consumer-owned, here)
       ↓ opens
standing release PRs (one per component, separate-pull-requests=true)
       ↓ merge
release-please creates tag <component>/v<version>
       ↓ tag push matches '*/v*'
publish.yml (consumer-owned, here)
       ↓ uses
hop-top/.github/.../publish-on-tag.yml@v0  (reusable)
       ↓ dispatches per ecosystem
publish-{ts,py,rs}.yml + mirror-subtree.yml
       ↓
registry publish + mirror push
```

## Components

| component | dir | release-type | ecosystem | package name | mirror | publish target |
|---|---|---|---|---|---|---|
| `xrr` | `go` | `go` | `go` | `hop.top/xrr` | `hop-top/xrr` | proxy.golang.org (auto, tag-driven) |
| `xrr-ts` | `ts` | `node` | `ts` | `@hop-top/xrr` | `hop-top/xrr-ts` | npm |
| `xrr-py` | `py` | `python` | `py` | `hop-top-xrr` | `hop-top/xrr-py` | PyPI (token auth) |
| `xrr-rs` | `rs` | `rust` | `rs` | `hop-top-xrr` | `hop-top/xrr-rs` | crates.io |
| `xrr-php` | `php` | `php` | `php` | `hop-top/xrr` | `hop-top/xrr-php` | Packagist (webhook) |

Registry name verification (2026-05-17): all four (`@hop-top/xrr`,
`hop-top-xrr` on PyPI, `hop-top-xrr` on crates.io, `hop-top/xrr` on
Packagist) returned 404 — available.

Crate name uses `hop-top-xrr` (not bare `xrr`) to dodge the generic-name
collision risk on crates.io and to mirror the `hop-top-uri` precedent.
PyPI name normalizes identically.

### Umbrella suppression (decision Y)

The `xrr-poly` umbrella component is **not** included in
release-please's `packages` map. Root-level commits (docs, spec, CI)
will not trigger an umbrella release. This avoids a tag (`xrr-poly/v...`)
that `publish-on-tag.yml`'s parse step would fail on (no matching
`ecosystems:` entry). Consequence: root-only commits do not appear in
any component's CHANGELOG. Accepted — they will appear in `git log` and
this design doc.

## File layout

```
.github/
├── release-please-config.json     # NEW — 5 components, no umbrella
├── .release-please-manifest.json  # NEW — seeded at 0.1.0-alpha.3 for all 5
└── workflows/
    ├── ci.yml                     # untouched (already on actions/checkout@v5)
    ├── release-please.yml         # NEW
    └── publish.yml                # NEW — replaces split + nightly
```

Deleted:
- `.github/workflows/split-publish.yml`
- `.github/workflows/nightly-publish.yml`

## release-please-config.json

Mirrors `hop-top/poly-uri`'s shape exactly (same dir layout, same naming
shape). Key fields per component: `release-type`, `component`,
`changelog-path`, `bump-minor-pre-major: true`, `prerelease: true`,
`prerelease-type: "alpha.0"`, `versioning: "prerelease"`. Top-level:
`separate-pull-requests: true`, `include-component-in-tag: true`,
`tag-separator: "/"`.

The `py` component carries an `extra-files` entry to bump
`pyproject.toml`'s `$.project.version` — same as `poly-uri`. The `rs`
and `ts` components rely on release-please's built-in handling of
`Cargo.toml` and `package.json`. PHP and Go have no embedded version
fields; tag-driven.

## Manifest bootstrap

```json
{
  "go":  "0.1.0-alpha.3",
  "ts":  "0.1.0-alpha.3",
  "py":  "0.1.0-alpha.3",
  "rs":  "0.1.0-alpha.3",
  "php": "0.1.0-alpha.3"
}
```

Pre-seeded so the next conventional commit advances each component to
`0.1.0-alpha.4`. Source files (`ts/package.json`, `py/pyproject.toml`,
`rs/Cargo.toml`) are bumped from `0.1.0` to `0.1.0-alpha.3` in the same
PR to prevent release-please drift detection.

## release-please.yml

Identical to `poly-uri`'s. No GitHub App; uses `GH_RELEASE_PLEASE_PAT`
directly. Triggers on push to `main`.

## publish.yml

Triggers on tag push matching `*/v*`. Calls
`hop-top/.github/.../publish-on-tag.yml@v0` (rolling major pin,
recommended in the dotgithub SKILL). Passes 4 secrets, sets `homepage:
https://hop.top/xrr`, `description-prefix: "READ-ONLY MIRROR"`.

The `ecosystems:` map follows `poly-uri`'s pattern, including:

- `pypi-auth: token` on `xrr-py` (mirrors poly-uri + poly-kit; OIDC
  trusted publishing has been flaky for them, token auth is the safe
  default until `hop-top/.github#10` is debugged upstream)
- `test-command: pnpm dlx --config.ignore-scripts=true vitest@3 run` on
  `xrr-ts` (no `pnpm-lock.yaml` exists in `ts/`; dlx-based test avoids
  installing node_modules)
- `build-command: pnpm ci:build` on `xrr-ts` (new script added in this
  PR; runs `tsc --build`)
- `test-command: pip install -e . && pytest` on `xrr-py` (tests import
  the package)

Permissions: `contents: read`, `id-token: write` (harmless under token
auth; left for future OIDC flip without permission churn).

## Secrets required

| secret | scope | used by | notes |
|---|---|---|---|
| `GH_RELEASE_PLEASE_PAT` | org or repo | `release-please.yml` | fine-grained PAT: Contents RW + Pull Requests RW + Workflows RW on this repo |
| `GH_MIRROR_PAT` | org | `mirror-subtree.yml` | Administration RW + Contents RW on every `xrr-*` mirror repo |
| `NPM_REGISTRY_TOKEN` | org | `publish-ts.yml` | npm Granular Access Token, publish on `@hop-top/*` |
| `CARGO_REGISTRY_TOKEN` | org | `publish-rs.yml` | crates.io API token, account email verified |
| `PYPI_REGISTRY_TOKEN` | repo or org | `publish-py.yml` | PyPI API token scoped to `hop-top-xrr` |

`SPLIT_TOKEN` (used by the soon-deleted workflows) can be revoked after
PR merge.

Verification: pre-flight runs `gh secret list --repo hop-top/xrr-poly`
and `gh secret list --org hop-top` (if access permits). Missing secrets
are flagged before any push.

## Mirror repos

Existing: `hop-top/xrr` ✓, `hop-top/xrr-ts` ✓.

Auto-created on first publish by `mirror-subtree.yml`:
- `hop-top/xrr-py`
- `hop-top/xrr-rs`
- `hop-top/xrr-php`

## Execution plan

### Phase 1 — Pre-flight (read-only)

1. List org+repo secrets via `gh secret list`. Flag any missing.
2. Confirm mirror repos that will need auto-creation.
3. Probe `poly-uri`'s actual handling of `uri-poly` tag pushes to
   verify umbrella suppression decision Y is consistent with their setup.

### Phase 2 — Land conventions (one PR on `main`)

Commits, in order:

```
chore(release): pre-seed manifest versions to 0.1.0-alpha.3
chore(ts): add ci:build script for publish workflow
ci: adopt hop-top release-please + publish-on-tag conventions
```

The third commit:
- adds `.github/release-please-config.json`
- adds `.github/.release-please-manifest.json` (seeded at alpha.3)
- adds `.github/workflows/release-please.yml`
- adds `.github/workflows/publish.yml`
- deletes `.github/workflows/split-publish.yml`
- deletes `.github/workflows/nightly-publish.yml`

### Phase 3 — `release-please --dry-run` gate

Run:

```sh
npx release-please release-pr \
  --token=$GH_RELEASE_PLEASE_PAT \
  --repo-url=hop-top/xrr-poly \
  --config-file=.github/release-please-config.json \
  --manifest-file=.github/.release-please-manifest.json \
  --dry-run
```

(Repo URL switches to `hop-top/poly-xrr` after Phase 6.)

**Pass criteria (all 5 must hold before proceeding):**

| # | criterion |
|---|---|
| 1 | dry-run exits 0 |
| 2 | All 5 components (`xrr`, `xrr-ts`, `xrr-py`, `xrr-rs`, `xrr-php`) appear in output |
| 3 | Each proposed version is `0.1.0-alpha.4` |
| 4 | Each proposed tag is `<component>/v0.1.0-alpha.4` |
| 5 | No umbrella `xrr-poly` PR is proposed |

If any fails: stop, fix config, re-run. No tags, no merges, until all 5
pass.

### Phase 4 — Trigger commits

After Phase 3 passes: 5 empty commits, one per component, each carrying
a `Release-As: 0.1.0-alpha.4` footer to force the exact version on the
next standing PR. Empty commits avoid path-attribution ambiguity; if
release-please doesn't route them per-component as expected, fall back
to one no-op file change per component dir (`<dir>/.release-trigger`)
with the footer.

### Phase 5 — Re-run `--dry-run`

After trigger commits: expect 5 PR proposals (one per component). If
the count differs: stop, fix, re-trigger. Only proceed when dry-run
shows exactly 5 PRs, each at `0.1.0-alpha.4`.

### Phase 6 — Repo rename

After Phase 5 passes:

1. `gh repo rename poly-xrr --repo hop-top/xrr-poly`
2. `git remote set-url origin git@github.com:hop-top/poly-xrr.git`
3. Update repo references:
   - `README.md` (any `xrr-poly` strings)
   - `.github/workflows/publish.yml` `homepage:` if it pointed at the
     old slug (it doesn't — uses `hop.top/xrr`)
4. Verify `git fetch origin` post-rename.

GitHub auto-redirects old refs (`xrr-poly` → `poly-xrr`) indefinitely,
but the explicit `set-url` ensures the local remote is clean.

### Phase 7 — Open PR

```sh
gh pr create \
  --title "ci: adopt hop-top release/publish conventions; rename to poly-xrr" \
  --body "<plan, dry-run output, secrets verified, mirrors to be auto-created>"
```

PR contains Phase 2 commits + Phase 6 rename effects. Phase 4 trigger
commits land **after** PR merge — that's the path release-please picks
them up on.

### Phase 8 — PR #7 disposition

PR #7 (actions v5 bump) was already squash-merged locally as commit
`23f6c2e` but never pushed to `origin`. The PR on GitHub still shows
OPEN. Close it with comment back-referencing the new PR; the squashed
content will be pushed as part of the new PR's base.

## Risks & mitigations

| risk | mitigation |
|---|---|
| Missing org secret blocks publish | Phase 1 pre-flight check |
| `--dry-run` proposes wrong versions | Phase 3 gate blocks tag creation |
| Empty trigger commits don't scope per-component | Fallback: path-touching no-op commit per dir |
| crates.io / PyPI / npm name claimed between design and ship | Phase 1 re-checks availability |
| Mirror auto-create fails (PAT scope) | `mirror-subtree.yml` fails visibly; pre-flight checks PAT |
| Repo rename breaks consumers | hop-top mirrors are read-only; no external consumers pinned to source repo |
| Old `SPLIT_TOKEN` exposed in deleted workflows | Revoke post-merge as cleanup task |

## Open questions resolved during brainstorm

- **Where did alpha.3 come from?** Mental version; no prior publishes.
  Seed manifest accordingly (decision A on question 2).
- **OIDC or token for PyPI?** Token (matches poly-uri/poly-kit
  production reality).
- **Mirror nightly?** Replaced entirely by tag-driven mirroring (decision A
  on question 4).
- **Umbrella component?** Suppress (decision Y in Section 3).
- **Trigger style for alpha.4?** Per-component `Release-As: 0.1.0-alpha.4`
  empty commits (decision A in Section 2).
