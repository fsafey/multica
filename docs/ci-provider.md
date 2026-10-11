# CI provider operator

The shared registry and operator live in Standalone Search at
`config/ci-providers.json` and `scripts/ci/provider_swap.py`.
This repository's `scripts/ci-provider` forwards its Git remote identity and
all operator arguments to that entry point. Set `SEARCH_CI_REPO` when the shared
checkout is not the sibling `standalone-search` directory.

```sh
scripts/ci-provider --help
scripts/ci-provider
scripts/ci-provider --to github
scripts/ci-provider --to cloudbuild
```

No `--to` selects the registry's preferred provider, Codex. The default operation
is a read-only plan. GitHub Actions and Google Cloud Build remain selectable
secondary providers; selection does not establish qualification or activation.
The registry declares this repository inventory-only. An activation attempt must
fail until an accepted full workload and native provider adapter exist. A plan
can read live repository gates; these local files do not prove live required-check
settings or successful provider execution.

## Existing workload boundary

Current source workflow: CI. `changes`, `sqlc-check`, frontend build/test/views shards and aggregate, backend tests/agent tests/vulnerability scan and aggregate, `windows-execenv`, `macos-runtime`, `image-budget`, `installer`, `script-checks`, and `frontend-quality`. Mobile Verify is a separate affected-path check.
The existing GitHub Actions workflows remain intact and own execution until a
replacement is qualified against the exact workload and required checks.

Separate operations: release, desktop packaging, UI performance, OpenClaw compatibility smoke and production operations. Provider planning does not authorize or migrate
these operations.

`windows-execenv`, `macos-runtime`, and the Windows/macOS `installer` matrix lanes require their native operating systems. Linux Codex and Linux Cloud Build cannot replace them. Activation must fail rather than omit these checks; qualifying only Linux lanes is insufficient. The remote identity is `fsafey/multica`; the pristine sibling `multica` checkout is a read-only upstream reference.

## Deterministic portable workload

`config/ci-workload.json` declares the existing workflow commands, and
`scripts/ci-cloud` delegates to the shared `scripts/ci/portable_codex_job.py`.
It runs all portable scopes, records real exits and logs, and exports
`CI_OUTPUT_DIR/result.json`. This is candidate workload evidence with
`app_bound_proof=false`; provider-native publication and acceptance remain
separate coordinator requirements.

The external supervisor must set `CI_PROVIDER=codex`, `CODEX_CI_RUN_ID`,
`CI_EVENT=push|pull_request`, `CI_EVENT_HEAD` (full SHA), `CI_EVENT_BRANCH`,
and a new absolute `CI_OUTPUT_DIR` outside the tested checkout. For pull requests,
set `CI_BASE_SHA`, positive `CI_PR`, and `CI_DRAFT=false`; the clean checkout must
be the exact base/head merge, with both its parents and merged tree verified.
Push checkouts must match the exact event head. The runner requires native Linux,
Bash, uv, and the workload's declared toolchain. A source mismatch, missing tool,
failed command, timeout or interruption produces a nonzero result.

```sh
# Run only from the supervisor-prepared clean source and environment.
SEARCH_CI_REPO=/path/to/reviewed/standalone-search scripts/ci-cloud
```

The Linux lane runs every portable scope, both views shards, mobile checks and image-budget checks for PRs, with forced fresh Turbo execution. Backend tests own disposable pgvector PostgreSQL 17 and Redis 7 containers on random loopback ports; their migrations apply only inside those containers, which are removed on exit. A Docker/Go/Helm/pnpm capability gap fails the actual check. Windows execution, macOS runtime and native installer lanes remain unsupported here and prevent full qualification. Existing GitHub aggregates/path filtering remain coordinator requirements. Knip remains advisory with its actual exit recorded.
