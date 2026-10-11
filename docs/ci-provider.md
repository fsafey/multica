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
