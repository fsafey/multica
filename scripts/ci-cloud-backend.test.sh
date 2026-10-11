#!/usr/bin/env bash
# Exercise service isolation and cleanup on a real failed child command.
set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -r "$scratch"' EXIT
mkdir -p "$scratch/bin" "$scratch/fixture/scripts" "$scratch/fixture/server"
cp "$repo_root/scripts/ci-cloud-backend" "$scratch/fixture/scripts/ci-cloud-backend"
cat > "$scratch/bin/docker" <<'STUB'
#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >> "$TEST_LOG"
case "$1" in
  container) exit 1 ;;
  port)
    if [[ "$3" = 5432/tcp ]]; then echo 127.0.0.1:15432; else echo 127.0.0.1:16379; fi ;;
  run|exec|rm) exit 0 ;;
  *) exit 99 ;;
esac
STUB
cat > "$scratch/bin/go" <<'STUB'
#!/usr/bin/env bash
set -eu
[[ "$DATABASE_URL" = postgres://multica:multica@127.0.0.1:15432/multica?sslmode=disable ]]
[[ "$REDIS_TEST_URL" = redis://127.0.0.1:16379/1 ]]
[[ "$GOFLAGS" = -count=1 ]]
if [[ "$1" = run && "${FAIL_MIGRATION:-}" = true ]]; then exit 23; fi
STUB
cat > "$scratch/fixture/scripts/test-go.test.sh" <<'STUB'
#!/usr/bin/env bash
echo wrapper-ran >> "$TEST_LOG"
STUB
cat > "$scratch/fixture/scripts/test-go.sh" <<'STUB'
#!/usr/bin/env bash
echo suite-ran >> "$TEST_LOG"
STUB
chmod +x "$scratch/bin/docker" "$scratch/bin/go"
export PATH="$scratch/bin:$PATH" TEST_LOG="$scratch/commands.log" CI_OUTPUT_DIR="$scratch/evidence"
export DATABASE_URL=forbidden-host-database REDIS_TEST_URL=forbidden-host-redis
set +e
(cd "$scratch/fixture" && FAIL_MIGRATION=true bash scripts/ci-cloud-backend)
code=$?
set -e
[[ "$code" = 23 ]]
[[ "$(tail -n 1 "$TEST_LOG")" = rm\ --force\ multica-ci-pg-* ]]
if [[ "$(cat "$TEST_LOG")" = *suite-ran* ]]; then
  echo 'Suite ran after failed migration' >&2
  exit 1
fi
: > "$TEST_LOG"
(cd "$scratch/fixture" && bash scripts/ci-cloud-backend)
[[ "$(cat "$TEST_LOG")" = *suite-ran* ]]
[[ "$(tail -n 1 "$TEST_LOG")" = rm\ --force\ multica-ci-pg-* ]]
echo 'PASS disposable service isolation, failure propagation and owned cleanup'
