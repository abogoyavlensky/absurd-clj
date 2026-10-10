#!/usr/bin/env bash
# Cross-SDK compatibility check between absurd-clj and the Python SDK.
# Requires Docker. Run with `bb cross-sdk-test`.
set -euo pipefail
cd "$(dirname "$0")"

cleanup() {
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose up -d --wait postgres python-worker

for _ in $(seq 1 120); do
  if docker compose logs python-worker 2>/dev/null | grep -q "listening on xsdk_py"; then
    break
  fi
  sleep 1
done
docker compose logs python-worker | grep -q "listening on xsdk_py" || {
  echo "python worker did not start"; docker compose logs python-worker; exit 1;
}

PORT=$(docker compose port postgres 5432 | awk -F: '{print $NF}')
export ABSURD_DATABASE_URL="postgresql://absurd:absurd@127.0.0.1:${PORT}/absurd"

(cd .. && clojure -M:cross-sdk -m cross-sdk.main) &
CLJ_PID=$!

sleep 3
set +e
docker compose run --rm python-client
PY_STATUS=$?
wait "$CLJ_PID"
CLJ_STATUS=$?
set -e

echo "python-client exit=${PY_STATUS} clojure exit=${CLJ_STATUS}"
[ "$PY_STATUS" -eq 0 ] && [ "$CLJ_STATUS" -eq 0 ]
