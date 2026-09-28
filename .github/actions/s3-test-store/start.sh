#!/usr/bin/env bash
# Starts the S3-compatible test store (a MinIO server) for the integration
# tests, and says whether it is ready.
#
# This script NEVER fails the step. When the store cannot start, it writes
# ready=false and an empty endpoint, prints why, and exits 0, so every other
# integration test still runs. The S3 tests skip on an empty endpoint, and the
# s3-test-store-check action turns the job red afterwards. A store that did
# not start is a red result, never a silent skip.
#
# Environment:
#   S3_STORE_IMAGE         image reference, pinned by digest (required)
#   S3_STORE_PULL_TIMEOUT  seconds allowed for the pull (default 180)
#   S3_STORE_WAIT          seconds allowed for the health check (default 60)
#   GITHUB_OUTPUT          file the step outputs are appended to
set -uo pipefail

name=bintrail-test-minio
port=9000
endpoint="http://127.0.0.1:${port}"
pull_timeout="${S3_STORE_PULL_TIMEOUT:-180}"
wait_seconds="${S3_STORE_WAIT:-60}"
out="${GITHUB_OUTPUT:-/dev/stdout}"

not_ready() {
  echo "::error title=S3-compatible test store::$1"
  echo "The S3-compatible tests will skip, and the S3 store check at the end of this job will fail."
  {
    echo "ready=false"
    echo "endpoint="
  } >>"$out"
  exit 0
}

if [ -z "${S3_STORE_IMAGE:-}" ]; then
  not_ready "no image reference given"
fi

echo "Pulling ${S3_STORE_IMAGE} (at most ${pull_timeout}s)"
if ! timeout "${pull_timeout}" docker pull "${S3_STORE_IMAGE}"; then
  not_ready "could not pull ${S3_STORE_IMAGE}"
fi

if ! docker run -d --name "${name}" \
  -e MINIO_ROOT_USER=bintrail \
  -e MINIO_ROOT_PASSWORD=bintrail-it-secret \
  -p "${port}:9000" \
  "${S3_STORE_IMAGE}" server /data; then
  docker logs "${name}" 2>&1 || true
  not_ready "the container did not start"
fi

echo "Waiting for ${endpoint}/minio/health/live (at most ${wait_seconds}s)"
for _ in $(seq 1 "${wait_seconds}"); do
  if curl -sf "${endpoint}/minio/health/live" >/dev/null; then
    echo "S3-compatible test store is ready at ${endpoint}"
    {
      echo "ready=true"
      echo "endpoint=${endpoint}"
    } >>"$out"
    exit 0
  fi
  sleep 1
done

echo "Container log:"
docker logs "${name}" 2>&1 || true
not_ready "the store started but did not answer its health check within ${wait_seconds}s"
