#!/usr/bin/env bash
# Fails the job unless the S3-compatible leg of the integration tests really
# ran and passed. Pairs with the s3-test-store action.
#
# Environment:
#   S3_STORE_READY  the `ready` output of s3-test-store ("true" or anything else)
#   S3_TEST_LOG     the `go test -v` output of the integration tests
#
# Anything but ready=true fails, including an empty value: a start step that
# was skipped or never wired is not a green S3 leg.
set -uo pipefail

fail() {
  echo "::error title=S3-compatible leg::$1"
  exit 1
}

if [ "${S3_STORE_READY:-}" != "true" ]; then
  fail "the S3-compatible test store did not start, so the S3 tests did not run. The reason is in the log of the step that starts it."
fi

if [ -z "${S3_TEST_LOG:-}" ] || [ ! -f "${S3_TEST_LOG}" ]; then
  fail "no integration test log at '${S3_TEST_LOG:-}'"
fi

# Anchored: the top-level line is "--- PASS: TestS3Compat_MinIO (1.23s)".
# A bare prefix would also match TestS3Compat_MinIO_bucketStore.
if ! grep -qE '^--- PASS: TestS3Compat_MinIO \(' "${S3_TEST_LOG}"; then
  fail "the store was up but TestS3Compat_MinIO did not pass. False-green tripwire."
fi

echo "S3-compatible leg ran and passed."
