#!/usr/bin/env bash
# Keeps ci.yml, release.yaml and the image mirror from drifting apart on the
# S3-compatible test store (#1884). Run from the repository root, or pass the
# root as the first argument. Exits 1 and says what diverged.
#
# 1. ci.yml and release.yaml each start the store through
#    ./.github/actions/s3-test-store, check it through
#    ./.github/actions/s3-test-store-check, and hand the store's endpoint to
#    the tests. What the release runs is what CI proves.
# 2. No workflow names the image itself: the action is the one place, and
#    the mirror workflow is the one exception.
# 3. The tag and digest the action pulls are the tag and digest the mirror
#    copies. A digest bumped in one place and not the other would pull an
#    image the mirror never received.
set -euo pipefail

root="${1:-.}"
action="$root/.github/actions/s3-test-store/action.yml"
mirror="$root/.github/workflows/mirror-ci-images.yml"
status=0

fail() {
  echo "s3 store lockstep: $1" >&2
  status=1
}

count() { # count <fixed string> <file>
  grep -cF -- "$1" "$2" || true
}

for wf in ci.yml release.yaml; do
  f="$root/.github/workflows/$wf"
  [ -f "$f" ] || { fail "$wf is missing"; continue; }
  n="$(grep -cE '^[[:space:]]*uses: \./\.github/actions/s3-test-store[[:space:]]*$' "$f" || true)"
  [ "$n" = 1 ] || fail "$wf uses ./.github/actions/s3-test-store $n times, want 1"
  n="$(grep -cE '^[[:space:]]*uses: \./\.github/actions/s3-test-store-check[[:space:]]*$' "$f" || true)"
  [ "$n" = 1 ] || fail "$wf uses ./.github/actions/s3-test-store-check $n times, want 1"
  # shellcheck disable=SC2016 # the literal workflow expression is the point
  n="$(count 'BINTRAIL_TEST_MINIO_ENDPOINT: ${{ steps.s3store.outputs.endpoint }}' "$f")"
  [ "$n" = 1 ] || fail "$wf does not pass the store endpoint to the tests exactly once (found $n)"
  # shellcheck disable=SC2016
  n="$(count 'ready: ${{ steps.s3store.outputs.ready }}' "$f")"
  [ "$n" = 1 ] || fail "$wf does not pass the store readiness to the check exactly once (found $n)"
done

for f in "$root"/.github/workflows/*.yml "$root"/.github/workflows/*.yaml; do
  [ -f "$f" ] || continue
  [ "$f" = "$mirror" ] && continue
  if grep -qE 'minio:RELEASE\.' "$f"; then
    fail "$(basename "$f") names the MinIO image; use ./.github/actions/s3-test-store"
  fi
done

ref="$(sed -nE 's/^[[:space:]]*default: ([^[:space:]]+@sha256:[0-9a-f]{64})[[:space:]]*$/\1/p' "$action" 2>/dev/null || true)"
if [ "$(printf '%s' "$ref" | grep -c . || true)" != 1 ]; then
  fail "the action needs exactly one image default pinned by digest, found: '${ref}'"
else
  action_digest="${ref##*@}"
  action_tag="${ref%@*}"
  action_tag="${action_tag##*:}"
  mirror_digest="$(sed -nE 's/^[[:space:]]*DIGEST: (sha256:[0-9a-f]{64})[[:space:]]*$/\1/p' "$mirror" 2>/dev/null || true)"
  mirror_tag="$(sed -nE 's/^[[:space:]]*TAG: ([^[:space:]]+)[[:space:]]*$/\1/p' "$mirror" 2>/dev/null || true)"
  [ -n "$mirror_digest" ] && [ "$(printf '%s\n' "$mirror_digest" | wc -l | tr -d ' ')" = 1 ] \
    || fail "mirror-ci-images.yml needs exactly one DIGEST, found: '${mirror_digest}'"
  [ -n "$mirror_tag" ] && [ "$(printf '%s\n' "$mirror_tag" | wc -l | tr -d ' ')" = 1 ] \
    || fail "mirror-ci-images.yml needs exactly one TAG, found: '${mirror_tag}'"
  [ "$action_digest" = "$mirror_digest" ] \
    || fail "digest differs: the action pulls $action_digest, the mirror copies $mirror_digest"
  [ "$action_tag" = "$mirror_tag" ] \
    || fail "tag differs: the action pulls $action_tag, the mirror copies $mirror_tag"
fi

[ "$status" = 0 ] && echo "s3 store lockstep: ok"
exit "$status"
