#!/usr/bin/env bash
# ci.yml and release.yaml run the MySQL integration suite the same way: the
# same shards, through the same three scripts. They are two files because a
# shared workflow would rename the required checks (see the note at the top of
# release.yaml's jobs), so nothing but this check keeps them together. The
# release's copy once ran the suite as one job, with no GTID source and 7 of
# the key tests; nobody saw it until a release took an hour.
#
#   scripts/check-mysql-shard-lockstep.sh [repo root]
set -euo pipefail

root="${1:-.}"
status=0
fail() { echo "mysql shard lockstep: $1" >&2; status=1; }

want="$(bash "$root/scripts/mysql-integration-shard.sh" count)"
list="["; for i in $(seq 1 "$want"); do list="$list$i, "; done; list="${list%, }]"

for wf in ci.yml release.yaml; do
  f="$root/.github/workflows/$wf"
  [ -f "$f" ] || { fail "$wf is missing"; continue; }
  # One shard matrix, naming 1..SHARD_COUNT.
  got="$(sed -nE 's/^[[:space:]]*shard:[[:space:]]*(\[.*\])[[:space:]]*$/\1/p' "$f")"
  [ "$got" = "$list" ] || fail "$wf runs shards '${got:-none}', want $list (SHARD_COUNT of scripts/mysql-integration-shard.sh)"
  # Each step that makes the run mean something, exactly once.
  for step in \
    'bash scripts/mysql-integration-shard.sh run ${{ matrix.shard }} /tmp/mysql-it-shard/mysql-it.out' \
    'bash scripts/mysql-integration-shard.sh collect /tmp/mysql-it-shards "${{ matrix.mysql }}" /tmp/mysql-it.out' \
    'bash scripts/mysql-integration-shard.sh check-split /tmp/mysql-it.out' \
    'bash scripts/mysql-integration-key-tests.sh /tmp/mysql-it.out' \
    "BINTRAIL_REQUIRE_GTID_SOURCE: \${{ matrix.shard == 2 && '1' || '' }}" \
    'BINTRAIL_REQUIRE_MYSQL: "1"'; do
    n="$(grep -cF -- "$step" "$f" || true)"
    case "$step" in
      'BINTRAIL_REQUIRE_MYSQL: "1"') [ "$n" -ge 1 ] || fail "$wf does not set $step" ;;
      *) [ "$n" = 1 ] || fail "$wf has '$step' $n times, want 1" ;;
    esac
  done
  # A list of key tests written into a workflow is a second copy. (The
  # MariaDB and PostgreSQL suites keep theirs in their own jobs, over their
  # own logs; this is about the MySQL one.)
  if grep -F -- "--- PASS: " "$f" | grep -qF -- "/tmp/mysql-it.out"; then
    fail "$wf greps the MySQL test log for a test by name; the list lives in scripts/mysql-integration-key-tests.sh"
  fi
done
exit "$status"
