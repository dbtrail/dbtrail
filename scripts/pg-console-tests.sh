#!/usr/bin/env bash
# The console's tests that need a PostgreSQL source and a MySQL index at once
# (the "Console tests that need MySQL and PostgreSQL together" step of the
# `integration-postgres-source` job in .github/workflows/ci.yml). Run from
# the repository root.
#
#   scripts/pg-console-tests.sh pattern
#       -> the -run pattern that selects them
#   scripts/pg-console-tests.sh run <log>
#       -> build consoleapp's integration test binary, run the tests the
#          pattern selects into <log>, then `check` what ran
#   scripts/pg-console-tests.sh check <listing> <log>
#       -> <listing> is what the binary's -test.list prints, <log> the -v
#          output of the run: every listed test the pattern selects must have
#          passed or failed in the log
#   scripts/pg-console-tests.sh check-mysql-log <log>
#       -> over the MySQL shards' log: no consoleapp test skipped there for
#          want of a PostgreSQL under a name the pattern does not select
#   scripts/pg-console-tests.sh --self-test
#       -> `check` and `check-mysql-log` against made-up listings and logs
#
# Why. consoleapp's integration tests run in the MySQL shards, where the ones
# that need a PostgreSQL skip. This step is the one place they run, and it
# used to name them one by one in ci.yml: a test nobody added to the list ran
# nowhere, and every job stayed green. 17 router tests went unrun that way
# until #3057, and 9 more until this script.
#
# Which tests: the top-level tests of consoleapp whose NAME holds "PG" or
# "Postgres" (TESTS below). A name rule cannot know what a test needs, so two
# checks stand behind it:
#   - `check`, after the run: each test the binary lists under the rule has a
#     result line in the log. One that exists and has none fails the job by
#     name, and so does one that skipped, unless ALLOWED_SKIPS names it with
#     the reason. Only the test itself counts: a subtest may skip (a client
#     that is not installed on the runner, say).
#   - `check-mysql-log`, in the `integration` job: a consoleapp test that
#     skipped in the MySQL shards saying it has no PostgreSQL, under a name
#     the rule does not select, fails that job by name. That is the test the
#     rule would leave out: it runs in no job at all.
# The rule also takes tests that need no PostgreSQL (unit tests of the
# PostgreSQL port, mostly). They run here a second time, which costs seconds.
set -euo pipefail
shopt -s inherit_errexit 2>/dev/null || true

PKG="consoleapp"

# Go regular expression for -test.run, and an extended one for grep: keep it
# to what both read the same way. No "/" (that would select subtests).
TESTS='PG|Postgres'

# Tests the rule selects that may skip, each as "Name: why". Keep it short:
# an entry is a test no job runs. An entry whose test no longer exists fails
# the check.
ALLOWED_SKIPS=()

# What testutil.SkipIfNoPostgres logs when it skips (internal/testutil/
# postgres.go). check-mysql-log finds the tests that need a PostgreSQL by it.
NO_PG_REASON="BINTRAIL_TEST_PG_DSN not set"

# What scripts/mysql-integration-shard.sh writes around consoleapp's part of
# each shard (its SPLIT_MARK).
SPLIT_BEGIN="mysql-integration-shard: split begin"
SPLIT_END="mysql-integration-shard: split end"

die() {
  echo "pg-console-tests: $*" >&2
  exit 1
}

# results <log>: "<name> <PASS|FAIL|SKIP>" for every result line of a
# top-level test. The line may follow other output a test left unfinished,
# so the match is anywhere in the line; a subtest's name has a "/" and does
# not match.
results() {
  awk '
    match($0, /--- (PASS|FAIL|SKIP): [^ \/]+ \(/) {
      s = substr($0, RSTART + 4, RLENGTH - 6)
      print substr(s, 7), substr(s, 1, 4)
    }
  ' "$1"
}

# allowed_reason <name>: the reason ALLOWED_SKIPS gives for it, or status 1.
allowed_reason() {
  local entry
  for entry in ${ALLOWED_SKIPS[@]+"${ALLOWED_SKIPS[@]}"}; do
    if [ "${entry%%:*}" = "$1" ]; then
      printf '%s\n' "${entry#*: }"
      return 0
    fi
  done
  return 1
}

check() {
  local listing="$1" log="$2" names seen name verdicts entry reason
  local bad=0 total=0 passed=0 failed=0 skipped=0
  [ -f "$listing" ] || die "check: no test listing at $listing"
  [ -f "$log" ] || die "check: no test log at $log"
  names="$(grep -E '^Test' "$listing" | grep -E -- "$TESTS" | sort -u || true)"
  [ -n "$names" ] || die "check: the listing of $PKG holds no test whose name matches '$TESTS': nothing would run"
  for entry in ${ALLOWED_SKIPS[@]+"${ALLOWED_SKIPS[@]}"}; do
    case "$entry" in
      *": "?*) ;;
      *) die "check: ALLOWED_SKIPS entry '$entry' is not 'Name: why'" ;;
    esac
    if ! grep -qxF -- "${entry%%:*}" <<<"$names"; then
      echo "pg-console-tests: ALLOWED_SKIPS names ${entry%%:*}, which is not a test of $PKG the pattern selects: remove the entry" >&2
      bad=1
    fi
  done
  seen="$(results "$log")"
  while IFS= read -r name; do
    total=$((total + 1))
    verdicts="$(awk -v t="$name" '$1 == t { print $2 }' <<<"$seen" | sort -u | paste -sd' ' -)"
    case " $verdicts " in
      *" FAIL "*) failed=$((failed + 1)) ;;
      *" PASS "*) passed=$((passed + 1)) ;;
      *" SKIP "*)
        if reason="$(allowed_reason "$name")"; then
          skipped=$((skipped + 1))
          echo "pg-console-tests: $name skipped, as ALLOWED_SKIPS lets it: $reason"
        else
          echo "pg-console-tests: $PKG $name SKIPPED: a test that needs a PostgreSQL has to run here, the one job with one. Give the job what the test asks for, or name it in ALLOWED_SKIPS of scripts/pg-console-tests.sh with the reason" >&2
          bad=1
        fi
        ;;
      *)
        echo "pg-console-tests: $PKG $name did not run: the test binary lists it and the log has no result for it" >&2
        bad=1
        ;;
    esac
  done <<<"$names"
  [ "$bad" = 0 ] || die "check: not every $PKG test whose name matches '$TESTS' ran"
  echo "pg-console-tests: each of the $total $PKG tests whose name matches '$TESTS' ran: $passed passed, $failed failed, $skipped skipped as allowed"
}

check_mysql_log() {
  local log="$1" skipped name bad=0 total=0
  [ -f "$log" ] || die "check-mysql-log: no log at $log"
  # Top-level tests of consoleapp (between the split marks) that skipped
  # right after logging that they have no PostgreSQL.
  skipped="$(awk -v begin="$SPLIT_BEGIN" -v end="$SPLIT_END" -v why="$NO_PG_REASON" '
    index($0, begin) == 1 { inside = 1; nopg = 0; next }
    index($0, end) == 1 { inside = 0; next }
    !inside { next }
    /=== RUN   [^ \/]+$/ { nopg = 0 }
    index($0, why) { nopg = 1 }
    match($0, /--- (PASS|FAIL|SKIP): [^ \/]+ \(/) {
      s = substr($0, RSTART + 4, RLENGTH - 6)
      if (nopg && substr(s, 1, 4) == "SKIP") print substr(s, 7)
      nopg = 0
    }
  ' "$log" | sort -u)"
  # None at all means this check reads nothing: the reason's text changed,
  # or the split marks did.
  [ -n "$skipped" ] \
    || die "check-mysql-log: no $PKG test skipped saying '$NO_PG_REASON'. If testutil.SkipIfNoPostgres words it differently now, or scripts/mysql-integration-shard.sh marks the package differently, update this script"
  while IFS= read -r name; do
    total=$((total + 1))
    if ! grep -qE -- "$TESTS" <<<"$name"; then
      echo "pg-console-tests: $PKG $name skipped here for want of a PostgreSQL, and its name does not match '$TESTS': no job runs it. Rename it (TestIntegrationPG...), or widen TESTS in scripts/pg-console-tests.sh" >&2
      bad=1
    fi
  done <<<"$skipped"
  [ "$bad" = 0 ] || die "check-mysql-log: a $PKG test that needs a PostgreSQL runs in no job"
  echo "pg-console-tests: each of the $total $PKG tests that skipped here for want of a PostgreSQL has a name the PostgreSQL job selects ('$TESTS')"
}

run_tests() {
  local log="$1" bindir dir rc guard=0
  [ -d "$PKG" ] || die "run: no ./$PKG here; run from the repository root"
  bindir="$(mktemp -d)"
  dir="$PWD/$PKG"
  go test -tags=integration -race -c -o "$bindir/$PKG.test" "./$PKG/" \
    || die "building the test binary of $PKG failed"
  (cd "$dir" && "$bindir/$PKG.test" -test.list '.*' </dev/null) >"$bindir/listing" \
    || die "listing the tests of $PKG failed"
  echo "pg-console-tests: running $(grep -E '^Test' "$bindir/listing" | grep -cE -- "$TESTS" || true) of the $(grep -cE '^Test' "$bindir/listing" || true) tests of $PKG (-test.run '$TESTS')"
  set +e
  # The flags `go test -v -count=1 -timeout 45m` passes. From the package
  # directory, as `go test` runs it.
  (cd "$dir" && "$bindir/$PKG.test" -test.paniconexit0 -test.timeout=45m0s -test.count=1 -test.v=true -test.run="$TESTS" </dev/null) 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  set -e
  # On its own line whatever the binary left unfinished.
  if [ "$rc" = 0 ]; then printf '\nok  \t%s\n' "$PKG"; else printf '\nFAIL\t%s\t[exit %s]\n' "$PKG" "$rc"; fi
  (check "$bindir/listing" "$log") || guard=1
  rm -rf "$bindir"
  [ "$rc" = 0 ] && [ "$guard" = 0 ]
}

# expect <pass|fail> <what> <text the output must hold, or ""> <command...>
self_test_failures=0
expect() {
  local want="$1" what="$2" needle="$3" out got=pass
  shift 3
  out="$( ("$@") 2>&1)" || got=fail
  if [ "$got" != "$want" ]; then
    echo "self-test FAILED: $what: want $want, got $got" >&2
    printf '%s\n' "$out" | sed 's/^/    /' >&2
    self_test_failures=$((self_test_failures + 1))
  elif [ -n "$needle" ] && ! grep -qF -- "$needle" <<<"$out"; then
    echo "self-test FAILED: $what: the output does not say '$needle'" >&2
    printf '%s\n' "$out" | sed 's/^/    /' >&2
    self_test_failures=$((self_test_failures + 1))
  else
    echo "self-test ok: $what"
  fi
}

self_test() {
  local d
  d="$(mktemp -d)"
  ALLOWED_SKIPS=()

  # A listing as -test.list prints it: tests the rule selects, tests it does
  # not, and a line that is not a test.
  cat >"$d/listing" <<'EOF'
TestIntegrationPGRouter
TestIntegrationPGRouterForwardTLS
TestPGRouterTLS_realClients
TestIntegrationServerAddPostgresSetsTheForwardingUser
TestIntegrationFlashback
ExampleSomething
EOF
  cat >"$d/all-ran" <<'EOF'
=== RUN   TestIntegrationPGRouter
=== RUN   TestIntegrationPGRouter/psql
    pgrouter_test.go:1: psql is not installed here
    --- SKIP: TestIntegrationPGRouter/psql (0.00s)
--- PASS: TestIntegrationPGRouter (1.20s)
=== RUN   TestIntegrationPGRouterForwardTLS
--- PASS: TestIntegrationPGRouterForwardTLS (0.50s)
=== RUN   TestPGRouterTLS_realClients
--- FAIL: TestPGRouterTLS_realClients (0.10s)
=== RUN   TestIntegrationServerAddPostgresSetsTheForwardingUser
a line a test left unfinished--- PASS: TestIntegrationServerAddPostgresSetsTheForwardingUser (0.10s)
FAIL
EOF
  expect pass "every selected test has a result (a failure and a skipped subtest count as run)" \
    "each of the 4 consoleapp tests" check "$d/listing" "$d/all-ran"

  grep -v 'TestPGRouterTLS_realClients' "$d/all-ran" >"$d/missing"
  expect fail "a listed test with no result line" \
    "consoleapp TestPGRouterTLS_realClients did not run" check "$d/listing" "$d/missing"

  # Only its subtest has a result: the test itself never finished.
  grep -v -- '^--- PASS: TestIntegrationPGRouter (' "$d/all-ran" >"$d/subtest-only"
  expect fail "a test with a result for its subtest alone" \
    "consoleapp TestIntegrationPGRouter did not run" check "$d/listing" "$d/subtest-only"

  sed 's/^--- PASS: TestIntegrationPGRouterForwardTLS (0.50s)/--- SKIP: TestIntegrationPGRouterForwardTLS (0.00s)/' "$d/all-ran" >"$d/skipped"
  expect fail "a selected test that skipped" \
    "consoleapp TestIntegrationPGRouterForwardTLS SKIPPED" check "$d/listing" "$d/skipped"

  ALLOWED_SKIPS=('TestIntegrationPGRouterForwardTLS: no PostgreSQL with TLS here')
  expect pass "a skip that ALLOWED_SKIPS names" \
    "no PostgreSQL with TLS here" check "$d/listing" "$d/skipped"
  expect pass "an ALLOWED_SKIPS test that ran after all" "" check "$d/listing" "$d/all-ran"
  expect fail "the allow-list does not cover a test that never ran" \
    "TestPGRouterTLS_realClients did not run" check "$d/listing" "$d/missing"

  ALLOWED_SKIPS=('TestIntegrationPGGone: renamed since')
  expect fail "an ALLOWED_SKIPS entry for a test that does not exist" \
    "ALLOWED_SKIPS names TestIntegrationPGGone" check "$d/listing" "$d/all-ran"
  ALLOWED_SKIPS=('TestIntegrationPGRouter no reason')
  expect fail "an ALLOWED_SKIPS entry without a reason" "is not 'Name: why'" check "$d/listing" "$d/all-ran"
  ALLOWED_SKIPS=()

  printf 'TestIntegrationFlashback\nTestSomethingElse\n' >"$d/no-pg-listing"
  expect fail "a listing with no test the rule selects" "nothing would run" check "$d/no-pg-listing" "$d/all-ran"
  expect fail "a log that does not exist" "no test log" check "$d/listing" "$d/nowhere"

  # The MySQL shards' log: consoleapp between its marks, another package
  # outside them.
  cat >"$d/mysql" <<'EOF'
=== RUN   TestLoad_emptyTable
    target_integration_test.go:120: skipping: BINTRAIL_TEST_PG_DSN not set (no live PostgreSQL with logical replication)
--- SKIP: TestLoad_emptyTable (0.00s)
ok  	github.com/dbtrail/dbtrail/internal/pgdrill	0.1s
mysql-integration-shard: split begin -test.skip=x
=== RUN   TestIntegrationPGRouter
    pgrouter_test.go:76: skipping: BINTRAIL_TEST_PG_DSN not set (no live PostgreSQL with logical replication)
--- SKIP: TestIntegrationPGRouter (0.00s)
=== RUN   TestIntegrationServerAddPostgresSetsTheForwardingUser
    x_test.go:31: skipping: BINTRAIL_TEST_PG_DSN not set (no live PostgreSQL with logical replication)
--- SKIP: TestIntegrationServerAddPostgresSetsTheForwardingUser (0.00s)
=== RUN   TestIntegrationDumpFillsARealWorkingFolder_1938
    dump_test.go:36: BINTRAIL_TEST_SMALL_FS is not set: no small filesystem to fill
--- SKIP: TestIntegrationDumpFillsARealWorkingFolder_1938 (0.00s)
=== RUN   TestIntegrationFlashback
--- PASS: TestIntegrationFlashback (2.00s)

mysql-integration-shard: split end
EOF
  expect pass "every consoleapp test without a PostgreSQL has a name the rule selects" \
    "each of the 2 consoleapp tests" check_mysql_log "$d/mysql"

  sed 's/TestIntegrationServerAddPostgresSetsTheForwardingUser/TestIntegrationServerAddSetsTheForwardingUser/' "$d/mysql" >"$d/mysql-unnamed"
  expect fail "a consoleapp test that needs a PostgreSQL under a name the rule misses" \
    "consoleapp TestIntegrationServerAddSetsTheForwardingUser skipped here for want of a PostgreSQL" check_mysql_log "$d/mysql-unnamed"

  grep -v 'BINTRAIL_TEST_PG_DSN not set' "$d/mysql" >"$d/mysql-no-reason"
  expect fail "a MySQL log where no consoleapp test names the missing PostgreSQL" \
    "no consoleapp test skipped saying" check_mysql_log "$d/mysql-no-reason"

  # The rule as Go and as grep must agree on the names above.
  expect pass "the pattern has no '/'" "" bash -c 'case "$1" in */*) exit 1 ;; esac' _ "$TESTS"

  rm -rf "$d"
  [ "$self_test_failures" = 0 ] || die "self-test: $self_test_failures cases failed"
  echo "pg-console-tests: self-test passed"
}

case "${1:-}" in
  pattern) printf '%s\n' "$TESTS" ;;
  run)
    [ -n "${2:-}" ] || die "usage: $0 run <log file>"
    run_tests "$2"
    ;;
  check)
    [ -n "${2:-}" ] && [ -n "${3:-}" ] || die "usage: $0 check <test listing> <test log>"
    check "$2" "$3"
    ;;
  check-mysql-log)
    [ -n "${2:-}" ] || die "usage: $0 check-mysql-log <log file>"
    check_mysql_log "$2"
    ;;
  --self-test) self_test ;;
  *) die "usage: $0 pattern | run <log file> | check <test listing> <test log> | check-mysql-log <log file> | --self-test" ;;
esac
