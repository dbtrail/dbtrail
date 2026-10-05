#!/usr/bin/env bash
# Runs one shard of the MySQL integration tests (the `integration-shard` job in
# .github/workflows/ci.yml). Run from the repository root.
#
#   scripts/mysql-integration-shard.sh count            -> number of shards
#   scripts/mysql-integration-shard.sh list <shard>     -> that shard's packages
#   scripts/mysql-integration-shard.sh run <shard> <log> -> build and run them
#   scripts/mysql-integration-shard.sh check-split <log> -> over every shard's
#       log together: each test of the split package ran exactly once
#
# Which packages: only those with integration-tagged test files, found by
# asking `go list` which packages see more test files under -tags integration.
# A package without any runs in the `unit` job (also with -race) and nowhere
# here: running it again under the integration tag only cost time.
#
# Which shard: shards 1..N-1 name their packages below, balanced on measured
# run times. The LAST shard has no list: it takes every integration package
# the others do not name, so a new package always runs somewhere. A named
# package that has no integration tests any more (renamed, moved, tests
# deleted) stops the run instead of quietly running nothing.
#
# One package runs in TWO shards, split by test name: see SPLIT_PKG below.
#
# How: every test binary of the shard is compiled first, in parallel
# (`go test -c`), then the binaries run ONE AT A TIME from their package
# directory, as `go test -p 1` would. They must not overlap: the packages
# share one MySQL server and its binlog, and the binlog tests count exact
# events. Each package ends with the same `ok`/`FAIL` line `go test` prints.
set -euo pipefail
# A failing `go list` inside $(...) must stop the script too, not shorten a
# list. bash 4.4+ (the CI runners); macOS's bash 3.2 lacks the option and
# runs without it, which only matters for a local `list`.
shopt -s inherit_errexit 2>/dev/null || true

SHARD_COUNT=3

# Balanced on the integration (8.0) run of 2026-10-02 (seconds of tests plus
# build, serial): about 360 / 370 / 400. Keep the catch-all shard the one
# with many small packages. consoleapp is named in one shard and runs in two
# (SPLIT_PKG below).
shard_list() {
  case "$1" in
    1) echo "internal/console internal/cli internal/cascade internal/doctor internal/metadata test/e2e" ;;
    2) echo "consoleapp internal/streamrun cmd/bintrail-mcp internal/icebergexport internal/verify internal/shim" ;;
    *) echo "" ;;
  esac
}

# consoleapp alone reached 550 s of the 600 s a package gets (#2132), most of
# it tests that start one worker process per SQL statement. It runs in two
# parts: the shard that names it in shard_list runs the tests whose name
# matches SPLIT_RUN (-test.run), and shard SPLIT_REST_SHARD runs every other
# test of the package (-test.skip with the same pattern). Each part is its own
# run of the binary, with its own 10 minutes. Keep each part under about 300 s.
#
# One pattern, used both ways, cannot leave a test out or run it twice as long
# as it has no "/" (with one, -test.run and -test.skip stop being opposites).
# Two checks do not take that on trust:
#   - before running, each part must hold at least one test (split_guard), so
#     a pattern that stopped matching anything does not put the whole package
#     back in one run;
#   - after every shard ran, the `integration` job reads their logs together
#     and fails unless each test the binary lists ran exactly once
#     (check-split).
SPLIT_PKG="consoleapp"
SPLIT_RUN='^TestIntegration(SQLCompare|Flashback)'
SPLIT_REST_SHARD=3

# What check-split reads in the logs.
SPLIT_MARK="mysql-integration-shard: split"

now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }

die() {
  echo "mysql-integration-shard: $*" >&2
  exit 1
}

check_shard() {
  case "$1" in
    '' | *[!0-9]*) die "shard must be a number from 1 to $SHARD_COUNT, got '$1'" ;;
  esac
  [ "$1" -ge 1 ] && [ "$1" -le "$SHARD_COUNT" ] \
    || die "shard must be from 1 to $SHARD_COUNT, got $1 (the ci.yml matrix and SHARD_COUNT must agree)"
}

# Import paths of every package with integration-tagged test files.
integration_packages() {
  local fmt='{{.ImportPath}} {{join .TestGoFiles ","}} {{join .XTestGoFiles ","}}'
  local plain tagged
  plain="$(go list -f "$fmt" ./... | sort)"
  tagged="$(go list -tags integration -f "$fmt" ./... | sort)"
  comm -13 <(printf '%s\n' "$plain") <(printf '%s\n' "$tagged") | awk '{print $1}'
}

shard_packages() {
  local shard="$1" module all named pkg i seen=" "
  module="$(go list -m)"
  all="$(integration_packages)"
  [ -n "$all" ] || die "go list found no package with integration tests"

  named=""
  for ((i = 1; i < SHARD_COUNT; i++)); do
    for pkg in $(shard_list "$i"); do
      grep -qxF "$module/$pkg" <<<"$all" \
        || die "shard $i names $pkg, which has no integration tests; update shard_list"
      case "$seen" in *" $pkg "*) die "$pkg is named in two shards" ;; esac
      seen="$seen$pkg "
      named="$named$module/$pkg"$'\n'
    done
  done

  case "$seen" in
    *" $SPLIT_PKG "*) ;;
    *) die "the split package $SPLIT_PKG must be named in one shard of shard_list" ;;
  esac
  case " $(shard_list "$SPLIT_REST_SHARD") " in
    *" $SPLIT_PKG "*) die "shard $SPLIT_REST_SHARD both names $SPLIT_PKG and runs the rest of it; move one" ;;
  esac

  if [ "$shard" -lt "$SHARD_COUNT" ]; then
    for pkg in $(shard_list "$shard"); do echo "$module/$pkg"; done
  else
    comm -23 <(printf '%s\n' "$all" | sort) <(printf '%s' "$named" | sort)
  fi
  # The second part of the split package.
  if [ "$shard" = "$SPLIT_REST_SHARD" ]; then
    echo "$module/$SPLIT_PKG"
  fi
}

# The tests, examples and fuzz targets a test binary holds whose name matches
# a pattern, one per line. Run from the package directory, as the tests are.
list_tests() {
  local dir="$1" bin="$2" pattern="$3" out
  out="$(cd "$dir" && "$bin" -test.list "$pattern" </dev/null)" \
    || die "listing the tests of $bin failed"
  grep -E '^(Test|Example|Fuzz)' <<<"$out" || true
}

# Refuses a split that is not one: a pattern with a "/", or a part with no
# test in it.
split_guard() {
  local dir="$1" bin="$2" all part
  case "$SPLIT_RUN" in
    */*) die "SPLIT_RUN must not contain '/': with one, -test.run and -test.skip are not opposites" ;;
  esac
  all="$(list_tests "$dir" "$bin" '.*' | wc -l | tr -d ' ')"
  part="$(list_tests "$dir" "$bin" "$SPLIT_RUN" | wc -l | tr -d ' ')"
  [ "$part" -gt 0 ] \
    || die "SPLIT_RUN matches none of the $all tests of $SPLIT_PKG: the package would run whole in one shard"
  [ "$part" -lt "$all" ] \
    || die "SPLIT_RUN matches all $all tests of $SPLIT_PKG: the package would run whole in one shard"
  echo "$SPLIT_PKG: $part of $all tests match $SPLIT_RUN"
}

# check-split: over the logs of every shard, concatenated. Each test the
# binary of the split package lists must have started exactly once, inside one
# of that package's two runs.
check_split() {
  local log="$1" listed ran name n bad=0 total=0
  [ -f "$log" ] || die "check-split: no log at $log"
  listed="$(awk -v mark="$SPLIT_MARK list " 'index($0, mark) == 1 { print substr($0, length(mark) + 1) }' "$log" | sort -u)"
  [ -n "$listed" ] || die "check-split: no shard listed the tests of $SPLIT_PKG"
  # Top-level tests only (no "/"), between a part's begin and end marks. The
  # name may follow other output on its line: a test that left a line
  # unfinished does not hide the next one.
  ran="$(awk -v begin="$SPLIT_MARK begin" -v end="$SPLIT_MARK end" '
    index($0, begin) == 1 { inside = 1; next }
    index($0, end) == 1 { inside = 0; next }
    inside && match($0, /=== RUN   [^ \/]+$/) { print substr($0, RSTART + 10) }
  ' "$log" | sort | uniq -c | awk '{ print $2 " " $1 }')"
  while IFS= read -r name; do
    total=$((total + 1))
    n="$(awk -v t="$name" '$1 == t { print $2 }' <<<"$ran")"
    if [ "${n:-0}" != 1 ]; then
      echo "check-split: $SPLIT_PKG $name ran ${n:-0} times across the shards, want 1" >&2
      bad=1
    fi
  done <<<"$listed"
  [ "$bad" = 0 ] || die "check-split: the two parts of $SPLIT_PKG do not add up to the package (SPLIT_RUN, SPLIT_REST_SHARD)"
  echo "check-split: each of the $total tests of $SPLIT_PKG ran exactly once"
}

run_shard() {
  local shard="$1" log="$2" bindir t0 status=0 pkg dir bin rc start elapsed ran=0 dirs
  local list module label name
  local -a pkgs=() part=()
  # Not a process substitution: a refusal inside shard_packages must stop the
  # run, not leave it with a partial list.
  list="$(shard_packages "$shard")"
  while IFS= read -r pkg; do
    [ -n "$pkg" ] && pkgs+=("$pkg")
  done <<<"$list"
  [ "${#pkgs[@]}" -gt 0 ] || die "shard $shard has no packages"
  echo "shard $shard of $SHARD_COUNT: ${#pkgs[@]} packages"
  printf '  %s\n' "${pkgs[@]}"

  bindir="$(mktemp -d)"
  t0=$SECONDS
  go test -tags=integration -race -c -o "$bindir/" "${pkgs[@]}" \
    || die "building the test binaries failed"
  echo "built ${#pkgs[@]} test binaries in parallel in $((SECONDS - t0))s"

  dirs="$(go list -f '{{.ImportPath}} {{.Dir}}' "${pkgs[@]}")"
  module="$(go list -m)"
  : >"$log"
  while read -r pkg dir; do
    ran=$((ran + 1))
    bin="$bindir/${pkg##*/}.test"
    if [ ! -x "$bin" ]; then
      printf 'FAIL\t%s\t[no test binary at %s]\n' "$pkg" "$bin" | tee -a "$log"
      status=1
      continue
    fi
    # The split package: which half of its tests this shard runs, and what
    # check-split needs in the log (the full list goes to the log file only).
    part=()
    label=""
    if [ "$pkg" = "$module/$SPLIT_PKG" ]; then
      split_guard "$dir" "$bin"
      if [ "$shard" = "$SPLIT_REST_SHARD" ]; then
        part=("-test.skip=$SPLIT_RUN")
      else
        part=("-test.run=$SPLIT_RUN")
      fi
      label=$'\t'"[${part[0]}]"
      list_tests "$dir" "$bin" '.*' | while IFS= read -r name; do
        printf '%s list %s\n' "$SPLIT_MARK" "$name"
      done >>"$log"
      printf '%s begin %s\n' "$SPLIT_MARK" "${part[0]}" | tee -a "$log"
    fi
    start="$(now)"
    set +e
    # The flags `go test -v -count=1` passes, with its default 10m timeout.
    # stdin is /dev/null: a test that read it would otherwise swallow the
    # package list this loop reads. `timeout` is the watchdog `go test` keeps
    # past the binary's own timer, for a hang that timer cannot interrupt.
    (cd "$dir" && timeout -s QUIT 11m "$bin" -test.paniconexit0 -test.timeout=10m0s -test.count=1 -test.v=true ${part[@]+"${part[@]}"} </dev/null) 2>&1 | tee -a "$log"
    rc=${PIPESTATUS[0]}
    set -e
    if [ -n "$label" ]; then
      # On its own line whatever the binary left unfinished.
      printf '\n%s end\n' "$SPLIT_MARK" | tee -a "$log"
    fi
    elapsed="$(awk -v a="$start" -v b="$(now)" 'BEGIN { printf "%.3f", b - a }')"
    if [ "$rc" = 0 ]; then
      printf 'ok  \t%s\t%ss%s\n' "$pkg" "$elapsed" "$label" | tee -a "$log"
    else
      printf 'FAIL\t%s\t%ss%s\n' "$pkg" "$elapsed" "$label" | tee -a "$log"
      status=1
    fi
  done <<<"$dirs"
  rm -rf "$bindir"
  if [ "$ran" -ne "${#pkgs[@]}" ]; then
    printf 'FAIL\tshard %s ran %s of its %s packages\n' "$shard" "$ran" "${#pkgs[@]}" | tee -a "$log"
    status=1
  fi
  return "$status"
}

case "${1:-}" in
  count) echo "$SHARD_COUNT" ;;
  list)
    check_shard "${2:-}"
    shard_packages "$2"
    ;;
  run)
    check_shard "${2:-}"
    [ -n "${3:-}" ] || die "usage: $0 run <shard> <log file>"
    run_shard "$2" "$3"
    ;;
  check-split)
    [ -n "${2:-}" ] || die "usage: $0 check-split <log file>"
    check_split "$2"
    ;;
  *) die "usage: $0 count | list <shard> | run <shard> <log file> | check-split <log file>" ;;
esac
