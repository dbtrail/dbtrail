#!/usr/bin/env bash
# Runs one shard of the MySQL integration tests (the `integration-shard` job in
# .github/workflows/ci.yml). Run from the repository root.
#
#   scripts/mysql-integration-shard.sh count            -> number of shards
#   scripts/mysql-integration-shard.sh list <shard>     -> that shard's packages
#   scripts/mysql-integration-shard.sh run <shard> <log> -> build and run them
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
# How: every test binary of the shard is compiled first, in parallel
# (`go test -c`), then the binaries run ONE AT A TIME from their package
# directory, as `go test -p 1` would. They must not overlap: the packages
# share one MySQL server and its binlog, and the binlog tests count exact
# events. Each package ends with the same `ok`/`FAIL` line `go test` prints.
set -euo pipefail
# A failing `go list` inside $(...) must stop the script too, not shorten a list.
shopt -s inherit_errexit

SHARD_COUNT=3

# Balanced on the integration (8.0) run of 2026-10-02 (seconds of tests plus
# build, serial): about 360 / 370 / 400. Keep the catch-all shard the one
# with many small packages.
shard_list() {
  case "$1" in
    1) echo "internal/console internal/cli internal/cascade internal/doctor internal/metadata test/e2e" ;;
    2) echo "consoleapp internal/streamrun cmd/bintrail-mcp internal/icebergexport internal/verify internal/shim" ;;
    *) echo "" ;;
  esac
}

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

  if [ "$shard" -lt "$SHARD_COUNT" ]; then
    for pkg in $(shard_list "$shard"); do echo "$module/$pkg"; done
  else
    comm -23 <(printf '%s\n' "$all" | sort) <(printf '%s' "$named" | sort)
  fi
}

run_shard() {
  local shard="$1" log="$2" bindir t0 status=0 pkg dir bin rc start elapsed ran=0 dirs
  local list
  local -a pkgs=()
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
  : >"$log"
  while read -r pkg dir; do
    ran=$((ran + 1))
    bin="$bindir/${pkg##*/}.test"
    if [ ! -x "$bin" ]; then
      printf 'FAIL\t%s\t[no test binary at %s]\n' "$pkg" "$bin" | tee -a "$log"
      status=1
      continue
    fi
    start="$(now)"
    set +e
    # The flags `go test -v -count=1` passes, with its default 10m timeout.
    # stdin is /dev/null: a test that read it would otherwise swallow the
    # package list this loop reads. `timeout` is the watchdog `go test` keeps
    # past the binary's own timer, for a hang that timer cannot interrupt.
    (cd "$dir" && timeout -s QUIT 11m "$bin" -test.paniconexit0 -test.timeout=10m0s -test.count=1 -test.v=true </dev/null) 2>&1 | tee -a "$log"
    rc=${PIPESTATUS[0]}
    set -e
    elapsed="$(awk -v a="$start" -v b="$(now)" 'BEGIN { printf "%.3f", b - a }')"
    if [ "$rc" = 0 ]; then
      printf 'ok  \t%s\t%ss\n' "$pkg" "$elapsed" | tee -a "$log"
    else
      printf 'FAIL\t%s\t%ss\n' "$pkg" "$elapsed" | tee -a "$log"
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
  *) die "usage: $0 count | list <shard> | run <shard> <log file>" ;;
esac
