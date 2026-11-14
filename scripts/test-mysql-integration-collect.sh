#!/usr/bin/env bash
# Tests `scripts/mysql-integration-shard.sh collect` over made-up result
# folders: which attempt it reads for each shard, and what it refuses. Also
# `check-split` over made-up logs, and the split configurations the script
# refuses before any Go runs. Run from the repository root. No database and
# no Go: the workflow-lockstep job runs it on every CI run.
set -uo pipefail

script="$(cd "$(dirname "$0")" && pwd)/mysql-integration-shard.sh"
count="$(bash "$script" count)"
[ "$count" = 3 ] || { echo "these cases are written for 3 shards, SHARD_COUNT is $count: update them" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
failed=0

# result <shard> <attempt> <outcome> <s3-ready> [version]: one uploaded result.
result() {
  local d="$work/in/mysql-it-${5:-8.0}-shard-$1-attempt-$2"
  mkdir -p "$d"
  printf '%s\n' "$3" > "$d/outcome"
  printf '%s\n' "$4" > "$d/s3-ready"
  printf 'log of shard %s attempt %s\n' "$1" "$2" > "$d/mysql-it.out"
}

fresh() {
  rm -rf "$work/in" "$work/log" "$work/out"
  mkdir -p "$work/in"
}

# expect <name> <want exit> <want ready> <want log lines, "|"-separated> [text the output must hold]
expect() {
  local name="$1" want_rc="$2" want_ready="$3" want_log="$4" want_text="${5:-}" out rc ready log
  out="$(GITHUB_OUTPUT="$work/out" bash "$script" collect "$work/in" 8.0 "$work/log" 2>&1)"
  rc=$?
  ready="$(sed -n 's/^ready=//p' "$work/out" 2>/dev/null | paste -sd, -)"
  log="$(paste -sd'|' - < "$work/log" 2>/dev/null)"
  if [ "$rc" != "$want_rc" ] || [ "$ready" != "$want_ready" ] || [ "$log" != "$want_log" ] \
    || { [ -n "$want_text" ] && ! grep -qF -- "$want_text" <<<"$out"; }; then
    echo "FAIL: $name"
    echo "  exit  got $rc, want $want_rc"
    echo "  ready got '$ready', want '$want_ready'"
    echo "  log   got '$log', want '$want_log'"
    [ -z "$want_text" ] || echo "  output must hold: $want_text"
    echo "$out" | sed 's/^/  | /'
    failed=1
  else
    echo "ok: $name"
  fi
}

all="log of shard 1 attempt 1|log of shard 2 attempt 1|log of shard 3 attempt 1"

fresh; result 1 1 success true; result 2 1 success true; result 3 1 success true
expect "every shard passed in attempt 1" 0 true "$all"

# The case of #2299: the rerun passed, and the older result must not be read.
fresh; result 1 1 success true; result 2 1 success true
result 3 1 skipped false; result 3 2 success true
expect "a shard skipped in attempt 1 and passed in attempt 2" 0 true \
  "log of shard 1 attempt 1|log of shard 2 attempt 1|log of shard 3 attempt 2" "shard 3 of 3: reading attempt 2"

fresh; result 1 1 failure true; result 1 2 success true
result 2 1 success true; result 3 1 success true
expect "only shard 1 ran again; the others keep attempt 1" 0 true \
  "log of shard 1 attempt 2|log of shard 2 attempt 1|log of shard 3 attempt 1"

fresh; result 1 1 success true; result 2 1 success true; result 3 1 skipped false
expect "a shard whose only result is skipped" 1 false "$all" "shard 3 of 3: tests ended 'skipped' (attempt 1)"

# The newest attempt decides, also when it is the worse one.
fresh; result 1 1 success true; result 2 1 success true
result 3 1 success true; result 3 2 failure true
expect "a shard that passed, then failed in a later attempt" 1 true \
  "log of shard 1 attempt 1|log of shard 2 attempt 1|log of shard 3 attempt 2" "tests ended 'failure' (attempt 2)"

fresh; result 1 1 success true; result 2 1 success true
result 3 9 skipped false; result 3 10 success true
expect "attempt 10 is newer than attempt 9" 0 true \
  "log of shard 1 attempt 1|log of shard 2 attempt 1|log of shard 3 attempt 10"

fresh; result 1 1 success true; result 2 1 success true
result 3 1 success false; result 3 2 success true
expect "the S3 store state comes from the attempt read (down, then up)" 0 true \
  "log of shard 1 attempt 1|log of shard 2 attempt 1|log of shard 3 attempt 2"

fresh; result 1 1 success true; result 2 1 success true
result 3 1 success true; result 3 2 success false
expect "the S3 store state comes from the attempt read (up, then down)" 0 false \
  "log of shard 1 attempt 1|log of shard 2 attempt 1|log of shard 3 attempt 2"

fresh; result 1 1 success true; result 3 1 success true
expect "a shard with no result" 1 false \
  "log of shard 1 attempt 1|log of shard 3 attempt 1" "shard 2 of 3 left no result"

fresh
expect "no result at all" 1 false "" "shard 1 of 3 left no result"

# download-artifact extracts a single matching artifact without its folder.
fresh; printf 'success\n' > "$work/in/outcome"; printf 'true\n' > "$work/in/s3-ready"
expect "one artifact in all, extracted flat" 1 "" "" "only one of 3 shards reported a result"

fresh; result 1 1 success true; result 2 1 success true; result 3 1 success true; result 4 1 success true
expect "a result for a shard past SHARD_COUNT" 1 true "$all" "found a result for shard 4"

fresh; result 1 1 success true; result 2 1 success true; result 3 1 success true
mkdir "$work/in/mysql-it-8.0-shard-3"; printf 'skipped\n' > "$work/in/mysql-it-8.0-shard-3/outcome"
expect "a folder named without an attempt" 1 true "$all" "unexpected result folder 'mysql-it-8.0-shard-3'"

fresh; result 1 1 success true; result 2 1 success true; result 3 1 success true
result 3 2 success true 8.4
expect "a result of another MySQL version" 1 true "$all" "unexpected result folder 'mysql-it-8.4-shard-3-attempt-2'"

fresh; result 1 1 success true; result 2 1 success true; result 3 1 success true
rm "$work/in/mysql-it-8.0-shard-2-attempt-1/outcome"
expect "a result without its outcome file" 1 false \
  "log of shard 1 attempt 1|log of shard 3 attempt 1" "shard 2 of 3: the result of attempt 1 has no outcome file"

fresh; result 1 1 success true; result 2 1 success true; result 3 1 success true
rm "$work/in/mysql-it-8.0-shard-2-attempt-1/s3-ready"
expect "a result without its S3 store state" 0 false "$all"

fresh; result 1 1 success true; result 2 1 success true; result 3 1 success true
rm "$work/in/mysql-it-8.0-shard-2-attempt-1/mysql-it.out"
expect "a result without its test log" 1 true \
  "log of shard 1 attempt 1|log of shard 3 attempt 1" "shard 2 of 3: the result of attempt 1 has no test log"

# check-split over made-up logs: every listed test of the split package must
# have started exactly once, between a part's begin and end marks.
mark="mysql-integration-shard: split"

# split_log <file> <lines...>: a log, one argument per line.
split_log() {
  local f="$1"
  shift
  printf '%s\n' "$@" > "$f"
}

# expect_split <name> <want exit> <text the output must hold>
expect_split() {
  local name="$1" want_rc="$2" want_text="$3" out rc
  out="$(bash "$script" check-split "$work/split.log" 2>&1)"
  rc=$?
  if [ "$rc" != "$want_rc" ] || ! grep -qF -- "$want_text" <<<"$out"; then
    echo "FAIL: $name"
    echo "  exit got $rc, want $want_rc; output must hold: $want_text"
    echo "$out" | sed 's/^/  | /'
    failed=1
  else
    echo "ok: $name"
  fi
}

list3=("$mark list TestA" "$mark list TestB" "$mark list TestC")

split_log "$work/split.log" "${list3[@]}" \
  "$mark begin -test.run=^TestA" "=== RUN   TestA" "$mark end" \
  "${list3[@]}" "$mark begin -test.run=^TestB" "=== RUN   TestB" "$mark end" \
  "${list3[@]}" "$mark begin -test.skip=(?:^TestA)|(?:^TestB)" "=== RUN   TestC" "$mark end"
expect_split "three parts, each test once" 0 "each of the 3 tests of consoleapp ran exactly once"

split_log "$work/split.log" "${list3[@]}" \
  "$mark begin -test.run=^TestA" "=== RUN   TestA" "=== RUN   TestB" "$mark end" \
  "$mark begin -test.run=^TestB" "=== RUN   TestB" "$mark end" \
  "$mark begin -test.skip=x" "=== RUN   TestC" "$mark end"
expect_split "a test two parts ran" 1 "consoleapp TestB ran 2 times across the shards, want 1"

split_log "$work/split.log" "${list3[@]}" \
  "$mark begin -test.run=^TestA" "=== RUN   TestA" "$mark end" \
  "$mark begin -test.skip=x" "=== RUN   TestC" "$mark end"
expect_split "a test no part ran" 1 "consoleapp TestB ran 0 times across the shards, want 1"

split_log "$work/split.log" "${list3[@]}" \
  "$mark begin -test.run=^TestA" "=== RUN   TestA" "=== RUN   TestC" "$mark end" \
  "=== RUN   TestB"
expect_split "a run line outside every part does not count" 1 "consoleapp TestB ran 0 times"

split_log "$work/split.log" "${list3[@]}" \
  "$mark begin -test.run=^TestA" "=== RUN   TestA" "=== RUN   TestA/sub" "=== RUN   TestB" "$mark end" \
  "$mark begin -test.skip=x" "unfinished output=== RUN   TestC" "$mark end"
expect_split "subtests do not count; a name after unfinished output does" 0 "each of the 3 tests"

split_log "$work/split.log" "$mark begin -test.run=^TestA" "=== RUN   TestA" "$mark end"
expect_split "a log no shard listed the tests in" 1 "no shard listed the tests of consoleapp"

# The split configuration is refused before any Go runs. Each case runs a
# copy of the script whose SPLIT_PARTS (and SPLIT_REST_SHARD) are replaced.
# variant <rest shard> <parts...>: writes that copy to $work/variant.sh.
variant() {
  local rest="$1" entry body=""
  shift
  for entry in "$@"; do body="$body  '$entry'"$'\n'; done
  awk -v body="$body" -v rest="$rest" '
    /^SPLIT_PARTS=\($/ { print; printf "%s", body; skip = 1; next }
    skip && /^\)$/ { skip = 0 }
    skip { next }
    /^SPLIT_REST_SHARD=/ { print "SPLIT_REST_SHARD=" rest; next }
    { print }
  ' "$script" > "$work/variant.sh"
}

# expect_refused <name> <text the refusal must hold>
expect_refused() {
  local name="$1" want_text="$2" out rc
  out="$(bash "$work/variant.sh" list 1 2>&1)"
  rc=$?
  if [ "$rc" = 0 ] || ! grep -qF -- "$want_text" <<<"$out"; then
    echo "FAIL: $name"
    echo "  exit got $rc, want non-zero; output must hold: $want_text"
    echo "$out" | sed 's/^/  | /'
    failed=1
  else
    echo "ok: $name"
  fi
}

variant 3
expect_refused "no part at all" "SPLIT_PARTS is empty"
variant 3 '1:'
expect_refused "an empty pattern" "has an empty pattern, which matches every test"
variant 3 '1:^TestA/sub'
expect_refused "a pattern with a slash" "must not contain '/'"
variant 3 '^TestA'
expect_refused "an entry without a shard" "is not <shard>:<pattern>"
variant 3 'x:^TestA'
expect_refused "a shard that is not a number" "the shard must be a number"
variant 3 '01:^TestA'
expect_refused "a shard with a leading zero" "write the shard without a leading zero"
variant 03 '1:^TestA'
expect_refused "a rest shard with a leading zero" "write the shard without a leading zero"
variant 3 '4:^TestA'
expect_refused "a part past SHARD_COUNT" "shard 4 is not from 1 to 3"
variant 0 '1:^TestA'
expect_refused "a rest shard of 0" "shard 0 is not from 1 to 3"
variant 3 '1:^TestA' '1:^TestB'
expect_refused "two parts in one shard" "shard 1 runs two parts of consoleapp"
variant 3 '1:^TestA' '3:^TestB'
expect_refused "a part in the rest shard" "shard 3 runs two parts of consoleapp"
variant 3 '1:^TestA'
sed -i.bak 's|^    2) echo "|    2) echo "consoleapp |' "$work/variant.sh"
expect_refused "the split package named in shard_list" "shard 2 names the split package consoleapp"

if [ "$failed" != 0 ]; then
  echo "test-mysql-integration-collect: some cases failed" >&2
  exit 1
fi
echo "test-mysql-integration-collect: all cases passed"
