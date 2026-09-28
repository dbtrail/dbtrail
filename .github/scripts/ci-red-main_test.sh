#!/usr/bin/env bash
#
# Tests for ci-red-main.sh. No network and no GitHub token: a fake `gh` is put
# first on PATH. It answers reads from fixture files and records every call,
# so each case asserts on what the script would have sent to GitHub.
#
#   bash .github/scripts/ci-red-main_test.sh
#
# SCRIPT=<path> runs the same cases against another copy of the script.
#
# shellcheck disable=SC2016 # the backticks in the expected texts are markdown
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${SCRIPT:-$HERE/ci-red-main.sh}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

REPO="acme/widgets"

mkdir -p "$WORK/bin"
cat > "$WORK/bin/gh" <<'FAKE'
#!/usr/bin/env bash
# Fake gh. Understands only: gh api [--paginate] [-X METHOD] PATH [fields...]
set -euo pipefail
# One JSON list per call. Built one argument at a time: handing "$@" to jq
# would let it read "--paginate" as an option of its own.
for a in "$@"; do printf '%s' "$a" | jq -Rs .; done | jq -cs . >> "$FAKE_GH_LOG"
[ "$1" = "api" ] || { echo "fake gh: unsupported command $1" >&2; exit 64; }
shift
[ "${1:-}" = "--paginate" ] && shift
method=GET
if [ "${1:-}" = "-X" ]; then method="$2"; shift 2; fi
path="$1"
call="$method $path"
if [ -n "${FAKE_GH_FAIL_MATCH:-}" ] && [[ "$call" =~ $FAKE_GH_FAIL_MATCH ]]; then
  echo "gh: Resource not accessible by integration (HTTP 403)" >&2
  exit 1
fi
# gh exits 1 on an HTTP error and still prints a body. FAILBODY prints the
# answer a success would have carried, so only the exit code tells.
failbody=0
if [ -n "${FAKE_GH_FAILBODY_MATCH:-}" ] && [[ "$call" =~ $FAKE_GH_FAILBODY_MATCH ]]; then
  failbody=1
fi
if [ -n "${FAKE_GH_GARBAGE_MATCH:-}" ] && [[ "$call" =~ $FAKE_GH_GARBAGE_MATCH ]]; then
  echo "<html>502 Bad Gateway</html>"
  exit 0
fi
if [ -n "${FAKE_GH_EMPTY_MATCH:-}" ] && [[ "$call" =~ $FAKE_GH_EMPTY_MATCH ]]; then
  exit 0
fi
case "$call" in
  "GET repos/"*"/actions/runs/"*"/jobs"*)       cat "$FAKE_GH_DIR/jobs.json" ;;
  "GET repos/"*"/actions/runs/"*)               cat "$FAKE_GH_DIR/run.json" ;;
  "GET repos/"*"/actions/workflows/"*"/runs"*)  cat "$FAKE_GH_DIR/runs.json" ;;
  "GET repos/"*"/issues?"*)                     cat "$FAKE_GH_DIR/issues.json" ;;
  "GET repos/"*"/labels?"*)                     cat "$FAKE_GH_DIR/labels.json" ;;
  "POST repos/"*"/labels")                      echo '{"name":"created"}' ;;
  "POST repos/"*"/issues")                      echo '{"number":501}' ;;
  "POST repos/"*"/issues/"*"/comments")         echo '{"id":9001}' ;;
  "PATCH repos/"*"/issues/"*)                   echo '{"number":1,"state":"closed"}' ;;
  *) echo "fake gh: no route for: $call" >&2; exit 64 ;;
esac
if [ "$failbody" -eq 1 ]; then
  echo "gh: Server Error (HTTP 500)" >&2
  exit 1
fi
FAKE
chmod +x "$WORK/bin/gh"

pass=0
fail=0
case_name=""
case_failed=0

# ---- fixtures -------------------------------------------------------------

SHA="0123456789abcdef0123456789abcdef01234567"

# run <event> <branch> <conclusion> [attempt] [status] [path]
fx_run() {
  jq -n --arg e "$1" --arg b "$2" --arg c "$3" --argjson a "${4:-1}" \
        --arg s "${5:-completed}" --arg p "${6:-.github/workflows/ci.yml}" --arg sha "$SHA" '
    {id: 1000, run_number: 50, run_attempt: $a, event: $e, head_branch: $b,
     status: $s, conclusion: (if $c == "" then null else $c end), path: $p,
     head_sha: $sha, created_at: "2026-09-24T13:01:46Z",
     updated_at: "2026-09-24T13:10:45Z",
     display_title: "$(touch /tmp/pwned) `id` @someone"}' > "$FAKE_GH_DIR/run.json"
}

# jobs: pairs of name=conclusion, one per argument
fx_jobs() {
  local a
  {
    for a in "$@"; do
      jq -n --arg n "${a%%=*}" --arg c "${a##*=}" '
        {name: $n, conclusion: $c,
         steps: [{name: "Set up job", conclusion: "success"},
                 {name: "Start MinIO (bintrail-test-minio)",
                  conclusion: (if $c == "failure" then "failure" else $c end)}]}'
    done
  } | jq -s '{total_count: length, jobs: .}' > "$FAKE_GH_DIR/jobs.json"
}

# runs: triples number:event:conclusion, newest first, all on main
fx_runs() {
  local a
  {
    for a in "$@"; do
      IFS=: read -r n e c <<<"$a"
      jq -n --argjson n "$n" --arg e "$e" --arg c "$c" '
        {id: ($n + 950), run_number: $n, event: $e, head_branch: "main",
         status: "completed", conclusion: $c,
         created_at: ("2026-09-2" + (($n % 10)|tostring) + "T08:00:00Z")}'
    done
  } | jq -s '{total_count: length, workflow_runs: .}' > "$FAKE_GH_DIR/runs.json"
}

# issues: numbers of the open issues carrying the label, in the order given
fx_issues() {
  local a
  { for a in "$@"; do jq -n --argjson n "$a" '{number: $n, state: "open"}'; done; } \
    | jq -s '.' > "$FAKE_GH_DIR/issues.json"
}

fx_labels() {
  local a
  { for a in "$@"; do jq -n --arg n "$a" '{name: $n}'; done; } \
    | jq -s '.' > "$FAKE_GH_DIR/labels.json"
}

# ---- harness --------------------------------------------------------------

begin() {
  case_name="$1"
  case_failed=0
  FAKE_GH_DIR="$WORK/case"
  rm -rf "$FAKE_GH_DIR"; mkdir -p "$FAKE_GH_DIR"
  FAKE_GH_LOG="$FAKE_GH_DIR/calls.log"
  : > "$FAKE_GH_LOG"
  export FAKE_GH_DIR FAKE_GH_LOG
  unset FAKE_GH_FAIL_MATCH FAKE_GH_GARBAGE_MATCH FAKE_GH_EMPTY_MATCH FAKE_GH_FAILBODY_MATCH
  # A sane default world: a failed push on main, nothing open, label present.
  fx_run push main failure
  fx_jobs "unit=success" "integration (8.0)=failure" "integration (8.4)=failure" "signing=skipped"
  fx_runs "49:push:success"
  fx_issues
  fx_labels bug ci-red-main
  ENV_REPO="$REPO"; ENV_RUN_ID="1000"; ENV_LABEL="ci-red-main"
  ENV_EXTRA=()
}

run_script() {
  set +e
  env -i PATH="$WORK/bin:$PATH" HOME="$WORK" \
    FAKE_GH_DIR="$FAKE_GH_DIR" FAKE_GH_LOG="$FAKE_GH_LOG" \
    FAKE_GH_FAIL_MATCH="${FAKE_GH_FAIL_MATCH:-}" \
    FAKE_GH_GARBAGE_MATCH="${FAKE_GH_GARBAGE_MATCH:-}" \
    FAKE_GH_EMPTY_MATCH="${FAKE_GH_EMPTY_MATCH:-}" \
    FAKE_GH_FAILBODY_MATCH="${FAKE_GH_FAILBODY_MATCH:-}" \
    GITHUB_STEP_SUMMARY="$FAKE_GH_DIR/summary.md" \
    REPO="$ENV_REPO" RUN_ID="$ENV_RUN_ID" LABEL="$ENV_LABEL" \
    ${ENV_EXTRA[@]+"${ENV_EXTRA[@]}"} \
    bash "$SCRIPT" > "$FAKE_GH_DIR/stdout" 2> "$FAKE_GH_DIR/stderr" < /dev/null
  RC=$?
  set -e
}

bad() { case_failed=1; echo "    FAIL: $*"; }

want_rc() { [ "$RC" -eq "$1" ] || bad "exit code $RC, want $1 (stderr: $(tr '\n' ' ' < "$FAKE_GH_DIR/stderr"))"; }
want_fail() { [ "$RC" -ne 0 ] || bad "exit code 0, want a failure"; }
want_result() { grep -qx "result: $1 .*" "$FAKE_GH_DIR/stdout" || bad "want 'result: $1', got: $(grep '^result:' "$FAKE_GH_DIR/stdout" || echo none)"; }
want_stderr() { grep -q -- "$1" "$FAKE_GH_DIR/stderr" || bad "stderr lacks '$1': $(tr '\n' ' ' < "$FAKE_GH_DIR/stderr")"; }

# writes: every recorded call that changes something, as "METHOD path"
writes() { jq -r 'select(.[1] == "-X") | "\(.[2]) \(.[3])"' "$FAKE_GH_LOG"; }
want_writes() {
  local got want
  got="$(writes)"
  want="$(printf '%s\n' "$@")"
  [ "$#" -eq 0 ] && want=""
  [ "$got" = "$want" ] || bad "writes were:$(printf '\n%s' "$got" | sed 's/^/      /')
    want:$(printf '\n%s' "$want" | sed 's/^/      /')"
}
# field <METHOD path> <name>: the value sent as -f name=value on that call
field() {
  jq -r --arg m "${1%% *}" --arg p "${1#* }" --arg k "$2=" '
    select(.[1] == "-X" and .[2] == $m and .[3] == $p)
    | . as $a | range(0; length) | select($a[.] == "-f")
    | $a[. + 1] | select(startswith($k)) | ltrimstr($k)' "$FAKE_GH_LOG"
}
want_in() { # want_in <text> <needle> <what>
  case "$1" in *"$2"*) ;; *) bad "$3 lacks '$2'. It was:
$1" ;; esac
}
want_not_in() {
  case "$1" in *"$2"*) bad "$3 must not contain '$2'. It was:
$1" ;; *) ;; esac
}

end() {
  if [ "$case_failed" -eq 0 ]; then pass=$((pass + 1)); echo "ok    $case_name"
  else fail=$((fail + 1)); echo "FAIL  $case_name"; fi
}

# ---- the cases ------------------------------------------------------------

begin "first failure opens one issue"
run_script
want_rc 0; want_result opened
want_writes "POST repos/$REPO/issues"
title="$(field "POST repos/$REPO/issues" title)"
body="$(field "POST repos/$REPO/issues" body)"
want_in "$title" "main is red" title
want_in "$title" "2026-09-24 13:01 UTC" title
want_in "$(field "POST repos/$REPO/issues" 'labels[]')" "ci-red-main" labels
want_in "$body" "https://github.com/$REPO/actions/runs/1000" body
want_in "$body" "$SHA" body
want_in "$body" '`integration (8.0)`' body
want_in "$body" '`integration (8.4)`' body
want_in "$body" 'Start MinIO (bintrail-test-minio)' body
want_in "$body" 'Red since: 2026-09-24 13:01 UTC' body
want_in "$body" '`push`' body
want_not_in "$body" '`unit`' body
want_not_in "$body" '`signing`' body
want_not_in "$body" 'pwned' body
want_not_in "$body" '@someone' body
want_not_in "$title$body" '—' "title and body"
end

begin "first failure, label missing: creates the label, then the issue"
fx_labels bug enhancement
run_script
want_rc 0; want_result opened
want_writes "POST repos/$REPO/labels" "POST repos/$REPO/issues"
want_in "$(field "POST repos/$REPO/labels" name)" "ci-red-main" "label name"
end

begin "label exists with another casing: not created again"
fx_labels CI-Red-Main
run_script
want_rc 0; want_result opened
want_writes "POST repos/$REPO/issues"
end

begin "a label that only shares the prefix does not count as present"
fx_labels ci-red-main-selftest ci-red
run_script
want_rc 0
want_writes "POST repos/$REPO/labels" "POST repos/$REPO/issues"
end

begin "second failure in a row comments, opens nothing"
fx_issues 77
fx_runs "49:push:failure" "48:schedule:failure" "47:push:success" "46:push:failure"
run_script
want_rc 0; want_result commented
want_writes "POST repos/$REPO/issues/77/comments"
body="$(field "POST repos/$REPO/issues/77/comments" body)"
want_in "$body" "https://github.com/$REPO/actions/runs/1000" comment
want_in "$body" '`integration (8.0)`' comment
want_in "$body" "Failed runs in a row: 3" comment
want_in "$body" "$SHA" comment
end

begin "streak start is the oldest failure after the last green"
fx_runs "49:push:failure" "48:schedule:failure" "47:push:success" "46:push:failure"
run_script
want_rc 0; want_result opened
body="$(field "POST repos/$REPO/issues" body)"
want_in "$body" "Red since: 2026-09-28 08:00 UTC" body
want_in "$body" "https://github.com/$REPO/actions/runs/998" body
want_in "$(field "POST repos/$REPO/issues" title)" "2026-09-28 08:00 UTC" title
end

begin "cancelled and skipped runs inside a streak neither break nor extend it"
fx_runs "49:push:cancelled" "48:push:failure" "47:push:skipped" "46:push:failure" "45:push:success"
run_script
want_rc 0
body="$(field "POST repos/$REPO/issues" body)"
want_in "$body" "Red since: 2026-09-26 08:00 UTC" body
end

begin "pull request runs and other branches in the history are ignored"
jq -n '{total_count: 3, workflow_runs: [
  {id: 1, run_number: 49, event: "pull_request", head_branch: "main", status: "completed", conclusion: "success", created_at: "2026-09-29T08:00:00Z"},
  {id: 2, run_number: 48, event: "push", head_branch: "release", status: "completed", conclusion: "success", created_at: "2026-09-28T08:00:00Z"},
  {id: 3, run_number: 47, event: "push", head_branch: "main", status: "completed", conclusion: "failure", created_at: "2026-09-27T08:00:00Z"}]}' > "$FAKE_GH_DIR/runs.json"
run_script
want_rc 0
want_in "$(field "POST repos/$REPO/issues" body)" "Red since: 2026-09-27 08:00 UTC" body
end

begin "back to green with the issue open: comments and closes"
fx_run push main success
fx_jobs "unit=success" "integration (8.0)=success" "signing=skipped"
fx_issues 77
run_script
want_rc 0; want_result closed
want_writes "POST repos/$REPO/issues/77/comments" "PATCH repos/$REPO/issues/77"
want_in "$(field "POST repos/$REPO/issues/77/comments" body)" "green" comment
want_in "$(field "POST repos/$REPO/issues/77/comments" body)" "https://github.com/$REPO/actions/runs/1000" comment
want_in "$(field "PATCH repos/$REPO/issues/77" state)" "closed" state
end

begin "green on a re-run says it was a re-run"
fx_run push main success 2
fx_jobs "unit=success"
fx_issues 77
run_script
want_rc 0; want_result closed
body="$(field "POST repos/$REPO/issues/77/comments" body)"
want_in "$body" "attempt 2" comment
want_in "$body" "re-run" comment
want_in "$body" "actions/runs/1000/attempts/2" comment
end

begin "green with nothing open: no writes"
fx_run push main success
fx_jobs "unit=success"
run_script
want_rc 0; want_result none
want_writes
end

begin "green with two issues open: closes both"
fx_run schedule main success
fx_jobs "unit=success"
fx_issues 80 77
run_script
want_rc 0; want_result closed
want_writes "POST repos/$REPO/issues/77/comments" "PATCH repos/$REPO/issues/77" \
            "POST repos/$REPO/issues/80/comments" "PATCH repos/$REPO/issues/80"
end

begin "cancelled run: nothing, even with an issue open"
fx_run push main cancelled
fx_issues 77
run_script
want_rc 0; want_result none
want_writes
end

begin "run whose conclusion is skipped: nothing"
fx_run push main skipped
fx_issues 77
run_script
want_rc 0; want_result none
want_writes
end

begin "green run where every job was skipped: nothing, the issue stays open"
fx_run push main success
fx_jobs "unit=skipped" "console-e2e=skipped"
fx_issues 77
run_script
want_rc 0; want_result none
want_writes
end

begin "green run with no jobs at all: nothing"
fx_run push main success
fx_jobs
fx_issues 77
run_script
want_rc 0; want_result none
want_writes
end

begin "failure on a pull request: nothing"
fx_run pull_request feature/x failure
run_script
want_rc 0; want_result none
want_writes
end

begin "failure on a pull request from a fork branch named main: nothing"
fx_run pull_request main failure
run_script
want_rc 0; want_result none
want_writes
end

begin "failed push to another branch: nothing"
fx_run push release/1.0 failure
run_script
want_rc 0; want_result none
want_writes
end

begin "branch names compare exactly: Main and main/ are not main"
fx_run push Main failure
run_script
want_result none; want_writes
fx_run push "main/" failure
: > "$FAKE_GH_LOG"
run_script
want_result none; want_writes
fx_run push " main" failure
: > "$FAKE_GH_LOG"
run_script
want_result none; want_writes
end

begin "manual run on main: nothing"
fx_run workflow_dispatch main failure
run_script
want_rc 0; want_result none
want_writes
end

begin "timed out run counts as red"
fx_run schedule main timed_out
fx_jobs "unit=success" "integration (8.4)=timed_out"
run_script
want_rc 0; want_result opened
body="$(field "POST repos/$REPO/issues" body)"
want_in "$body" '`integration (8.4)`' body
want_in "$body" '`schedule`' body
end

begin "red run where no job failed says so instead of an empty list"
fx_run push main startup_failure
fx_jobs
run_script
want_rc 0; want_result opened
want_in "$(field "POST repos/$REPO/issues" body)" "No job reported a failure" body
end

begin "two issues open after a race: comments on the oldest and names the other"
fx_issues 80 77
fx_runs "49:push:failure" "48:push:success"
run_script
want_rc 0; want_result commented
want_writes "POST repos/$REPO/issues/77/comments"
body="$(field "POST repos/$REPO/issues/77/comments" body)"
want_in "$body" "#80" comment
want_in "$body" "oldest" comment
end

begin "a pull request carrying the label is not the alert issue"
jq -n '[{number: 60, state: "open", pull_request: {url: "x"}}]' > "$FAKE_GH_DIR/issues.json"
run_script
want_rc 0; want_result opened
want_writes "POST repos/$REPO/issues"
end

begin "old failure that finishes after a newer green run: nothing"
fx_runs "52:push:success" "51:push:failure" "49:push:success"
run_script
want_rc 0; want_result none
want_writes
end

begin "old green that finishes after a newer failure: does not close"
fx_run push main success
fx_jobs "unit=success"
fx_issues 77
fx_runs "51:push:failure" "49:push:failure"
run_script
want_rc 0; want_result none
want_writes
end

begin "old failure that finishes after a newer failure: still comments"
fx_issues 77
fx_runs "51:push:failure" "49:push:success"
run_script
want_rc 0; want_result commented
end

begin "SKIP_SUPERSEDED_CHECK lets the self-test replay an old run"
fx_runs "52:push:success" "49:push:success"
ENV_EXTRA=(SKIP_SUPERSEDED_CHECK=1 TITLE_PREFIX="[alert self-test] ")
run_script
want_rc 0; want_result opened
want_in "$(field "POST repos/$REPO/issues" title)" "[alert self-test] main is red" title
end

begin "job names cannot break out of the code span or add lines"
jq -n '{total_count: 1, jobs: [{name: "evil` @team\n# big **x**", conclusion: "failure", steps: [{name: "s`tep\r\n@x", conclusion: "failure"}]}]}' > "$FAKE_GH_DIR/jobs.json"
run_script
want_rc 0
body="$(field "POST repos/$REPO/issues" body)"
want_in "$body" '`evil  @team # big **x**`' body
want_in "$body" '`s tep  @x`' body
end

# ---- failures must be loud ------------------------------------------------

for spec in \
  "reading the run|GET repos/.*/actions/runs/1000$" \
  "reading the jobs|GET repos/.*/jobs" \
  "reading the history|GET repos/.*/workflows/.*/runs" \
  "listing issues|GET repos/.*/issues\?" \
  "listing labels|GET repos/.*/labels\?" \
  "creating the label|POST repos/.*/labels$" \
  "creating the issue|POST repos/.*/issues$"; do
  for mode in FAIL FAILBODY GARBAGE EMPTY; do
    begin "gh breaks while ${spec%%|*} ($mode): exit is not zero"
    fx_labels bug
    export "FAKE_GH_${mode}_MATCH=${spec#*|}"
    run_script
    want_fail
    want_stderr "NOT delivered"
    [ -z "$(grep '^result:' "$FAKE_GH_DIR/stdout" || true)" ] || bad "printed a result line on failure"
    end
  done
done

for mode in FAIL FAILBODY GARBAGE EMPTY; do
  begin "gh breaks while commenting ($mode): exit is not zero"
  fx_issues 77
  export "FAKE_GH_${mode}_MATCH=POST repos/.*/comments$"
  run_script
  want_fail; want_stderr "NOT delivered"
  end

  begin "gh breaks while closing ($mode): exit is not zero"
  fx_run push main success
  fx_jobs "unit=success"
  fx_issues 77
  export "FAKE_GH_${mode}_MATCH=PATCH repos/.*/issues/77$"
  run_script
  want_fail; want_stderr "NOT delivered"
  end
done

begin "the issue was created but the answer has no number: failure"
cat > "$WORK/bin/gh.real" < "$WORK/bin/gh"
sed 's/{"number":501}/{"message":"ok"}/' "$WORK/bin/gh.real" > "$WORK/bin/gh"
run_script
cat "$WORK/bin/gh.real" > "$WORK/bin/gh"
want_fail; want_stderr "NOT delivered"
end

begin "the comment was sent but the answer has no id: failure"
fx_issues 77
cat > "$WORK/bin/gh.real" < "$WORK/bin/gh"
sed 's/{"id":9001}/{"message":"ok"}/' "$WORK/bin/gh.real" > "$WORK/bin/gh"
run_script
cat "$WORK/bin/gh.real" > "$WORK/bin/gh"
want_fail; want_stderr "carries no comment id"; want_stderr "NOT delivered"
end

begin "close answered but the issue is still open: failure"
fx_run push main success
fx_jobs "unit=success"
fx_issues 77
cat > "$WORK/bin/gh.real" < "$WORK/bin/gh"
sed 's/"state":"closed"/"state":"open"/' "$WORK/bin/gh.real" > "$WORK/bin/gh"
run_script
cat "$WORK/bin/gh.real" > "$WORK/bin/gh"
want_fail; want_stderr "NOT delivered"
end

# ---- bad input must be loud -----------------------------------------------

bad_input() { # name, then VAR=value pairs applied over the defaults
  begin "$1"; shift
  local kv
  for kv in "$@"; do
    case "$kv" in
      REPO=*) ENV_REPO="${kv#REPO=}" ;;
      RUN_ID=*) ENV_RUN_ID="${kv#RUN_ID=}" ;;
      LABEL=*) ENV_LABEL="${kv#LABEL=}" ;;
    esac
  done
  run_script
  want_fail; want_writes
  [ ! -s "$FAKE_GH_LOG" ] || bad "called gh before rejecting the input"
  end
}
bad_input "empty RUN_ID" "RUN_ID="
bad_input "RUN_ID that is not a number" "RUN_ID=12a"
bad_input "RUN_ID with a path in it" "RUN_ID=1/../2"
bad_input "RUN_ID with a space" "RUN_ID= 1000"
bad_input "RUN_ID on two lines" "RUN_ID=1000
2000"
bad_input "empty REPO" "REPO="
bad_input "REPO without an owner" "REPO=widgets"
bad_input "REPO with an extra slash" "REPO=acme/widgets/"
bad_input "REPO with a query" "REPO=acme/widgets?x=1"
bad_input "empty LABEL" "LABEL="
bad_input "LABEL with a space" "LABEL=ci red"
bad_input "LABEL with a comma" "LABEL=ci-red-main,bug"
bad_input "LABEL with an ampersand" "LABEL=a&state=all"

begin "run that is still in progress: failure, not a guess"
fx_run push main "" 1 in_progress
run_script
want_fail; want_writes; want_stderr "not completed"; want_stderr "NOT delivered"
end

begin "completed run with no conclusion: failure"
fx_run push main ""
run_script
want_fail; want_writes
end

begin "conclusion this script does not know: failure"
fx_run push main exploded
run_script
want_fail; want_writes
end

begin "run of another workflow: failure"
fx_run push main failure 1 completed .github/workflows/release.yaml
run_script
want_fail; want_writes
end

begin "commit that is not a full hash: failure"
jq '.head_sha = "main; rm -rf /"' "$FAKE_GH_DIR/run.json" > "$FAKE_GH_DIR/r2" && mv "$FAKE_GH_DIR/r2" "$FAKE_GH_DIR/run.json"
run_script
want_fail; want_writes
end

begin "issue list that is not a list: failure"
echo '{"message":"Bad credentials"}' > "$FAKE_GH_DIR/issues.json"
run_script
want_fail; want_writes; want_stderr "the answer is not a list"; want_stderr "NOT delivered"
end

begin "history without workflow_runs: failure"
echo '{"message":"Not Found"}' > "$FAKE_GH_DIR/runs.json"
run_script
want_fail; want_writes; want_stderr "has no run list"; want_stderr "NOT delivered"
end

begin "jobs answer without jobs: failure"
echo '{"message":"Not Found"}' > "$FAKE_GH_DIR/jobs.json"
run_script
want_fail; want_writes; want_stderr "has no job list"; want_stderr "NOT delivered"
end

echo
echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
