#!/usr/bin/env bash
#
# Keeps ONE issue that says "main is red" (#1882).
#
# Given a finished run of the CI workflow, it does one of four things:
#
#   opened     the run failed on main and no alert issue is open
#   commented  the run failed on main and the alert issue is already open
#   closed     the run passed on main and the alert issue is open
#   none       anything else: a pull request, another branch, a manual run,
#              a cancelled or skipped run, a run overtaken by a newer one
#
# The alert issue is found by its LABEL, never by its title.
#
# Input, all from the environment:
#
#   REPO      owner/name
#   RUN_ID    id of the finished CI run
#   LABEL     label that marks the alert issue
#   GH_TOKEN  read by gh. Needs issues: write and actions: read.
#
# Everything else about the run (event, branch, result, commit, jobs) is READ
# FROM THE API by RUN_ID. Nothing the run controls is taken from the caller,
# and none of it is ever executed: job and step names only reach the issue
# text, inside a code span, with backticks and line breaks removed.
#
# Two knobs exist for the manual self-test in ci-alert.yml and nothing else:
#
#   TITLE_PREFIX            text put in front of the issue title
#   SKIP_SUPERSEDED_CHECK   "1" replays an old run as if it were the newest
#
# Any API error, and any answer that does not look like what was asked for,
# ends the script with a non-zero exit and "the alert was NOT delivered" on
# stderr. It never reports success on a guess.
#
# Tests: bash .github/scripts/ci-red-main_test.sh
set -euo pipefail

MAIN_BRANCH="main"
CI_WORKFLOW_FILE="ci.yml"
CI_WORKFLOW_PATH=".github/workflows/$CI_WORKFLOW_FILE"
ALERT_WORKFLOW_PATH=".github/workflows/ci-alert.yml"
HISTORY_PAGE=100

REPO="${REPO:-}"
RUN_ID="${RUN_ID:-}"
LABEL="${LABEL:-}"
TITLE_PREFIX="${TITLE_PREFIX:-}"
SKIP_SUPERSEDED_CHECK="${SKIP_SUPERSEDED_CHECK:-}"
SERVER_URL="${GITHUB_SERVER_URL:-https://github.com}"

die() {
  echo "ERROR: $*" >&2
  echo "ERROR: the alert was NOT delivered." >&2
  exit 1
}

finish() { # finish <action> <reason>
  echo "result: $1 ($2)"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    echo "main alert: **$1** ($2)" >> "$GITHUB_STEP_SUMMARY"
  fi
  exit 0
}

# api <what it is doing> <gh api arguments...>
# Prints the answer. Fails on a gh error and on an answer that is not a JSON
# object or list: nothing at all, or a proxy error page with exit code 0.
api() {
  local what="$1" out
  shift
  if ! out="$(gh api "$@")"; then
    die "$what: the GitHub API call failed"
  fi
  if ! printf '%s' "$out" | jq -e 'type == "object" or type == "array"' >/dev/null 2>&1; then
    die "$what: the GitHub API answer is empty or not JSON"
  fi
  printf '%s' "$out"
}

# "2026-09-24T13:01:46Z" -> "2026-09-24 13:01 UTC"
when() {
  local ts="$1"
  [[ "$ts" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] \
    || die "unexpected timestamp in the API answer: '$ts'"
  echo "${ts:0:10} ${ts:11:5} UTC"
}

# ---- input ----------------------------------------------------------------

# [[ =~ ]] with ^ and $ matches the whole value, line breaks included, so a
# second line cannot ride along behind a valid first one.
[[ "$REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || die "REPO must be owner/name, got '$REPO'"
[[ "$RUN_ID" =~ ^[0-9]+$ ]] || die "RUN_ID must be a number, got '$RUN_ID'"
[[ "$LABEL" =~ ^[A-Za-z0-9_.-]+$ ]] || die "LABEL must be letters, digits, '.', '_' or '-', got '$LABEL'"

# ---- the run --------------------------------------------------------------

run="$(api "reading run $RUN_ID" "repos/$REPO/actions/runs/$RUN_ID")"

run_path="$(jq -r '.path // ""' <<<"$run")"
run_status="$(jq -r '.status // ""' <<<"$run")"
run_event="$(jq -r '.event // ""' <<<"$run")"
run_branch="$(jq -r '.head_branch // ""' <<<"$run")"
run_conclusion="$(jq -r '.conclusion // ""' <<<"$run")"
run_sha="$(jq -r '.head_sha // ""' <<<"$run")"
run_attempt="$(jq -r '.run_attempt // ""' <<<"$run")"
run_number="$(jq -r '.run_number // ""' <<<"$run")"
run_created="$(jq -r '.created_at // ""' <<<"$run")"
run_updated="$(jq -r '.updated_at // ""' <<<"$run")"

# The workflow path may carry "@ref" on some events. Compare the file only.
[ "${run_path%%@*}" = "$CI_WORKFLOW_PATH" ] \
  || die "run $RUN_ID belongs to '$run_path', not to $CI_WORKFLOW_PATH"
[ "$run_status" = "completed" ] \
  || die "run $RUN_ID is '$run_status', not completed"

if [ "$run_event" != "push" ] && [ "$run_event" != "schedule" ]; then
  finish none "event is '$run_event', only push and schedule alert"
fi
if [ "$run_branch" != "$MAIN_BRANCH" ]; then
  finish none "branch is not $MAIN_BRANCH"
fi

case "$run_conclusion" in
  success) colour=green ;;
  failure | timed_out | startup_failure) colour=red ;;
  cancelled | skipped | neutral) finish none "run ended as '$run_conclusion', which is not a failure" ;;
  *) die "run $RUN_ID has a conclusion this script does not know: '$run_conclusion'" ;;
esac

[[ "$run_sha" =~ ^[0-9a-f]{40}$ ]] || die "unexpected commit in the API answer: '$run_sha'"
[[ "$run_attempt" =~ ^[0-9]+$ ]] || die "unexpected attempt in the API answer: '$run_attempt'"
[[ "$run_number" =~ ^[0-9]+$ ]] || die "unexpected run number in the API answer: '$run_number'"

# Built here, not copied from the answer.
run_url="$SERVER_URL/$REPO/actions/runs/$RUN_ID"
if [ "$run_attempt" -gt 1 ]; then
  run_url="$run_url/attempts/$run_attempt"
fi
run_line="[run $RUN_ID]($run_url) (\`$run_event\`, attempt $run_attempt), finished $(when "$run_updated")"

# ---- its jobs -------------------------------------------------------------

jobs="$(api "reading the jobs of run $RUN_ID" \
  "repos/$REPO/actions/runs/$RUN_ID/jobs?filter=latest&per_page=100")"
jq -e '.jobs | type == "array"' <<<"$jobs" >/dev/null \
  || die "reading the jobs of run $RUN_ID: the answer has no job list"

passed_jobs="$(jq '[.jobs[] | select(.conclusion == "success")] | length' <<<"$jobs")"
failed_jobs="$(jq -r '
  def clean: tostring | gsub("[`\r\n]"; " ");
  .jobs[]
  | select(.conclusion == "failure" or .conclusion == "timed_out")
  | ([.steps[]? | select(.conclusion == "failure" or .conclusion == "timed_out") | .name | clean]) as $steps
  | "  - `\(.name | clean)`"
    + (if .conclusion == "timed_out" then " (timed out)" else "" end)
    + (if ($steps | length) > 0 then ": step " + ($steps | map("`\(.)`") | join(", ")) else "" end)
' <<<"$jobs")"
if [ -z "$failed_jobs" ]; then
  failed_jobs="  - No job reported a failure. The run ended as \`$run_conclusion\` outside its jobs."
fi

# A job the path filter skipped is not a pass. A green run in which nothing
# ran proves nothing about main, so it does not close the issue.
if [ "$colour" = "green" ] && [ "$passed_jobs" -eq 0 ]; then
  finish none "the run is green but no job ran"
fi

# ---- the runs around it ---------------------------------------------------

history="$(api "reading the CI history of $MAIN_BRANCH" \
  "repos/$REPO/actions/workflows/$CI_WORKFLOW_FILE/runs?branch=$MAIN_BRANCH&status=completed&per_page=$HISTORY_PAGE")"
jq -e '.workflow_runs | type == "array"' <<<"$history" >/dev/null \
  || die "reading the CI history of $MAIN_BRANCH: the answer has no run list"

# Only runs that say something about main count: push or schedule, on main,
# ended green or red. Cancelled and skipped runs are stepped over.
around="$(jq -c --argjson n "$run_number" --arg branch "$MAIN_BRANCH" --argjson page "$HISTORY_PAGE" '
  def red: . == "failure" or . == "timed_out" or . == "startup_failure";
  [ .workflow_runs[]
    | select(.head_branch == $branch)
    | select(.event == "push" or .event == "schedule")
    | select(.status == "completed")
    | select(.conclusion == "success" or (.conclusion | red)) ] as $said
  | ($said | map(select(.run_number > $n)) | sort_by(.run_number) | last) as $newer
  | ($said | map(select(.run_number < $n)) | sort_by(-.run_number)) as $older
  | ($older | map(.conclusion == "success") | index(true)) as $green_at
  | ($older[0:($green_at // ($older | length))]) as $streak
  | { newer_colour: (if $newer == null then "" elif $newer.conclusion == "success" then "green" else "red" end),
      newer_id: ($newer.id // ""),
      streak_len: (($streak | length) + 1),
      first_id: (($streak | last | .id) // ""),
      first_created: (($streak | last | .created_at) // ""),
      open_ended: ($green_at == null and ((.workflow_runs | length) >= $page)) }
' <<<"$history")" || die "reading the CI history of $MAIN_BRANCH: unexpected shape"

newer_colour="$(jq -r '.newer_colour' <<<"$around")"
newer_id="$(jq -r '.newer_id' <<<"$around")"
streak_len="$(jq -r '.streak_len' <<<"$around")"
first_id="$(jq -r '.first_id' <<<"$around")"
first_created="$(jq -r '.first_created' <<<"$around")"
open_ended="$(jq -r '.open_ended' <<<"$around")"

# Runs finish out of order. When a newer run already said the opposite, this
# one is old news: a late failure must not reopen the alarm on a green main,
# and a late pass must not close it on a red one.
if [ "$SKIP_SUPERSEDED_CHECK" != "1" ] && [ -n "$newer_colour" ] && [ "$newer_colour" != "$colour" ]; then
  finish none "overtaken by newer run $newer_id, which ended $newer_colour"
fi

if [ -z "$first_id" ]; then
  first_id="$RUN_ID"
  first_created="$run_created"
  first_url="$SERVER_URL/$REPO/actions/runs/$RUN_ID"
else
  [[ "$first_id" =~ ^[0-9]+$ ]] || die "unexpected run id in the CI history: '$first_id'"
  first_url="$SERVER_URL/$REPO/actions/runs/$first_id"
fi
red_since="$(when "$first_created")"
since_note=""
if [ "$open_ended" = "true" ]; then
  since_note=" or earlier (no green run in the last $HISTORY_PAGE)"
fi

# ---- the alert issue ------------------------------------------------------

issues="$(api "listing open issues labelled $LABEL" \
  "repos/$REPO/issues?labels=$LABEL&state=open&per_page=100")"
jq -e 'type == "array"' <<<"$issues" >/dev/null \
  || die "listing open issues labelled $LABEL: the answer is not a list"
# The issues endpoint also returns pull requests. Oldest first.
open_numbers="$(jq -r '[.[] | select(has("pull_request") | not) | .number] | sort | .[]' <<<"$issues")"
for n in $open_numbers; do
  [[ "$n" =~ ^[0-9]+$ ]] || die "unexpected issue number in the API answer: '$n'"
done
oldest="$(head -n 1 <<<"$open_numbers")"
others="$(tail -n +2 <<<"$open_numbers")"

comment_on() { # comment_on <issue> <body>
  local answer
  answer="$(api "commenting on issue #$1" -X POST "repos/$REPO/issues/$1/comments" -f "body=$2")"
  jq -e '.id | type == "number"' <<<"$answer" >/dev/null \
    || die "commenting on issue #$1: the answer carries no comment id"
}

if [ "$colour" = "green" ]; then
  if [ -z "$oldest" ]; then
    finish none "main is green and no alert issue is open"
  fi
  body="\`$MAIN_BRANCH\` is green again.

- Run: $run_line
- Commit: $run_sha"
  if [ "$run_attempt" -gt 1 ]; then
    body="$body
- This was a re-run: attempt $run_attempt of a run that had failed."
  fi
  closed=""
  for n in $open_numbers; do
    comment_on "$n" "$body"
    answer="$(api "closing issue #$n" -X PATCH "repos/$REPO/issues/$n" -f state=closed -f state_reason=completed)"
    [ "$(jq -r '.state // ""' <<<"$answer")" = "closed" ] \
      || die "closing issue #$n: the issue is not closed after the call"
    closed="$closed #$n"
  done
  finish closed "main is green, closed$closed"
fi

# Red from here on.

if [ -n "$oldest" ]; then
  body="Still red. Failed runs in a row: $streak_len.

- Run: $run_line
- Commit: $run_sha
- Failed jobs:
$failed_jobs"
  if [ -n "$others" ]; then
    list=""
    for n in $others; do list="$list #$n"; done
    body="$body

More than one open issue carries the \`$LABEL\` label:$list. This comment went to the oldest one. Close the others as duplicates."
  fi
  comment_on "$oldest" "$body"
  finish commented "main is still red, commented on #$oldest"
fi

labels="$(api "listing labels" --paginate "repos/$REPO/labels?per_page=100")"
# With --paginate the answer is one list per page. Label names do not
# distinguish case on GitHub, so neither does this.
have_label="$(jq -rs --arg l "$LABEL" '
  if all(.[]; type == "array") then
    ([.[][] | .name | ascii_downcase] | index($l | ascii_downcase)) != null
  else "bad" end' <<<"$labels")"
case "$have_label" in
  true) ;;
  false)
    answer="$(api "creating the label $LABEL" -X POST "repos/$REPO/labels" \
      -f "name=$LABEL" -f color=B60205 \
      -f "description=CI on main is failing. Opened and closed by ci-alert.yml.")"
    jq -e '.name | type == "string"' <<<"$answer" >/dev/null \
      || die "creating the label $LABEL: the answer carries no label"
    ;;
  *) die "listing labels: the answer is not a list" ;;
esac

title="${TITLE_PREFIX}main is red: CI failing since $red_since"
body="CI on \`$MAIN_BRANCH\` is failing.

- Red since: $red_since$since_note, [run $first_id]($first_url)
- Run: $run_line
- Commit: $run_sha
- Failed jobs:
$failed_jobs

Every later failed run adds a comment here. The first green run on \`$MAIN_BRANCH\` closes this issue.

Opened by \`$ALERT_WORKFLOW_PATH\`."

answer="$(api "creating the issue" -X POST "repos/$REPO/issues" \
  -f "title=$title" -f "body=$body" -f "labels[]=$LABEL")"
number="$(jq -r '.number // ""' <<<"$answer")"
[[ "$number" =~ ^[0-9]+$ ]] || die "creating the issue: the answer carries no issue number"
finish opened "main is red, opened #$number"
