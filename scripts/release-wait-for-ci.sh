#!/usr/bin/env bash
# Waits for the CI run that tested the commit a release tag points at, and
# exits 0 only when it passed. The release workflow (.github/workflows/
# release.yaml, job main-ci-passed) runs this before GoReleaser, instead of
# running the integration suite a second time. The rules, in order:
#
#   1. ci.yml on a push to main, on the tagged commit.
#   2. The same on its parent, when the tagged commit only touches
#      docs/CHANGELOG.md and docs/changelog.d/ (the release commit).
#   3. Only for a tag whose message says "urgent": the CI of the merged pull
#      request that made either of those commits.
#
# A run that is still going, or that failed and may be re-run, is waited
# for, up to WAIT_MINUTES (80). Needs
# GH_TOKEN, REPO (owner/name), SHA (the tagged commit) and TAG.
set -euo pipefail

: "${GH_TOKEN:?}" "${REPO:?}" "${SHA:?}" "${TAG:?}"
wait_minutes="${WAIT_MINUTES:-80}"

# The commits whose CI may vouch for this tag.
commits=("$SHA")
files="$(gh api "repos/$REPO/commits/$SHA" --jq '.files[].filename')"
parents="$(gh api "repos/$REPO/commits/$SHA" --jq '.parents | length')"
if [ -n "$files" ] && [ "$parents" = 1 ] \
  && ! grep -qvE '^docs/CHANGELOG\.md$|^docs/changelog\.d/' <<< "$files"; then
  parent="$(gh api "repos/$REPO/commits/$SHA" --jq '.parents[0].sha')"
  commits+=("$parent")
  echo "The tagged commit only touches the changelog: its parent $parent counts too."
fi

# An annotated tag carries a message; a lightweight one does not.
urgent=0
tag_object="$(gh api "repos/$REPO/git/ref/tags/$TAG" --jq '.object.type + " " + .object.sha')"
if [ "${tag_object%% *}" = tag ]; then
  message="$(gh api "repos/$REPO/git/tags/${tag_object#* }" --jq '.message')"
  if grep -qi 'urgent' <<< "$message"; then
    urgent=1
    echo "The tag's message says urgent: a pull request's CI counts too."
  fi
fi

# Each check is "<what>|<query>": the query lists ci.yml runs, newest first.
checks=()
for c in "${commits[@]}"; do
  checks+=("main's CI on $c|repos/$REPO/actions/workflows/ci.yml/runs?head_sha=$c&event=push&branch=main&per_page=1")
  if [ "$urgent" = 1 ]; then
    head="$(gh api "repos/$REPO/commits/$c/pulls" --jq '[.[] | select(.merged_at != null)][0].head.sha // empty')"
    if [ -n "$head" ]; then
      checks+=("the pull request's CI on $head (merged as $c)|repos/$REPO/actions/workflows/ci.yml/runs?head_sha=$head&event=pull_request&per_page=1")
    fi
  fi
done

deadline=$(( $(date +%s) + wait_minutes * 60 ))
while :; do
  finished_bad=0
  for check in "${checks[@]}"; do
    what="${check%%|*}"
    # A failed call is asked again on the next round, not a verdict: one
    # moment without the API must not stop a release that waited an hour.
    state="$(gh api "${check#*|}" --jq '.workflow_runs[0] | if . == null then "none" else .status + " " + (.conclusion // "") + " " + .html_url end')" \
      || state="unknown (the GitHub API did not answer)"
    case "$state" in
      "completed success "*)
        echo "Passed: $what (${state##* })."
        exit 0
        ;;
      completed*)
        echo "Did not pass: $what ($state)."
        finished_bad=$((finished_bad + 1))
        ;;
      none)
        echo "No run yet: $what."
        ;;
      *)
        echo "Still running: $what ($state)."
        ;;
    esac
  done
  # Not a verdict yet: main's CI fails now and then on a test that passes
  # when run again, and re-running a run's failed jobs (gh run rerun <id>
  # --failed) turns that same run green, which the next round sees.
  if [ "$finished_bad" = "${#checks[@]}" ]; then
    echo "Every run above finished without passing. If a failure is a flaky test, re-run that run's failed jobs and this step goes on by itself; otherwise fix main and tag again."
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "FATAL: no passing CI run for $TAG after $wait_minutes minutes." >&2
    exit 1
  fi
  sleep 60
done
