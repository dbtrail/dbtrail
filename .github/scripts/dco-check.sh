#!/usr/bin/env bash
#
# Checks that every commit of a pull request carries a DCO sign-off: a line
# that starts with "Signed-off-by:" followed by a name and an <email>, the
# trailer `git commit -s` writes. See "Developer Certificate of Origin" in
# .github/CONTRIBUTING.md.
#
# Input: the JSON list GitHub returns for GET repos/{repo}/pulls/{n}/commits,
# read from the file named by COMMITS_JSON (several pages concatenated, as
# `gh api --paginate` prints them, are fine).
#
# Skipped, because no person writes them: merge commits (more than one parent)
# and commits whose GitHub author is a bot.
#
# Exit 0 when every remaining commit is signed off, 1 when any is not (each one
# listed), 2 when the input is empty or unreadable: an answer we cannot read is
# never reported as a pass.
#
# Tests: bash .github/scripts/dco-check_test.sh
set -euo pipefail

: "${COMMITS_JSON:?COMMITS_JSON must name the file with the commits}"

if ! rows="$(jq -rs '
    [.[] | if type == "array" then .[] else error("not a list of commits") end]
    | if length == 0 then error("no commits") else . end
    | .[]
    | [ .sha[0:12],
        ((.parents | length) > 1),
        ((.author.type // "") == "Bot" or ((.author.login // "") | endswith("[bot]"))),
        (.commit.message | test("(^|\n)Signed-off-by:[ \t]*[^<\n]*\\S[^<\n]*<[^>\n]+@[^>\n]+>"; "i")),
        (.commit.message | split("\n")[0] | .[0:72])
      ] | @tsv' "$COMMITS_JSON" 2>&1)"; then
  echo "::error::could not read the pull request's commits: ${rows}" >&2
  exit 2
fi

missing=0
while IFS=$'\t' read -r sha is_merge is_bot signed subject; do
  if [ "$is_merge" = "true" ] || [ "$is_bot" = "true" ]; then
    continue
  fi
  if [ "$signed" != "true" ]; then
    echo "::error::commit ${sha} (\"${subject}\") has no Signed-off-by line"
    missing=$((missing + 1))
  fi
done <<< "$rows"

if [ "$missing" -gt 0 ]; then
  cat <<'HOW'

Every commit needs a DCO sign-off (see "Developer Certificate of Origin" in
.github/CONTRIBUTING.md). To add it to the commits already in this branch:

    git rebase --signoff origin/main
    git push --force-with-lease

and use `git commit -s` from now on.
HOW
  exit 1
fi
echo "every commit is signed off"
