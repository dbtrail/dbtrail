#!/usr/bin/env bash
#
# Tests for dco-check.sh. No network: each case writes the commits JSON GitHub
# would return and asserts on the exit code and on what was printed.
#
#   bash .github/scripts/dco-check_test.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${SCRIPT:-$HERE/dco-check.sh}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pass=0
fail=0

# commit SHA MESSAGE [PARENTS] [LOGIN] [TYPE] -> one commit object
commit() {
  jq -n --arg sha "$1" --arg msg "$2" --argjson parents "${3:-1}" \
    --arg login "${4:-alice}" --arg type "${5:-User}" '
    { sha: $sha,
      parents: [range($parents) | {sha: "p\(.)"}],
      author: {login: $login, type: $type},
      commit: {message: $msg} }'
}

# expect NAME WANT_EXIT WANT_TEXT JSON
expect() {
  local name="$1" want="$2" text="$3" json="$4" out code
  printf '%s' "$json" > "$WORK/commits.json"
  set +e
  out="$(COMMITS_JSON="$WORK/commits.json" bash "$SCRIPT" 2>&1)"
  code=$?
  set -e
  if [ "$code" = "$want" ] && [[ "$out" == *"$text"* ]]; then
    pass=$((pass + 1))
  else
    fail=$((fail + 1))
    echo "FAIL ${name}: exit ${code} (want ${want}), output:"
    printf '    %s\n' "${out//$'\n'/$'\n    '}"
  fi
}

SIGNED=$'Add a filter\n\nSigned-off-by: Alice Example <alice@example.com>'

expect "signed commit passes" 0 "every commit is signed off" \
  "[$(commit aaaaaaaaaaaa1 "$SIGNED")]"

expect "unsigned commit fails and is named" 1 'commit aaaaaaaaaaaa ("Add a filter") has no Signed-off-by line' \
  "[$(commit aaaaaaaaaaaa1 $'Add a filter\n\nWhy it matters.')]"

expect "one unsigned among signed fails" 1 'commit bbbbbbbbbbbb' \
  "[$(commit aaaaaaaaaaaa1 "$SIGNED"),$(commit bbbbbbbbbbbb2 'Fix typo')]"

expect "lowercase trailer still counts" 0 "every commit is signed off" \
  "[$(commit aaaaaaaaaaaa1 $'Fix\n\nsigned-off-by: Alice <alice@example.com>')]"

expect "CRLF line endings still count" 0 "every commit is signed off" \
  "[$(commit aaaaaaaaaaaa1 $'Fix\r\n\r\nSigned-off-by: Alice <alice@example.com>\r\n')]"

expect "sign-off on the subject line itself counts" 0 "every commit is signed off" \
  "[$(commit aaaaaaaaaaaa1 'Signed-off-by: Alice <alice@example.com>')]"

expect "mentioning the trailer mid-line does not count" 1 "has no Signed-off-by line" \
  "[$(commit aaaaaaaaaaaa1 $'Fix\n\nI will add Signed-off-by: Alice <alice@example.com> later')]"

expect "trailer without an email does not count" 1 "has no Signed-off-by line" \
  "[$(commit aaaaaaaaaaaa1 $'Fix\n\nSigned-off-by: Alice')]"

expect "trailer without a name does not count" 1 "has no Signed-off-by line" \
  "[$(commit aaaaaaaaaaaa1 $'Fix\n\nSigned-off-by: <alice@example.com>')]"

expect "email without @ does not count" 1 "has no Signed-off-by line" \
  "[$(commit aaaaaaaaaaaa1 $'Fix\n\nSigned-off-by: Alice <alice>')]"

expect "merge commit is skipped" 0 "every commit is signed off" \
  "[$(commit aaaaaaaaaaaa1 "$SIGNED"),$(commit cccccccccccc3 "Merge branch 'main'" 2)]"

expect "bot by type is skipped" 0 "every commit is signed off" \
  "[$(commit dddddddddddd4 'Bump x from 1 to 2' 1 some-app Bot)]"

expect "bot by [bot] login is skipped" 0 "every commit is signed off" \
  "[$(commit dddddddddddd4 'Bump x from 1 to 2' 1 'dependabot[bot]' User)]"

expect "a person named like a bot word is not skipped" 1 "has no Signed-off-by line" \
  "[$(commit eeeeeeeeeeee5 'Fix' 1 robotfan User)]"

expect "two pages from --paginate are both read" 1 'commit ffffffffffff' \
  "[$(commit aaaaaaaaaaaa1 "$SIGNED")][$(commit ffffffffffff6 'Unsigned on page two')]"

expect "empty list is an error, not a pass" 2 "could not read" '[]'
expect "empty input is an error, not a pass" 2 "could not read" ''
expect "an HTML error page is an error, not a pass" 2 "could not read" '<html>502 Bad Gateway</html>'
expect "an API error object is an error, not a pass" 2 "could not read" '{"message":"Not Found"}'

expect "subject with a tab does not shift the columns" 1 'has no Signed-off-by line' \
  "[$(commit aaaaaaaaaaaa1 $'Fix\tthing')]"

echo "dco-check: ${pass} passed, ${fail} failed"
[ "$fail" -eq 0 ]
