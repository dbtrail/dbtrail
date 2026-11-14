#!/bin/sh
# What a change touched on the MySQL path, as Markdown (#2816).
#
#   scripts/mysql-path-touched.sh <base>     e.g. origin/main
#
# Lists the files changed between <base> and HEAD that a MySQL source is
# served through, with the lines added and removed in each, and prints the
# removed lines. It informs and never fails a build: shared files are edited
# on purpose, and what this is for is that nobody has to remember to look. A
# squash that drops another change's lines shows here as removed lines that
# the change did not mean to remove.
#
# The list is the code MySQL statements run through: the port and its
# routing, the copy (written, viewed, queried), the index, and the reviewed
# answers of the MySQL regression gate. Keep it in step with
# internal/event/mysqlpath_guard_test.go.
set -u

base=${1:?usage: mysql-path-touched.sh <base>}
# The most removed lines printed for one file; the count above it is exact.
SHOWN=${MYSQL_PATH_SHOWN:-40}

paths="internal/shim internal/readrouter internal/sqlsandbox internal/views
internal/baseline internal/indexer internal/query internal/reconstruct
internal/event consoleapp/router.go consoleapp/flashback.go
internal/mysqlgate/testdata"

echo "### MySQL path"
echo

# shellcheck disable=SC2086 # the list is words on purpose
if ! stat=$(git diff --numstat "$base"...HEAD -- $paths 2>&1); then
  echo "The changed files could not be listed against \`$base\`:"
  echo
  echo '```'
  echo "$stat"
  echo '```'
  exit 0
fi
if [ -z "$stat" ]; then
  echo "This change touches no file on the MySQL path."
  exit 0
fi

echo "This change touches files a MySQL source is served through. Shared files are edited on purpose; this is the list to read them by."
echo
echo "| File | Added | Removed |"
echo "|---|---|---|"
echo "$stat" | while IFS="$(printf '\t')" read -r added removed file; do
  echo "| \`$file\` | $added | $removed |"
done

echo "$stat" | while IFS="$(printf '\t')" read -r added removed file; do
  case "$removed" in ''|0|-) continue ;; esac
  echo
  echo "<details><summary>$removed removed in <code>$file</code></summary>"
  echo
  echo '```diff'
  git diff -U0 "$base"...HEAD -- "$file" | grep '^-' | grep -v '^---' | head -n "$SHOWN"
  echo '```'
  if [ "$removed" -gt "$SHOWN" ] 2>/dev/null; then
    echo
    echo "The first $SHOWN of $removed."
  fi
  echo
  echo "</details>"
done
exit 0
