#!/usr/bin/env bash
# Smoke test for the console base image (build/Dockerfile.console-base): the one
# place mydumper is installed, so the one place it is tested.
#
# What it proves, per source server: the image's OWN mydumper, running as the
# image's OWN user, logs in as a user created the way docs/quickstart.md creates
# it, and brings back the rows. That is the step #1876 broke on arm64, where the
# package is linked against MariaDB Connector/C and loads caching_sha2_password
# from a plugin directory the image did not have.
#
# Usage:
#   build/smoke-console-base.sh                 # build for the host arch, then test
#   IMAGE=<ref> build/smoke-console-base.sh     # test an image that already exists
#   IMAGE=<ref> PLATFORM=linux/arm64 ...        # also assert the image's architecture
#
# Exit 0 only if every source passed. Needs docker; nothing else on the host.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${IMAGE:-}"
PLATFORM="${PLATFORM:-}"
# The servers a source user is most likely to live on. Each one creates users
# with caching_sha2_password unless told otherwise; the script ASSERTS that
# rather than assuming it, so an image whose default moved cannot turn this
# into a test of the plugin that always worked.
SOURCES="${SOURCES:-mysql:8.0 mysql:8.4}"
WANT_PLUGIN="caching_sha2_password"
DOCKERFILE="build/Dockerfile.console-base"

fail() { echo "FAIL: $*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || fail "docker is not on PATH"

# The pin lives in the Dockerfile; read it from there so the two cannot drift.
MYDUMPER_VERSION="$(sed -n 's/^ARG MYDUMPER_VERSION=//p' "$DOCKERFILE")"
[ -n "$MYDUMPER_VERSION" ] || fail "no 'ARG MYDUMPER_VERSION=' line in $DOCKERFILE"

RUN_ID="cbsmoke-$$"
NET="$RUN_ID-net"
SOURCE_NAME=""
BUILT_IMAGE=""
# Best effort by design: cleanup runs on every exit, including the ones where
# the container or the network was never created.
cleanup() {
  [ -n "$SOURCE_NAME" ] && docker rm -f "$SOURCE_NAME" >/dev/null 2>&1
  docker network rm "$NET" >/dev/null 2>&1
  [ -n "$BUILT_IMAGE" ] && docker rmi "$BUILT_IMAGE" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT
# Ctrl-C and a plain kill must clean up too. bash runs the EXIT trap on the
# way out of these, after the docker command in progress returns.
trap 'exit 130' INT
trap 'exit 143' TERM

# A plain string, not an array: bash 3.2 (the macOS default) treats an empty
# array as unset under `set -u`.
platform_flag=""
[ -n "$PLATFORM" ] && platform_flag="--platform=$PLATFORM"

if [ -z "$IMAGE" ]; then
  IMAGE="bintrail-console-base:smoke-$$"
  BUILT_IMAGE="$IMAGE"
  echo "== building $DOCKERFILE as $IMAGE"
  # shellcheck disable=SC2086 # platform_flag is empty or one word
  docker build $platform_flag -f "$DOCKERFILE" -t "$IMAGE" build
fi

# shellcheck disable=SC2086
in_image() { docker run --rm $platform_flag --entrypoint sh "$IMAGE" -c "$1"; }

# The server's own client, with the password in the environment so the
# "password on the command line" warning does not land in captured output.
source_sql() { docker exec -i -e MYSQL_PWD=smokeroot "$SOURCE_NAME" mysql -uroot "$@"; }

echo "== image: $IMAGE"
got_version="$(in_image 'mydumper --version 2>&1 | head -1')" || fail "could not run mydumper --version in $IMAGE"
echo "   $got_version"
case "$got_version" in
  "mydumper v${MYDUMPER_VERSION},"*) ;;
  *) fail "image reports '$got_version', the Dockerfile pins v${MYDUMPER_VERSION}" ;;
esac

got_arch="$(in_image 'uname -m')" || fail "could not run uname in $IMAGE"
echo "   architecture: $got_arch"
case "$PLATFORM" in
  "") ;;
  linux/amd64) [ "$got_arch" = "x86_64" ] || fail "asked for $PLATFORM, image runs as $got_arch" ;;
  linux/arm64) [ "$got_arch" = "aarch64" ] || fail "asked for $PLATFORM, image runs as $got_arch" ;;
  *) fail "unknown PLATFORM '$PLATFORM' (linux/amd64 or linux/arm64)" ;;
esac

# What every image built on this one relies on (see the consumers' Dockerfiles).
got_user="$(in_image 'id -u bintrail')" || fail "the image has no bintrail user"
[ "$got_user" = "999" ] || fail "bintrail is uid $got_user, the compose secret is chowned to 999"
for dir in /var/lib/bintrail /home/bintrail; do
  owner="$(in_image "stat -c %U $dir")" || fail "$dir is missing from the image"
  [ "$owner" = "bintrail" ] || fail "$dir is owned by $owner, not bintrail"
done

# Whole-line match against the dump container's output. Reads from a
# here-string, not a pipe: grep -q stops at the first match, and under
# pipefail a writer cut short would turn a match into a failure.
out_has_line() { grep -qxF -- "$1" <<< "$out"; }

docker network create "$NET" >/dev/null

tested=0

for src in $SOURCES; do
  SOURCE_NAME="$RUN_ID-$(printf '%s' "$src" | tr -c 'a-zA-Z0-9' '-')"
  name="$SOURCE_NAME"
  echo "== source $src"
  docker run -d --name "$name" --network "$NET" -e MYSQL_ROOT_PASSWORD=smokeroot "$src" >/dev/null

  # Three answers in a row: the entrypoint restarts mysqld once during init, so
  # a single success can be the temporary server.
  ok=0
  for _ in $(seq 1 90); do
    if source_sql -e 'SELECT 1' >/dev/null 2>&1; then
      ok=$((ok + 1)); [ "$ok" -ge 3 ] && break
    else
      ok=0
    fi
    sleep 2
  done
  [ "$ok" -ge 3 ] || { docker logs --tail 30 "$name" >&2; fail "$src did not come up"; }

  # The user and its grants are the ones docs/quickstart.md gives for a
  # self-hosted MySQL 8: no plugin named. It has never logged in, so this is
  # also the cold-cache handshake.
  source_sql <<'SQL' || fail "$src: could not create the schema and the user"
CREATE DATABASE shop;
CREATE TABLE shop.items (id INT PRIMARY KEY, name VARCHAR(40));
INSERT INTO shop.items VALUES (1,'one'),(2,'two'),(3,'three');
CREATE VIEW shop.big_items AS SELECT * FROM shop.items WHERE id > 1;
CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'smoke-pw';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';
GRANT RELOAD, BACKUP_ADMIN, SHOW VIEW ON *.* TO 'dbtrail'@'%';
SQL
  plugin="$(source_sql -N -e "SELECT plugin FROM mysql.user WHERE user='dbtrail' AND host='%'")" \
    || fail "$src: could not read the user's plugin"
  [ "$plugin" = "$WANT_PLUGIN" ] || fail "$src created the user with '$plugin', not $WANT_PLUGIN: this run would not test the login #1876 is about"

  # The flags the console passes (consoleapp/baseline.go buildConsoleMydumperArgs),
  # run as the image's default user. The checks run INSIDE the same container:
  # a bind mount would need a directory uid 999 can write on the host.
  # shellcheck disable=SC2086
  out="$(docker run --rm $platform_flag --network "$NET" -e MYSQL_PWD=smoke-pw \
    --entrypoint sh "$IMAGE" -c '
      id -u
      mydumper --host "$1" --port 3306 --user dbtrail --threads 4 \
        --compress-protocol --complete-insert \
        --sync-thread-lock-mode FTWRL --trx-tables \
        --regex "^(?!(mysql|sys|performance_schema|information_schema)(\$|\.))" \
        --outputdir /tmp/dump >/tmp/mydumper.log 2>&1
      rc=$?
      echo "rc=$rc"
      grep -E "CRITICAL|ERROR" /tmp/mydumper.log | head -5
      # For the log only. The rows below are what is checked.
      echo "files=$(ls /tmp/dump 2>/dev/null | wc -l | tr -d " ")"
      for v in one two three; do
        grep -q "\"$v\"" /tmp/dump/shop.items.*.sql 2>/dev/null && echo "has=$v"
      done
      echo "done=1"
    ' sh "$name" 2>&1)" || fail "$src: the dump container did not run: $out"
  printf '%s\n' "$out" | sed 's/^/   /'

  out_has_line 'done=1' || fail "$src: the dump container stopped before its checks finished"
  [ "${out%%$'\n'*}" = "999" ] || fail "$src: mydumper did not run as uid 999"
  out_has_line 'rc=0' || fail "$src: mydumper did not exit 0"
  # mydumper exits 0 on a regex that matches nothing; an exit code alone is
  # not a dump.
  for v in one two three; do
    out_has_line "has=$v" || fail "$src: row '$v' is not in the dump"
  done
  tested=$((tested + 1))

  docker rm -f "$name" >/dev/null
  SOURCE_NAME=""
done

# A SOURCES with nothing in it would otherwise pass without testing anything.
[ "$tested" -ge 1 ] || fail "no source was tested (SOURCES='$SOURCES')"

echo "PASS: $IMAGE dumped every source as a $WANT_PLUGIN user"
