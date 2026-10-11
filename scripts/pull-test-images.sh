#!/usr/bin/env bash
# Pulls the container images a CI job is about to start, each named in full
# on the command line (registry host included):
#
#   scripts/pull-test-images.sh mirror.gcr.io/library/mysql:8.4 [...]
#
# Why a step of its own, ahead of the `docker run` that would pull anyway:
# the readiness loops in the workflows start a short-lived container from the
# same image and discard its output, so an image that cannot be pulled used
# to end as "did not become ready in time", sixty tries later. Here the pull
# fails where it happens and names the image.
#
# Why it refuses a name without a registry host, and Docker Hub by name: a
# bare `mysql:8.4` is a pull from Docker Hub, whose pull limit stopped whole
# CI runs for hours (#2299). The workflows name a mirror (TEST_IMAGES); when
# that variable is missing the name starts with "/", and it is refused here
# instead of reaching Docker Hub quietly.
#
# Each image gets three tries, five minutes each. Exits 1 when any image
# could not be pulled, after trying them all, and 2 on a name it refuses.
set -uo pipefail

TRIES=3
PULL_TIMEOUT="${PULL_TIMEOUT:-300}"
RETRY_WAIT="${RETRY_WAIT:-5}"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <registry-host>/<image>[:tag] [...]" >&2
  exit 2
fi

# check_name <image>: prints why the name is refused, or nothing.
check_name() {
  local image="$1" host
  case "$image" in
    "") echo "an empty image name"; return ;;
    *[[:space:]]*) echo "the name holds a space or a line break"; return ;;
    */*) ;;
    *) echo "it has no registry host, so it would be pulled from Docker Hub"; return ;;
  esac
  host="$(printf '%s' "${image%%/*}" | tr '[:upper:]' '[:lower:]')"
  case "$host" in
    "") echo "it starts with '/': is TEST_IMAGES set in the workflow?" ;;
    docker.io | index.docker.io | registry-1.docker.io | registry.hub.docker.com)
      echo "it names Docker Hub ($host)" ;;
    *.* | *:*) ;;
    *) echo "'$host' is not a registry host, so it would be pulled from Docker Hub" ;;
  esac
}

for image in "$@"; do
  why="$(check_name "$image")"
  if [ -n "$why" ]; then
    echo "::error::refusing to pull '$image': $why"
    exit 2
  fi
done

status=0
for image in "$@"; do
  pulled=false
  for try in $(seq 1 "$TRIES"); do
    if timeout "$PULL_TIMEOUT" docker pull --quiet "$image"; then
      pulled=true
      break
    fi
    echo "pull of $image failed or passed ${PULL_TIMEOUT}s (try $try of $TRIES)" >&2
    [ "$try" -eq "$TRIES" ] || sleep "$((try * RETRY_WAIT))"
  done
  if [ "$pulled" != true ]; then
    echo "::error::could not pull $image in $TRIES tries. The reason is above; the registry is ${image%%/*}."
    status=1
  fi
done
exit "$status"
