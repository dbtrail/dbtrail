#!/usr/bin/env bash
# Downloads every Go module the repository builds and tests with, before a CI
# job compiles anything:
#
#   scripts/go-mod-download.sh
#
# Why a step of its own: without it, a module missing from the restored cache
# is fetched in the middle of `go test`, with one try, and a proxy that does
# not answer for a moment fails the whole job (a read from proxy.golang.org
# cut off mid-download did exactly that to a MySQL integration shard). Here
# the download gets three tries.
#
# Why the cache can miss modules at all: actions/setup-go keys its cache on
# go.sum and saves it once per key, from whichever job finishes first. A job
# that compiles one package saves a module cache holding only that package's
# modules, and every later job restores that partial cache as a hit. Running
# this script in every job that saves the cache also makes the saved cache
# complete, whichever job saves it.
#
# The last try lets the go command fetch a module from its origin when the
# proxy fails for any reason ("|direct"), not only when the proxy reports the
# module missing (the default ",direct"). go.sum still checks every byte.
#
# Exits 0 once every module is in the module cache, 1 after the last try fails.
set -uo pipefail

TRIES=3
RETRY_WAIT="${RETRY_WAIT:-10}"

for try in $(seq 1 "$TRIES"); do
  if [ "$try" -eq "$TRIES" ]; then
    proxy="https://proxy.golang.org|direct"
  else
    proxy="$(go env GOPROXY)"
  fi
  if GOPROXY="$proxy" go mod download; then
    echo "go modules: all downloaded (try $try of $TRIES)"
    exit 0
  fi
  echo "go mod download failed (try $try of $TRIES, GOPROXY=$proxy)" >&2
  [ "$try" -eq "$TRIES" ] || sleep "$((try * RETRY_WAIT))"
done
echo "::error::could not download the Go modules in $TRIES tries. The reason is above."
exit 1
