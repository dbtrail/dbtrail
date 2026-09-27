#!/usr/bin/env bash
# Says whether a tag is already published on ghcr.io. Used by
# .github/workflows/console-base-image.yml so a published base image tag is
# never overwritten.
#
# Usage:
#   REGISTRY_USER=<user> REGISTRY_TOKEN=<token> \
#     build/console-base-tag-state.sh ghcr.io/dbtrail/bintrail-console-base <tag>
#
# Prints exactly one word on stdout:
#   exists   the registry answered 200 for the tag
#   absent   the registry answered 404 (no such tag, or no such package yet)
# Any other answer (bad credentials, a registry outage, a rate limit) exits
# non-zero and prints nothing on stdout. Reading "could not ask" as "absent"
# would publish over a tag that may well be there.
set -euo pipefail

fail() { echo "console-base-tag-state: $*" >&2; exit 1; }

[ "$#" -eq 2 ] || fail "usage: $0 ghcr.io/<owner>/<name> <tag>"
image="$1"
tag="$2"
case "$image" in
  ghcr.io/*/*) repo="${image#ghcr.io/}" ;;
  *) fail "'$image' is not a ghcr.io/<owner>/<name> reference" ;;
esac
[[ "$tag" =~ ^[a-z0-9][a-z0-9._-]{0,100}$ ]] || fail "'$tag' is not a valid tag"
[ -n "${REGISTRY_USER:-}" ] || fail "REGISTRY_USER is not set"
[ -n "${REGISTRY_TOKEN:-}" ] || fail "REGISTRY_TOKEN is not set"

command -v curl >/dev/null 2>&1 || fail "curl is not on PATH"
command -v jq >/dev/null 2>&1 || fail "jq is not on PATH"

token_json="$(curl -sS --fail-with-body --max-time 30 \
  -u "${REGISTRY_USER}:${REGISTRY_TOKEN}" \
  "https://ghcr.io/token?service=ghcr.io&scope=repository:${repo}:pull")" \
  || fail "the registry refused to issue a pull token for ${repo}: ${token_json:-no answer}"
bearer="$(printf '%s' "$token_json" | jq -er '.token')" \
  || fail "the registry's token answer has no token in it"
[ -n "$bearer" ] || fail "the registry issued an empty token"

code="$(curl -sS --max-time 30 -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer ${bearer}" \
  -H 'Accept: application/vnd.oci.image.index.v1+json' \
  -H 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
  -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
  -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
  "https://ghcr.io/v2/${repo}/manifests/${tag}")" \
  || fail "could not reach the registry for ${image}:${tag}"

case "$code" in
  200) echo "exists" ;;
  404) echo "absent" ;;
  *) fail "the registry answered HTTP ${code} for ${image}:${tag}; refusing to guess whether the tag exists" ;;
esac
