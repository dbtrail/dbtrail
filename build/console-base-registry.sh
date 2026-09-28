#!/usr/bin/env bash
# Questions about the console base image that only the registry can answer.
# Used by .github/workflows/console-base-image.yml so that a published tag is
# never overwritten and a changed recipe is never left unpublished.
#
# Usage:
#   build/console-base-registry.sh recipe-hash
#   build/console-base-registry.sh state <image> <tag>
#   build/console-base-registry.sh digest <image> <tag>
#   build/console-base-registry.sh check-published <image> <tag> <recipe-hash>
#   build/console-base-registry.sh raw <image> <tag>
#
# <image> is ghcr.io/<owner>/<name>. Every command but recipe-hash needs
# REGISTRY_USER and REGISTRY_TOKEN in the environment.
#
# recipe-hash
#   Prints the SHA-256 of build/Dockerfile.console-base. The workflow stores it
#   in the image as the label named in RECIPE_LABEL below. Only the Dockerfile
#   is hashed: it copies nothing from the build context and the workflow passes
#   it no build arguments, so it is the only file in this repository that
#   decides what the image holds. The label is passed at build time and is not
#   written in the Dockerfile, so storing the hash does not change the hash.
#   What the hash cannot see: the Debian base image and the apt packages move on
#   their own. Two builds of the same recipe on different days can differ.
#
# state
#   Prints "exists" when the registry answers 200 with a Docker-Content-Digest
#   header, "absent" when it answers 404. Anything else is an error.
#   ghcr.io answers 404 both for a tag that does not exist and for a package
#   the token is not allowed to see. So "absent" means "absent, or hidden from
#   this token". Nothing gets overwritten because of that: a token that cannot
#   see a package cannot push to it either, and the publish fails there.
#
# digest
#   Prints the digest of a published tag. An absent tag is an error.
#
# check-published
#   For a tag that exists: requires linux/amd64 and linux/arm64 in it, reads
#   the recipe label of each and compares it with <recipe-hash>.
#   Prints "same" and exits 0 when both match.
#   Exits 3 when a label holds a different hash.
#   Exits 1 on anything else: the tag is absent, an architecture is missing,
#   the label is missing, the registry did not answer. A label that cannot be
#   read is never taken as a match.
#
# raw
#   Prints the HTTP codes of the token request and of the manifest request and
#   always exits 0. It decides nothing. It is there to put in a log what the
#   registry says to a given token.
#
# Errors go to stderr and leave stdout empty, so a caller that captures stdout
# cannot mistake an error for an answer.
set -euo pipefail

RECIPE="build/Dockerfile.console-base"
RECIPE_LABEL="com.dbtrail.console-base.recipe-sha256"
# Tests point this at a local stand-in. Nothing else should set it.
REGISTRY_URL="${CONSOLE_BASE_REGISTRY_URL:-https://ghcr.io}"
WANT_PLATFORMS="linux/amd64 linux/arm64"

fail() { echo "console-base-registry: $*" >&2; exit 1; }

cd "$(dirname "$0")/.."

recipe_hash() {
  [ -f "$RECIPE" ] || fail "$RECIPE is missing"
  local line
  if command -v sha256sum >/dev/null 2>&1; then
    line="$(sha256sum "$RECIPE")" || fail "could not hash $RECIPE"
  elif command -v shasum >/dev/null 2>&1; then
    line="$(shasum -a 256 "$RECIPE")" || fail "could not hash $RECIPE"
  else
    fail "neither sha256sum nor shasum is on PATH"
  fi
  local hash="${line%% *}"
  [[ "$hash" =~ ^[0-9a-f]{64}$ ]] || fail "unexpected hash '$hash' for $RECIPE"
  echo "$hash"
}

work=""
cleanup() { [ -n "$work" ] && rm -rf "$work"; return 0; }
trap cleanup EXIT

repo=""
image=""
bearer=""

setup() {
  image="$1"
  local tag="$2"
  case "$image" in
    ghcr.io/*/*) repo="${image#ghcr.io/}" ;;
    *) fail "'$image' is not a ghcr.io/<owner>/<name> reference" ;;
  esac
  [[ "$tag" =~ ^[a-z0-9][a-z0-9._-]{0,100}$ ]] || fail "'$tag' is not a valid tag"
  [ -n "${REGISTRY_USER:-}" ] || fail "REGISTRY_USER is not set"
  [ -n "${REGISTRY_TOKEN:-}" ] || fail "REGISTRY_TOKEN is not set"
  command -v curl >/dev/null 2>&1 || fail "curl is not on PATH"
  command -v jq >/dev/null 2>&1 || fail "jq is not on PATH"
  work="$(mktemp -d)"
}

# Both print the HTTP code and leave the answer in $work/body (and, for a
# manifest, its headers in $work/headers). A request that got no answer at all
# returns non-zero.
request_token() {
  curl -sS --max-time 30 -o "$work/body" -w '%{http_code}' \
    -u "${REGISTRY_USER}:${REGISTRY_TOKEN}" \
    "${REGISTRY_URL}/token?service=ghcr.io&scope=repository:${repo}:pull"
}

request() { # <path under /v2/<repo>/>
  : > "$work/headers"
  curl -sS -L --max-time 60 -o "$work/body" -D "$work/headers" -w '%{http_code}' \
    -H "Authorization: Bearer ${bearer}" \
    -H 'Accept: application/vnd.oci.image.index.v1+json' \
    -H 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
    -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
    -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    "${REGISTRY_URL}/v2/${repo}/$1"
}

login() {
  local code
  code="$(request_token)" || fail "could not reach the registry for a token"
  [ "$code" = "200" ] || fail "the registry answered HTTP ${code} to the token request for ${repo}: $(head -c 300 "$work/body")"
  bearer="$(jq -er '.token | select(type == "string" and length > 0)' "$work/body" 2>/dev/null)" \
    || fail "the registry's token answer has no token in it"
}

# The digest header of the last request, or nothing.
header_digest() {
  tr -d '\r' < "$work/headers" \
    | sed -n 's/^[Dd][Oo][Cc][Kk][Ee][Rr]-[Cc][Oo][Nn][Tt][Ee][Nn][Tt]-[Dd][Ii][Gg][Ee][Ss][Tt]: *\(sha256:[0-9a-f]\{64\}\) *$/\1/p' \
    | tail -n 1
}

# Fetches the manifest of <ref>. Prints "absent" on 404, the digest on 200.
# A 200 without the digest header is not a registry answer (a proxy page, a
# login page) and is an error.
manifest() {
  local ref="$1" code digest
  code="$(request "manifests/${ref}")" || fail "could not reach the registry for ${image}:${ref}"
  case "$code" in
    200)
      digest="$(header_digest)"
      [ -n "$digest" ] || fail "the registry answered HTTP 200 for ${image}:${ref} without a Docker-Content-Digest header; that is not a manifest"
      echo "$digest"
      ;;
    404) echo "absent" ;;
    *) fail "the registry answered HTTP ${code} for ${image}:${ref}; refusing to guess whether it exists" ;;
  esac
}

cmd_state() {
  setup "$1" "$2"
  login
  local got
  got="$(manifest "$2")"
  if [ "$got" = "absent" ]; then echo "absent"; else echo "exists"; fi
}

cmd_digest() {
  setup "$1" "$2"
  login
  local got
  got="$(manifest "$2")"
  [ "$got" != "absent" ] || fail "${image}:$2 is not published"
  echo "$got"
}

cmd_check_published() {
  local tag="$2" want_hash="$3"
  [[ "$want_hash" =~ ^[0-9a-f]{64}$ ]] || fail "'$want_hash' is not a SHA-256"
  setup "$1" "$tag"
  login
  local got
  got="$(manifest "$tag")"
  [ "$got" != "absent" ] || fail "${image}:${tag} is not published, there is nothing to compare"
  jq -e '.manifests | type == "array"' "$work/body" >/dev/null 2>&1 \
    || fail "${image}:${tag} is not a multi-architecture image"
  cp "$work/body" "$work/index"

  local platform os arch count arch_digest config_digest code label different=""
  for platform in $WANT_PLATFORMS; do
    os="${platform%/*}"
    arch="${platform#*/}"
    count="$(jq -r --arg os "$os" --arg arch "$arch" \
      '[.manifests[] | select(.platform.os == $os and .platform.architecture == $arch)] | length' "$work/index")" \
      || fail "could not read the platforms of ${image}:${tag}"
    [ "$count" = "1" ] || fail "${image}:${tag} holds ${count} images for ${platform}, expected exactly 1"
    arch_digest="$(jq -er --arg os "$os" --arg arch "$arch" \
      '.manifests[] | select(.platform.os == $os and .platform.architecture == $arch) | .digest' "$work/index")" \
      || fail "could not read the ${platform} digest of ${image}:${tag}"
    [[ "$arch_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "unexpected ${platform} digest '$arch_digest'"

    got="$(manifest "$arch_digest")"
    [ "$got" != "absent" ] || fail "${image}:${tag} points at ${arch_digest} for ${platform}, and the registry does not have it"
    config_digest="$(jq -er '.config.digest' "$work/body" 2>/dev/null)" \
      || fail "the ${platform} manifest of ${image}:${tag} has no image config"
    [[ "$config_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "unexpected ${platform} config digest '$config_digest'"

    code="$(request "blobs/${config_digest}")" || fail "could not reach the registry for the ${platform} image config"
    [ "$code" = "200" ] || fail "the registry answered HTTP ${code} for the ${platform} image config"
    jq -e 'type == "object"' "$work/body" >/dev/null 2>&1 \
      || fail "the ${platform} image config of ${image}:${tag} is not JSON"
    label="$(jq -r --arg l "$RECIPE_LABEL" '.config.Labels[$l] // ""' "$work/body")" \
      || fail "could not read the labels of the ${platform} image"
    [ -n "$label" ] || fail "the ${platform} image of ${image}:${tag} has no ${RECIPE_LABEL} label, so there is no way to tell which recipe built it"
    [[ "$label" =~ ^[0-9a-f]{64}$ ]] || fail "the ${platform} image of ${image}:${tag} has an unreadable ${RECIPE_LABEL} label: '$label'"
    if [ "$label" != "$want_hash" ]; then
      different="${different}  ${platform}: published from recipe ${label}"$'\n'
    fi
  done

  if [ -n "$different" ]; then
    {
      echo "console-base-registry: ${image}:${tag} was built from a different ${RECIPE}."
      echo "  this tree: recipe ${want_hash}"
      printf '%s' "$different"
    } >&2
    exit 3
  fi
  echo "same"
}

cmd_raw() {
  setup "$1" "$2"
  local code
  if code="$(request_token 2>"$work/err")"; then
    echo "token request: HTTP ${code}"
  else
    echo "token request: no answer ($(head -c 200 "$work/err"))"
    return 0
  fi
  if [ "$code" != "200" ]; then
    echo "token answer: $(head -c 300 "$work/body")"
    return 0
  fi
  bearer="$(jq -r '.token // ""' "$work/body" 2>/dev/null || true)"
  if [ -z "$bearer" ]; then
    echo "token answer: no token in it"
    return 0
  fi
  if code="$(request "manifests/$2" 2>"$work/err")"; then
    echo "manifest request: HTTP ${code}"
    [ "$code" = "200" ] || echo "manifest answer: $(head -c 300 "$work/body")"
    echo "manifest digest header: $(header_digest)"
  else
    echo "manifest request: no answer ($(head -c 200 "$work/err"))"
  fi
  return 0
}

[ "$#" -ge 1 ] || fail "usage: $0 recipe-hash | state|digest|raw <image> <tag> | check-published <image> <tag> <recipe-hash>"
command="$1"
shift
case "$command" in
  recipe-hash) [ "$#" -eq 0 ] || fail "recipe-hash takes no arguments"; recipe_hash ;;
  state) [ "$#" -eq 2 ] || fail "usage: $0 state <image> <tag>"; cmd_state "$@" ;;
  digest) [ "$#" -eq 2 ] || fail "usage: $0 digest <image> <tag>"; cmd_digest "$@" ;;
  raw) [ "$#" -eq 2 ] || fail "usage: $0 raw <image> <tag>"; cmd_raw "$@" ;;
  check-published) [ "$#" -eq 3 ] || fail "usage: $0 check-published <image> <tag> <recipe-hash>"; cmd_check_published "$@" ;;
  *) fail "unknown command '$command'" ;;
esac
