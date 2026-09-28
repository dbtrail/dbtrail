#!/usr/bin/env bash
# Tests for build/console-base-registry.sh against a stand-in registry on a
# local port the system picks. Needs bash, curl, jq and python3. No docker, no
# network.
#
# Each case names the answer the stand-in gives, the command, the exit code
# and the exact stdout expected. An error must leave stdout empty.
#
# shellcheck disable=SC2016 # the jq filters below are meant to stay unexpanded
set -euo pipefail

cd "$(dirname "$0")/.."
SCRIPT="build/console-base-registry.sh"

for tool in curl jq python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FAIL: $tool is not on PATH" >&2; exit 1; }
done

work="$(mktemp -d)"
server_pid=""
cleanup() {
  [ -n "$server_pid" ] && kill "$server_pid" >/dev/null 2>&1
  rm -rf "$work"
  return 0
}
trap cleanup EXIT

# The stand-in: answers every GET from routes.json, read again on each request.
# A path that is not in the file gets a 404, like a registry would give.
cat > "$work/server.py" <<'PY'
import base64, json, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

routes_path, port_path = sys.argv[1], sys.argv[2]

class Handler(BaseHTTPRequestHandler):
    timeout = 5

    def do_GET(self):
        with open(routes_path) as f:
            routes = json.load(f)
        path = self.path.split("?")[0]
        if path == "/token":
            want = "Basic " + base64.b64encode(b"tester:secret").decode()
            if self.headers.get("Authorization") != want:
                return self.answer({"code": 401, "body": '{"errors":[{"code":"UNAUTHORIZED"}]}'})
        elif self.headers.get("Authorization") != "Bearer stand-in-token":
            return self.answer({"code": 401, "body": '{"errors":[{"code":"UNAUTHORIZED"}]}'})
        self.answer(routes.get(path, {"code": 404, "body": '{"errors":[{"code":"MANIFEST_UNKNOWN"}]}'}))

    def answer(self, route):
        body = route.get("body", "")
        if not isinstance(body, str):
            body = json.dumps(body)
        data = body.encode()
        self.send_response(route["code"])
        for name, value in route.get("headers", {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(port_path, "w") as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
PY

echo '{}' > "$work/routes.json"
python3 "$work/server.py" "$work/routes.json" "$work/port" &
server_pid=$!
for _ in $(seq 1 50); do
  [ -s "$work/port" ] && break
  sleep 0.1
done
[ -s "$work/port" ] || { echo "FAIL: the stand-in registry did not start" >&2; exit 1; }
port="$(cat "$work/port")"

IMAGE="ghcr.io/example/base"
TAG="t1"
V2="/v2/example/base"
HASH_A="$(printf 'a%.0s' $(seq 1 64))"
HASH_B="$(printf 'b%.0s' $(seq 1 64))"
D_INDEX="sha256:$(printf '1%.0s' $(seq 1 64))"
D_AMD="sha256:$(printf '2%.0s' $(seq 1 64))"
D_ARM="sha256:$(printf '3%.0s' $(seq 1 64))"
D_ATT="sha256:$(printf '4%.0s' $(seq 1 64))"
C_AMD="sha256:$(printf '5%.0s' $(seq 1 64))"
C_ARM="sha256:$(printf '6%.0s' $(seq 1 64))"
LABEL="com.dbtrail.console-base.recipe-sha256"

# Writes routes.json for a published tag. Arguments: the label of the amd64
# image, the label of the arm64 image ("" leaves the label out), and a jq
# filter applied to the finished routes.
routes() {
  local amd_label="$1" arm_label="$2" edit="${3:-.}"
  jq -n \
    --arg v2 "$V2" --arg tag "$TAG" --arg lbl "$LABEL" \
    --arg d_index "$D_INDEX" --arg d_amd "$D_AMD" --arg d_arm "$D_ARM" --arg d_att "$D_ATT" \
    --arg c_amd "$C_AMD" --arg c_arm "$C_ARM" \
    --arg amd_label "$amd_label" --arg arm_label "$arm_label" '
    def labels($v): if $v == "" then {"other": "x"} else {"other": "x", ($lbl): $v} end;
    def manifest($d; $c): {code: 200, headers: {"Docker-Content-Digest": $d}, body: {schemaVersion: 2, config: {digest: $c}}};
    {
      "/token": {code: 200, body: {token: "stand-in-token"}},
      ($v2 + "/manifests/" + $tag): {code: 200, headers: {"docker-content-digest": $d_index}, body: {schemaVersion: 2, manifests: [
        {digest: $d_amd, platform: {os: "linux", architecture: "amd64"}},
        {digest: $d_arm, platform: {os: "linux", architecture: "arm64"}},
        {digest: $d_att, platform: {os: "unknown", architecture: "unknown"}},
        {digest: $d_att, platform: {os: "unknown", architecture: "unknown"}}
      ]}},
      ($v2 + "/manifests/" + $d_amd): manifest($d_amd; $c_amd),
      ($v2 + "/manifests/" + $d_arm): manifest($d_arm; $c_arm),
      ($v2 + "/blobs/" + $c_amd): {code: 200, body: {architecture: "amd64", config: {Labels: labels($amd_label)}}},
      ($v2 + "/blobs/" + $c_arm): {code: 200, body: {architecture: "arm64", config: {Labels: labels($arm_label)}}}
    } | '"$edit" > "$work/routes.json"
}

failures=0
cases=0
# expect <name> <exit code> <stdout> <text stderr must hold> -- <command...>
# A <stdout> that starts with "~" is text stdout must hold, not the whole of it.
expect() {
  local name="$1" want_rc="$2" want_out="$3" want_err="$4"
  shift 5
  local out rc=0
  out="$(CONSOLE_BASE_REGISTRY_URL="${REGISTRY_UNDER_TEST:-http://127.0.0.1:$port}" \
    REGISTRY_USER=tester REGISTRY_TOKEN=secret "$SCRIPT" "$@" 2>"$work/stderr")" || rc=$?
  cases=$((cases + 1))
  local problem=""
  [ "$rc" = "$want_rc" ] || problem="exit $rc, expected $want_rc"
  case "$want_out" in
    "~"*) case "$out" in *"${want_out#\~}"*) ;; *) problem="${problem:+$problem; }stdout '$out' does not hold '${want_out#\~}'" ;; esac ;;
    *) [ "$out" = "$want_out" ] || problem="${problem:+$problem; }stdout '$out', expected '$want_out'" ;;
  esac
  if [ -n "$want_err" ] && ! grep -qF -- "$want_err" "$work/stderr"; then
    problem="${problem:+$problem; }stderr does not hold '$want_err': $(head -c 300 "$work/stderr")"
  fi
  if [ -n "$problem" ]; then
    echo "FAIL $name: $problem"
    failures=$((failures + 1))
  else
    echo "ok   $name"
  fi
}

routes "$HASH_A" "$HASH_A"
expect "state: tag published" 0 "exists" "" -- state "$IMAGE" "$TAG"
expect "state: tag not published" 0 "absent" "" -- state "$IMAGE" "other"
expect "digest: tag published" 0 "$D_INDEX" "" -- digest "$IMAGE" "$TAG"
expect "digest: tag not published" 1 "" "is not published" -- digest "$IMAGE" "other"
expect "check: both labels match" 0 "same" "" -- check-published "$IMAGE" "$TAG" "$HASH_A"
expect "check: recipe changed" 3 "" "was built from a different" -- check-published "$IMAGE" "$TAG" "$HASH_B"
expect "check: tag not published" 1 "" "is not published" -- check-published "$IMAGE" "other" "$HASH_A"
expect "check: hash argument is not a hash" 1 "" "is not a SHA-256" -- check-published "$IMAGE" "$TAG" "abc"
expect "state: tag is not a tag" 1 "" "is not a valid tag" -- state "$IMAGE" "Bad Tag"
expect "state: image outside ghcr.io" 1 "" "is not a ghcr.io" -- state "docker.io/example/base" "$TAG"
expect "raw: prints the codes" 0 "token request: HTTP 200
manifest request: HTTP 200
manifest digest header: $D_INDEX" "" -- raw "$IMAGE" "$TAG"

routes "$HASH_A" "$HASH_B"
expect "check: only arm64 differs" 3 "" "linux/arm64: published from recipe $HASH_B" -- check-published "$IMAGE" "$TAG" "$HASH_A"

routes "$HASH_A" ""
expect "check: arm64 has no label" 1 "" "has no $LABEL label" -- check-published "$IMAGE" "$TAG" "$HASH_A"
routes "" ""
expect "check: no image has the label" 1 "" "has no $LABEL label" -- check-published "$IMAGE" "$TAG" "$HASH_A"
routes "$HASH_A" "not-a-hash"
expect "check: label is not a hash" 1 "" "unreadable" -- check-published "$IMAGE" "$TAG" "$HASH_A"

routes "$HASH_A" "$HASH_A" '.[$v2 + "/manifests/" + $tag].body.manifests |= map(select(.platform.architecture != "arm64"))'
expect "check: arm64 is missing" 1 "" "holds 0 images for linux/arm64" -- check-published "$IMAGE" "$TAG" "$HASH_A"
routes "$HASH_A" "$HASH_A" '.[$v2 + "/manifests/" + $tag].body = {schemaVersion: 2, config: {digest: $c_amd}}'
expect "check: single-architecture tag" 1 "" "is not a multi-architecture image" -- check-published "$IMAGE" "$TAG" "$HASH_A"
routes "$HASH_A" "$HASH_A" 'del(.[$v2 + "/manifests/" + $d_arm])'
expect "check: arm64 manifest is gone" 1 "" "does not have it" -- check-published "$IMAGE" "$TAG" "$HASH_A"
routes "$HASH_A" "$HASH_A" '.[$v2 + "/blobs/" + $c_arm] = {code: 500, body: "boom"}'
expect "check: config answers 500" 1 "" "HTTP 500" -- check-published "$IMAGE" "$TAG" "$HASH_A"
routes "$HASH_A" "$HASH_A" '.[$v2 + "/blobs/" + $c_arm] = {code: 200, body: "<html>sign in</html>"}'
expect "check: config is a web page" 1 "" "is not JSON" -- check-published "$IMAGE" "$TAG" "$HASH_A"

routes "$HASH_A" "$HASH_A" '.[$v2 + "/manifests/" + $tag] = {code: 200, headers: {"Content-Type": "text/html"}, body: "<html>sign in</html>"}'
expect "state: 200 without a digest header" 1 "" "without a Docker-Content-Digest header" -- state "$IMAGE" "$TAG"
expect "check: 200 without a digest header" 1 "" "without a Docker-Content-Digest header" -- check-published "$IMAGE" "$TAG" "$HASH_A"
routes "$HASH_A" "$HASH_A" '.[$v2 + "/manifests/" + $tag] = {code: 500, body: "boom"}'
expect "state: registry answers 500" 1 "" "HTTP 500" -- state "$IMAGE" "$TAG"
routes "$HASH_A" "$HASH_A" '.[$v2 + "/manifests/" + $tag] = {code: 429, body: "slow down"}'
expect "check: registry answers 429" 1 "" "HTTP 429" -- check-published "$IMAGE" "$TAG" "$HASH_A"

routes "$HASH_A" "$HASH_A" '."/token" = {code: 403, body: {errors: [{code: "DENIED"}]}}'
expect "state: token refused" 1 "" "HTTP 403" -- state "$IMAGE" "$TAG"
expect "check: token refused" 1 "" "HTTP 403" -- check-published "$IMAGE" "$TAG" "$HASH_A"
expect "raw: token refused still exits 0" 0 'token request: HTTP 403
token answer: {"errors": [{"code": "DENIED"}]}' "" -- raw "$IMAGE" "$TAG"
routes "$HASH_A" "$HASH_A" '."/token" = {code: 200, body: {token: ""}}'
expect "state: empty token" 1 "" "has no token in it" -- state "$IMAGE" "$TAG"
routes "$HASH_A" "$HASH_A" '."/token" = {code: 200, body: "<html>sign in</html>"}'
expect "state: token answer is a web page" 1 "" "has no token in it" -- state "$IMAGE" "$TAG"

# A registry that is not there: the stand-in is stopped and its port is closed.
routes "$HASH_A" "$HASH_A"
kill "$server_pid"
wait "$server_pid" 2>/dev/null || true
server_pid=""
expect "state: registry down" 1 "" "could not reach the registry" -- state "$IMAGE" "$TAG"
expect "check: registry down" 1 "" "could not reach the registry" -- check-published "$IMAGE" "$TAG" "$HASH_A"
expect "raw: registry down still exits 0" 0 "~token request: no answer" "" -- raw "$IMAGE" "$TAG"

want_hash="$(sha256sum build/Dockerfile.console-base 2>/dev/null || shasum -a 256 build/Dockerfile.console-base)"
expect "recipe-hash: the Dockerfile's SHA-256" 0 "${want_hash%% *}" "" -- recipe-hash

echo "$cases cases, $failures failed"
[ "$failures" -eq 0 ] || exit 1
[ "$cases" -ge 30 ] || { echo "FAIL: only $cases cases ran" >&2; exit 1; }
echo "PASS"
