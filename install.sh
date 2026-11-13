#!/bin/sh
# dbtrail one-command installer.
#
#   curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | sh
#
# Downloads the Docker Compose stack, brings it up, waits for the console to
# actually answer, then tells you exactly where to go next. Everything it does
# you could do by hand (it prints each step); it just removes the "...now what?"
# gap after `docker compose up -d`.
#
# Knobs (all optional, set them before the pipe — e.g.  DBTRAIL_DIR=/opt/dbtrail sh):
#   DBTRAIL_DIR      where to put the stack        (default: ./dbtrail)
#   DBTRAIL_REF      git ref for the compose file and the dbtrail command
#                    (default: the newest release, see "which version" below).
#                    A release tag (v0.130.0) also pins the images to it; any
#                    other ref (main, a branch) leaves them on `latest`.
#   DBTRAIL_PORT     host port the console answers  (default: 8090)
#   DBTRAIL_METRICS_PORT  host port for Prometheus /metrics (default: 9090,
#                    or the next free one when 9090 is taken)
#   DBTRAIL_NO_OPEN  set to 1 to NOT open a browser (default: opens best-effort)
#
# No root needed beyond whatever your Docker setup already requires.

set -eu

DIR="${DBTRAIL_DIR:-./dbtrail}"
PORT="${DBTRAIL_PORT:-8090}"
MPORT="${DBTRAIL_METRICS_PORT:-9090}"
# REF (the git ref the compose file and the dbtrail command come from) is
# chosen below, once the installer knows which version it installs.
RAW_URL="https://raw.githubusercontent.com/dbtrail/dbtrail"
RELEASE_API="https://api.github.com/repos/dbtrail/dbtrail/releases/latest"
HEALTH_URL="http://127.0.0.1:${PORT}/api/healthz"
CONSOLE_URL="http://127.0.0.1:${PORT}"
START_PAGE="https://www.dbtrail.com/docs/quickstart/"
# The promise, word for word the headline of the README's header and of the
# dbtrail.com home page (#2637); test/installer pins it.
PROMISE="Heavy MySQL queries, from minutes to seconds."

# ── color capability detection ──────────────────────────────────────────
# Four tiers so the sunset gradient degrades gracefully: 24-bit truecolor →
# 256-color cube → basic 8/16 ANSI → no color. Under `curl … | sh` only stdin
# is the pipe — stdout is still the terminal — so colors render; they drop only
# when stdout is redirected, NO_COLOR is set, or TERM is dumb.
COLORTIER=none
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ] && [ "${TERM:-dumb}" != "dumb" ]; then
  case "${COLORTERM:-}" in
    truecolor|24bit) COLORTIER=truecolor ;;
    *)
      ncolors=$(tput colors 2>/dev/null || echo 0)
      if   [ "${ncolors:-0}" -ge 256 ]; then COLORTIER=256
      elif [ "${ncolors:-0}" -ge 8   ]; then COLORTIER=basic
      fi ;;
  esac
fi

# Plain attributes (work in every color tier; empty when there's no color).
if [ "$COLORTIER" = none ]; then
  B=''; DIM=''; RST=''
else
  B=$(printf '\033[1m'); DIM=$(printf '\033[2m'); RST=$(printf '\033[0m')
fi

# fg R G B BASIC — emit a foreground color for the active tier. BASIC is the
# 8/16-color SGR code (e.g. 35=magenta) used when truecolor/256 aren't there.
rgb256() { printf '%d' $(( 16 + 36*(($1*5+127)/255) + 6*(($2*5+127)/255) + (($3*5+127)/255) )); }
fg() {
  case "$COLORTIER" in
    truecolor) printf '\033[38;2;%d;%d;%dm' "$1" "$2" "$3" ;;
    256)       printf '\033[38;5;%dm' "$(rgb256 "$1" "$2" "$3")" ;;
    basic)     printf '\033[%dm' "$4" ;;
  esac
}
crst() { [ "$COLORTIER" = none ] || printf '\033[0m'; }

# Semantic colors built on fg (so step/warn/die share the tiering).
cstep() { fg 124 92 255 36; }   # violet  / cyan
cwarn() { fg 255 138 61 33; }   # orange  / yellow
cerr()  { fg 255  77 141 31; }  # pink    / red
say()  { printf '%s\n' "$*"; }
step() { cstep; printf '==>'; crst; printf ' %s\n' "$*"; }
warn() { cwarn; printf '!  %s' "$*"; crst; printf '\n'; }
die()  { { cerr; printf 'ERROR:'; crst; printf ' %s\n' "$*"; } >&2; exit 1; }

# ── brand banner ────────────────────────────────────────────────────────
# The dbtrail wordmark in the site's tropical-sunset gradient (pink → orange →
# violet, #FF4D8D → #FF8A3D → #7C5CFF) topped by a gold "sun" node on a
# gradient bar. Per-char gradient in truecolor/256; a flat bold accent in basic.
sunset_word() {
  word=DBTrail; n=${#word}; i=0
  while [ "$i" -lt "$n" ]; do
    ch=$(printf '%s' "$word" | cut -c $((i + 1)))
    t=$(( i * 100 / (n - 1) ))           # position 0..100 along the gradient
    if [ "$t" -lt 50 ]; then             # pink → orange
      lt=$(( t * 2 ))
      r=255; g=$(( 77 + (138 - 77) * lt / 100 )); b=$(( 141 + (61 - 141) * lt / 100 ))
    else                                 # orange → violet
      lt=$(( (t - 50) * 2 ))
      r=$(( 255 + (124 - 255) * lt / 100 )); g=$(( 138 + (92 - 138) * lt / 100 )); b=$(( 61 + (255 - 61) * lt / 100 ))
    fi
    fg "$r" "$g" "$b" 35; printf '%s%s' "$B" "$ch"
    i=$(( i + 1 ))
  done
  crst
}
banner() {
  printf '\n  '
  fg 255 210 61 93; printf '●'; crst                          # gold sun node
  printf '   '; sunset_word; printf '\n  '
  fg 255 77 141 35; printf '▌'; crst                          # pink bar
  printf '   %s%s%s\n  ' "$DIM" "$PROMISE" "$RST"
  fg 255 138 61 33; printf '▌'; crst                          # orange bar
  printf '   %sinstalling the Docker stack → %s%s\n\n' "$DIM" "$DIR" "$RST"
}
banner

# ── 1. preflight: docker + compose v2 + a running daemon ────────────────
step "Checking prerequisites"

command -v docker >/dev/null 2>&1 || die \
  "Docker is not installed. Install Docker Desktop or Docker Engine first:
    https://docs.docker.com/get-docker/"

# Prefer the v2 plugin (\`docker compose\`); fall back to legacy \`docker-compose\`.
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
  warn "Using legacy docker-compose v1; v2 (\`docker compose\`) is recommended."
else
  die "Docker Compose is not available. Update Docker Desktop, or install the
    Compose plugin: https://docs.docker.com/compose/install/"
fi

docker info >/dev/null 2>&1 || die \
  "Docker isn't running. Start Docker and re-run this installer."

# Catch the single most common FRESH-install failure — port already taken — with
# an actionable message instead of Docker's raw bind error. Best-effort: if we
# have no probe tool, stay quiet and let Docker decide. Skipped when this dir
# already has a stack (a re-run, where dbtrail's OWN container legitimately holds
# the port) — there `up -d` is a harmless no-op and the console is already up.
port_in_use() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  elif command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$1" >/dev/null 2>&1
  else
    return 1
  fi
}
# first_free_port FROM TO [SKIP] prints the first port in FROM..TO that nothing
# listens on and is not SKIP. With no probe tool every port reads as free.
first_free_port() {
  p=$1
  while [ "$p" -le "$2" ]; do
    if [ "$p" != "${3:-}" ] && ! port_in_use "$p"; then
      printf '%s' "$p"
      return 0
    fi
    p=$((p + 1))
  done
  return 1
}

# rerun_cmd PORT prints this installer's own command line with the console on
# PORT, ready to paste: the variables go on `sh`, the side of the pipe that
# reads them (on `curl` they never reach the installer, #1768), and the knobs
# the operator already set ride along.
rerun_cmd() {
  envs="$1"
  [ "$DIR" != "./dbtrail" ] && envs="DBTRAIL_DIR=$DIR $envs"
  [ -n "${DBTRAIL_REF:-}" ] && [ "$DBTRAIL_REF" != "main" ] && envs="DBTRAIL_REF=$DBTRAIL_REF $envs"
  printf 'curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/%s/install.sh | %s sh' "${DBTRAIL_REF:-main}" "$envs"
}

if [ ! -f "$DIR/docker-compose.yml" ]; then
  if port_in_use "$PORT"; then
    free=$(first_free_port 8091 8099 "$PORT") || free=8091
    die "Port ${PORT} is already in use on this machine, so DBTrail can't use it.
    Run the installer with DBTrail on port ${free} instead:
        $(rerun_cmd "DBTRAIL_PORT=$free")"
  fi
  # The stack also publishes Prometheus /metrics on 9090, which is also
  # Prometheus's own default port. Metrics are secondary, so a taken 9090 moves
  # them to the next free port instead of failing on Docker's raw bind error;
  # a port the operator chose explicitly is theirs and is only checked.
  if [ "$MPORT" = "$PORT" ] || port_in_use "$MPORT"; then
    if [ -n "${DBTRAIL_METRICS_PORT:-}" ]; then
      die "The metrics port ${MPORT} is already in use (or is the port DBTrail answers on).
    Pick another one with DBTRAIL_METRICS_PORT, or leave it unset to have one chosen."
    fi
    MPORT=$(first_free_port 9091 9099 "$PORT") || die \
      "Ports 9090 to 9099 are all in use, so there is nowhere to publish metrics.
    Choose one with DBTRAIL_METRICS_PORT."
    MPORT_MOVED=1
  fi
  # The MySQL-protocol port (3309) is off until it is turned on in the web
  # interface, so a taken 3309 is no reason to fail: the stack is installed
  # without publishing it, and says so.
  if [ "$PORT" = "3309" ] || [ "$MPORT" = "3309" ] || port_in_use 3309; then
    SQLPORT_SKIP=1
  fi
fi

# Need curl or wget to fetch the compose file.
if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1" -o "$2"; }
  fetch_stdout() { curl -fsSL --max-time 30 "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -qO "$2" "$1"; }
  fetch_stdout() { wget -qO- -T 30 "$1"; }
else
  die "Need curl or wget to download the compose file."
fi
say "${DIM}    docker ✓   ${COMPOSE} ✓   Docker running ✓${RST}"

# ── which version: one release, written down ────────────────────────────
# The images are `${BINTRAIL_TAG:-latest}` in the compose file. Following
# `latest` (and taking the compose file from main) meant a re-run or a
# `docker compose pull` jumped over every release since the install, with
# nothing saying which version it had been on (#2638). So the installer
# installs ONE release: the compose file and the dbtrail command come from
# its git tag, and a fresh install writes BINTRAIL_TAG to .env.
#
# Images are tagged without the git tag's v (GoReleaser's {{ .Version }}:
# git tag v0.130.0 -> image 0.130.0), so BINTRAIL_TAG never carries it.
#
# Which version, first match wins:
#   1. DBTRAIL_REF. A release tag (v0.130.0, or a v0.130.0-rc1 prerelease)
#      is that version. Any other ref (main, a branch, a commit) has no
#      image of its own: the images stay on `latest` and nothing is written.
#   2. A BINTRAIL_TAG already set, in this shell (it beats .env in compose)
#      or in .env. A version number is that version, so a re-run, or an
#      upgrade done by changing BINTRAIL_TAG and deleting the compose file,
#      gets the compose file that matches the images. Never changed.
#   3. The newest release, from GitHub's API (no token; it allows 60 lookups
#      an hour per address). /releases/latest skips drafts and prereleases,
#      the same rule that moves the `latest` image.
# A fresh install whose lookup fails STOPS: falling back to main is the
# drift this exists to end. A re-run over an existing stack only needs the
# answer for the dbtrail command, so there a failed lookup keeps the one
# already here.

# env_has KEY FILE: FILE sets KEY, in any form compose's .env reader takes
# (KEY=, KEY:, export KEY=, a bare KEY). An empty value is still set.
env_has() {
  [ -f "$2" ] && grep -Eq "^[[:space:]]*(export[[:space:]]+)?$1[[:space:]]*([=:]|\$)" "$2"
}
# env_value KEY FILE prints the value of the last line that sets KEY, without
# its quotes, a trailing comment or a CR. Only called after env_has.
env_value() {
  grep -E "^[[:space:]]*(export[[:space:]]+)?$1[[:space:]]*([=:]|\$)" "$2" | tail -n 1 | tr -d '\r' |
    sed -e "s/^[[:space:]]*\(export[[:space:]]*\)\{0,1\}$1[[:space:]]*[=:]\{0,1\}[[:space:]]*//" \
        -e 's/[[:space:]][[:space:]]*#.*$//' -e 's/[[:space:]]*$//' \
        -e 's/^"\(.*\)"$/\1/' -e "s/^'\\(.*\\)'\$/\\1/"
}
is_version()     { printf '%s' "$1" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'; }
is_release_tag() { printf '%s' "$1" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'; }

# newest_release prints the tag of the newest release: 1 when GitHub did not
# answer, 2 when the answer holds no release tag (an empty body, a proxy's
# HTML page). The JSON is read with sed, with no jq: split on commas first,
# so a body on one line reads the same as an indented one.
newest_release() {
  body=$(fetch_stdout "$RELEASE_API" 2>/dev/null) || return 1
  tag=$(printf '%s\n' "$body" | tr ',' '\n' |
    sed -n 's/^[[:space:]{]*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
  printf '%s' "$tag" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$' || return 2
  printf '%s' "$tag"
}

step "Choosing the version"
REF=""          # git ref the compose file and the dbtrail command come from
WRITE_TAG=""    # the BINTRAIL_TAG a fresh install writes to .env
PIN=""          # the BINTRAIL_TAG already set, and where: shell or .env
PIN_FROM=""
if [ -n "${BINTRAIL_TAG+set}" ]; then
  PIN=$BINTRAIL_TAG; PIN_FROM="this shell"
elif env_has BINTRAIL_TAG "$DIR/.env"; then
  PIN=$(env_value BINTRAIL_TAG "$DIR/.env"); PIN_FROM=.env
fi
case "$PIN" in
  v[0-9]*)
    if is_version "${PIN#v}"; then
      die "BINTRAIL_TAG=${PIN} (in ${PIN_FROM}) starts with a v, and DBTrail's images are tagged without it.
    Change it to BINTRAIL_TAG=${PIN#v} and re-run."
    fi ;;
esac
REF_FROM_PIN=""

if [ -n "${DBTRAIL_REF:-}" ]; then
  REF=$DBTRAIL_REF
  if is_release_tag "$REF" && [ -z "$PIN_FROM" ]; then
    WRITE_TAG=${REF#v}
  fi
elif [ -n "$PIN_FROM" ] && is_version "$PIN"; then
  REF="v$PIN"
  REF_FROM_PIN=1
else
  rc=0
  REF=$(newest_release) || rc=$?
  if [ "$rc" -ne 0 ]; then
    REF=""
    if [ "$rc" -eq 1 ]; then
      why="GitHub did not answer (no network to api.github.com, or its limit of 60 lookups an hour from this address)."
    else
      why="GitHub's answer did not name a release (a network proxy may have answered instead)."
    fi
    if [ -f "$DIR/docker-compose.yml" ]; then
      warn "Couldn't find the newest DBTrail release: $why"
    else
      envs="DBTRAIL_REF=vX.Y.Z"
      [ "$PORT" != "8090" ] && envs="$envs DBTRAIL_PORT=$PORT"
      [ -n "${DBTRAIL_METRICS_PORT:-}" ] && envs="$envs DBTRAIL_METRICS_PORT=$DBTRAIL_METRICS_PORT"
      die "Couldn't find the newest DBTrail release, so nothing was installed.
    $why
    Try again in a few minutes, or choose the version yourself: take one from
    https://github.com/dbtrail/dbtrail/releases and run, with it in place of vX.Y.Z:
        $(rerun_cmd "$envs")"
    fi
  fi
  [ -n "$PIN_FROM" ] || WRITE_TAG=${REF#v}
fi
COMPOSE_URL="${RAW_URL}/${REF}/docker-compose.yml"
CLI_URL="${RAW_URL}/${REF}/dbtrail.sh"

# What this install runs, said once here and once in the end text.
if [ -n "$PIN_FROM" ] && is_version "$PIN"; then
  VERSION="$PIN"
  if [ "$PIN_FROM" = "this shell" ]; then
    say "${DIM}    BINTRAIL_TAG=${PIN} is set in this shell, so .env was left alone. The value lasts for this run only: put it in .env to keep it${RST}"
  else
    say "${DIM}    BINTRAIL_TAG=${PIN} is already in .env, so this install stays on version ${PIN}${RST}"
  fi
  if [ -n "${DBTRAIL_REF:-}" ] && [ "$DBTRAIL_REF" != "v$PIN" ]; then
    warn "DBTRAIL_REF=${DBTRAIL_REF} gives the compose file of ${DBTRAIL_REF}, but BINTRAIL_TAG=${PIN} keeps the images on ${PIN}. Make the two name the same version."
  fi
elif [ -n "$PIN_FROM" ]; then
  VERSION=""
  if [ -z "$PIN" ]; then
    say "${DIM}    BINTRAIL_TAG is set but empty (in ${PIN_FROM}), so it was left alone and the images are the newest ones, tagged latest${RST}"
  else
    say "${DIM}    BINTRAIL_TAG=${PIN} (in ${PIN_FROM}) is not a version number, so it was left alone and the images are the ones tagged ${PIN}${RST}"
  fi
elif [ -f "$DIR/docker-compose.yml" ]; then
  # A re-run over a stack installed before versions were written: its
  # compose file is reused and .env is not written on a re-run, so it keeps
  # following latest until someone chooses a version.
  VERSION=""
  suggest=${WRITE_TAG:-<version>}
  WRITE_TAG=""
  warn "This stack has no BINTRAIL_TAG in .env, so it runs the newest images each time they are downloaded. To stay on one version, add BINTRAIL_TAG=${suggest} to ${DIR}/.env (docs/upgrade.md)."
elif [ -n "$WRITE_TAG" ]; then
  VERSION="$WRITE_TAG"
  say "${DIM}    version ${WRITE_TAG}${RST}"
else
  VERSION=""
  say "${DIM}    DBTRAIL_REF=${DBTRAIL_REF} is not a release, so the images are the newest ones, tagged latest, and no version is written to .env${RST}"
fi

# ── 2. download the compose file into a self-contained directory ────────
step "Setting up the stack in ${B}${DIR}${RST}"
mkdir -p "$DIR"
cd "$DIR"

if [ -f docker-compose.yml ]; then
  warn "docker-compose.yml already exists here, so it was left alone (delete it to re-fetch)."
  warn "An existing file is never upgraded, and volumes and mounts can only come from it. If this is an upgrade, save your edits, delete the file, and re-run: docs/docker.md 'Upgrading the stack'."
  [ "$PORT" != "8090" ] && warn \
    "DBTRAIL_PORT=${PORT} ignored: the existing docker-compose.yml is reused (edit its ports: line by hand)."
  [ -n "${DBTRAIL_METRICS_PORT:-}" ] && warn \
    "DBTRAIL_METRICS_PORT=${DBTRAIL_METRICS_PORT} ignored: the existing docker-compose.yml is reused (edit its ports: line by hand)."
else
  FRESH=1
  if ! fetch "$COMPOSE_URL" docker-compose.yml; then
    rm -f docker-compose.yml
    [ -n "$REF_FROM_PIN" ] && die "BINTRAIL_TAG=${PIN} (in ${PIN_FROM}) names no DBTrail release: there is no ${REF}/docker-compose.yml.
    Check the version against the list of releases and change it, then re-run."
    die "Failed to download $COMPOSE_URL"
  fi
  say "${DIM}    downloaded docker-compose.yml${RST}"

  # The compose file publishes the console on 127.0.0.1:8090. If the operator
  # asked for a different host port, rewrite that one line in OUR freshly
  # downloaded copy (the container side stays 8090). Portable in-place edit
  # (BSD/GNU sed differ on -i), so write-and-move.
  # The console's startup banner address moves with it (#1784).
  if [ "$PORT" != "8090" ]; then
    sed -e "s|127.0.0.1:8090:8090|127.0.0.1:${PORT}:8090|" \
        -e "s|BINTRAIL_CONSOLE_URL: http://127.0.0.1:8090/|BINTRAIL_CONSOLE_URL: http://127.0.0.1:${PORT}/|" \
        docker-compose.yml > docker-compose.yml.tmp \
      && mv docker-compose.yml.tmp docker-compose.yml
    # sed exits 0 even when nothing matched — verify the rewrite actually landed
    # rather than print a false "port set" and bind the wrong port.
    grep -q "127.0.0.1:${PORT}:8090" docker-compose.yml || die \
      "Couldn't set the port to ${PORT}: the compose file's published-port
    line isn't what this installer expected. Edit the 'ports:' line in
    ${DIR}/docker-compose.yml by hand, or report it."
    # A compose file from before #1784 (an older DBTRAIL_REF) has no banner
    # address to move; only the banner in its logs is then off, so that is
    # not worth failing the install over.
    if grep -q "BINTRAIL_CONSOLE_URL:" docker-compose.yml; then
      grep -q "BINTRAIL_CONSOLE_URL: http://127.0.0.1:${PORT}/" docker-compose.yml || die \
        "Couldn't point DBTrail's startup banner at port ${PORT}: the
    BINTRAIL_CONSOLE_URL line in ${DIR}/docker-compose.yml isn't what this
    installer expected. Edit it by hand, or report it."
    fi
    say "${DIM}    DBTrail will answer on port ${PORT}${RST}"
  fi
  if [ -n "${SQLPORT_SKIP:-}" ]; then
    grep -v '"127.0.0.1:3309:3309"' docker-compose.yml > docker-compose.yml.tmp \
      && mv docker-compose.yml.tmp docker-compose.yml
    if grep -Eq '^[^#]*3309:3309' docker-compose.yml; then
      die "Port 3309 is in use on this machine and the MySQL port's published-port
    line in ${DIR}/docker-compose.yml isn't what this installer expected. Delete
    that 'ports:' line by hand, or report it."
    fi
    say "${DIM}    port 3309 is taken, so the MySQL port is not published (add a 'ports:' line to docker-compose.yml to use it)${RST}"
  fi
  # Same rewrite for the metrics mapping, verified the same way.
  if [ "$MPORT" != "9090" ]; then
    sed "s|127.0.0.1:9090:9090|127.0.0.1:${MPORT}:9090|" docker-compose.yml > docker-compose.yml.tmp \
      && mv docker-compose.yml.tmp docker-compose.yml
    grep -q "127.0.0.1:${MPORT}:9090" docker-compose.yml || die \
      "Couldn't set the metrics port to ${MPORT}: the compose file's published-port
    line isn't what this installer expected. Edit the 'ports:' line in
    ${DIR}/docker-compose.yml by hand, or report it."
    if [ -n "${MPORT_MOVED:-}" ]; then
      say "${DIM}    port 9090 is taken, so metrics are on ${MPORT}${RST}"
    else
      say "${DIM}    metrics port set to ${MPORT}${RST}"
    fi
  fi
fi

# Whether freshly downloaded or reused, make sure it's actually a compose file
# before handing it to Docker: a captive portal / proxy can return HTTP 200 with
# an HTML body, and a prior run interrupted mid-download leaves a truncated file.
# Either way `up -d` would emit an opaque YAML error; fail with a clear cause.
grep -q '^services:' docker-compose.yml || die \
  "${DIR}/docker-compose.yml doesn't look like a compose file (truncated download,
    or a network proxy/captive portal returned something else). Delete it and re-run."

# The version this install runs, written down (see "which version" above).
# Only a fresh install writes, and only a compose file that reads the value
# (every one since the images split; an older DBTRAIL_REF may not): a line
# nothing reads would claim a version the stack does not run.
# compose_reads_tag: every DBTrail image: line (comments aside) takes its tag
# from BINTRAIL_TAG. The file's own comments name the variable, so a bare grep
# would pass a file edited to `image: ...:latest` or a fixed number.
compose_reads_tag() {
  grep -Eq '^[^#]*image:[[:space:]]*ghcr\.io/dbtrail/' docker-compose.yml || return 1
  ! grep -E '^[^#]*image:[[:space:]]*ghcr\.io/dbtrail/' docker-compose.yml | grep -vq 'BINTRAIL_TAG'
}
if ! compose_reads_tag; then
  # Whether downloaded or reused: a version it does not read is not the one
  # it runs, so none is written or claimed.
  if [ -n "$VERSION" ] || [ -n "$WRITE_TAG" ]; then
    say "${DIM}    this docker-compose.yml does not take its image versions from BINTRAIL_TAG, so they are the ones its image: lines name${RST}"
  fi
  VERSION=""
  WRITE_TAG=""
elif [ -n "$WRITE_TAG" ] && [ -n "${FRESH:-}" ]; then
  if [ -s .env ] && [ -n "$(tail -c 1 .env)" ]; then printf '\n' >> .env; fi
  printf 'BINTRAIL_TAG=%s\n' "$WRITE_TAG" >> .env || die "Couldn't write ${DIR}/.env."
  say "${DIM}    BINTRAIL_TAG=${WRITE_TAG} written to .env, so the images stay on this version${RST}"
fi

# ── memory for the MySQL that keeps the history ─────────────────────────
# InnoDB's buffer pool defaults to 128 MB on any machine, which caps capture
# at about 1,400 row changes per second on a busy server (#2141). The compose
# file passes INDEX_BUFFER_POOL to it; a fresh install writes that into .env
# as a quarter of the memory Docker reports (Docker Desktop on a Mac reports
# its VM, which is all the container can use), whole GB from 1G up, 128 MB
# steps below, at most 32G; under about 1 GB the default stays. A quarter,
# because the web interface's SQL engine and often the MySQL being followed
# share the machine. internal/doctor RecommendedBufferPool is the same rule
# and test/installer holds the two together.
#
# Only a fresh install writes: on a re-run a new line would change the
# MySQL's configuration and the `up -d` below would restart it in the middle
# of capture, so the line is printed instead. A compose file of version 6 or
# later (the `pool-auto:begin` block on index-mysql) works the same rule out
# itself when .env has no value, so a re-run over one has nothing to suggest,
# and neither does a run where Docker did not report its memory: the container
# reads its own. A fresh install still writes the value it worked out, so the
# size stays what the install said even if the machine's memory changes later. A value already in .env, in any
# form compose's .env reader takes (KEY=, KEY:, export KEY=, a bare KEY), is
# never touched. A compose file without the setting (an older DBTRAIL_REF)
# gets no line: it would claim a size nothing passes on. MySQL rounds the
# pool up to a multiple of its chunk size times instances, so the value it
# runs with is this one or a little above.
pool_setting() {
  case "$1" in ''|*[!0-9]*) return 1 ;; esac
  [ "${#1}" -le 18 ] || return 1
  q=$(( $1 / 1048576 / 4 ))            # a quarter, in MiB
  if [ "$q" -ge 1024 ]; then
    g=$(( q / 1024 )); [ "$g" -gt 32 ] && g=32
    printf '%sG' "$g"
  elif [ "$q" -ge 256 ]; then
    printf '%sM' "$(( q / 128 * 128 ))"
  else
    printf 'default'
  fi
}
env_has_pool() {
  [ -f .env ] && grep -Eq '^[[:space:]]*(export[[:space:]]+)?INDEX_BUFFER_POOL[[:space:]]*([=:]|$)' .env
}
mem=$(docker info --format '{{.MemTotal}}' 2>/dev/null) || mem=""
[ "$mem" = 0 ] && mem=""
pool=$(pool_setting "$mem") || pool=""
if [ -n "${INDEX_BUFFER_POOL+set}" ]; then
  say "${DIM}    INDEX_BUFFER_POOL is set in this shell, so .env was left alone. The value lasts for this run only: put it in .env to keep it${RST}"
elif env_has_pool; then
  say "${DIM}    INDEX_BUFFER_POOL is already in .env, so it was left as you set it${RST}"
elif [ "$pool" = default ]; then
  say "${DIM}    this machine is small, so DBTrail's MySQL keeps the 128 MB memory default${RST}"
elif [ -n "$pool" ] && [ -n "${FRESH:-}" ] && ! grep -q -- '--innodb-buffer-pool-size=' docker-compose.yml; then
  say "${DIM}    this docker-compose.yml does not pass INDEX_BUFFER_POOL on, so DBTrail's MySQL keeps the 128 MB memory default${RST}"
elif [ -n "$pool" ] && [ -n "${FRESH:-}" ]; then
  if [ -s .env ] && [ -n "$(tail -c 1 .env)" ]; then printf '\n' >> .env; fi
  printf 'INDEX_BUFFER_POOL=%s\n' "$pool" >> .env || die "Couldn't write ${DIR}/.env."
  say "${DIM}    DBTrail's MySQL gets ${pool} of memory, a quarter of what Docker has and at most 32G (INDEX_BUFFER_POOL in .env)${RST}"
elif grep -q 'pool-auto:begin' docker-compose.yml; then
  say "${DIM}    DBTrail's MySQL works out its memory each time it starts, a quarter of what this machine has and at most 32G (INDEX_BUFFER_POOL in .env overrides it)${RST}"
elif [ -n "$pool" ]; then
  warn "DBTrail's MySQL may still run with the 128 MB memory default, which slows capture on a busy server."
  if grep -q -- '--innodb-buffer-pool-size=' docker-compose.yml; then
    warn "To give it ${pool}, add INDEX_BUFFER_POOL=${pool} to ${DIR}/.env and run: ${COMPOSE} up -d"
  else
    warn "To give it ${pool}, take the current docker-compose.yml (docs/docker.md 'Upgrading the stack'), then add INDEX_BUFFER_POOL=${pool} to ${DIR}/.env and run: ${COMPOSE} up -d"
  fi
else
  say "${DIM}    Docker did not say how much memory it has, so DBTrail's MySQL keeps the 128 MB memory default (set INDEX_BUFFER_POOL in .env to change it)${RST}"
fi

# The dbtrail command, beside docker-compose.yml. Fetched on every run: it is
# ours, holds nothing of the person's, and an older one may not know an option
# the container has. Without it the install still works, from the browser.
CLI=""
# It comes from the same ref as the compose file. With no ref (a re-run whose
# release lookup failed) the one already here is kept rather than taking main.
if [ -n "$REF" ] && fetch "$CLI_URL" dbtrail.new 2>/dev/null && [ -s dbtrail.new ] && head -n 1 dbtrail.new | grep -q '^#!/bin/sh'; then
  mv dbtrail.new dbtrail && chmod +x dbtrail && CLI=1
fi
rm -f dbtrail.new
if [ -z "$CLI" ] && [ -x dbtrail ]; then
  CLI=1
  if [ -z "$REF" ]; then
    warn "kept the dbtrail command already here, since the version to take a new one from is not known."
  else
    warn "could not download the dbtrail command of ${REF}; kept the one already here."
  fi
fi
[ -n "$CLI" ] || warn "could not download the dbtrail command; DBTrail works without it, from the browser."

# ── 3. bring it up ──────────────────────────────────────────────────────
step "Downloading the images (the first run can take a minute)"
# `up -d` starts whatever image this machine already has under the tag, so a
# machine that pulled "latest" once would start that old version for good.
# A failed pull (offline, a registry hiccup) is not fatal: the images already
# here still run, and the message says so.
$COMPOSE pull || warn "could not download the images; starting with the ones already on this machine, which may be a different version."

step "Starting containers"
$COMPOSE up -d || die "\`$COMPOSE up -d\` failed. Check the output above.
    Says \"invalid IP address in add-host\"? Your engine does not understand
    host-gateway: put HOST_GATEWAY=<this machine's address> in ${DIR}/.env and re-run."

# ── 4. wait for the console to actually answer ──────────────────────────
# `up -d` already blocks until the bundled index MySQL is healthy (the bintrail
# service has `depends_on: condition: service_healthy`, ~30-60s on a cold start),
# so by the time it returns the index is up. We still poll the unauthenticated
# liveness endpoint to wait out the short gap before the console process binds
# its HTTP listener — so we never print "ready" before the URL actually answers.
step "Waiting for DBTrail to answer"
ready=""
i=0
while [ "$i" -lt 90 ]; do
  if command -v curl >/dev/null 2>&1; then
    curl -fsS -o /dev/null "$HEALTH_URL" 2>/dev/null && { ready=1; break; }
  else
    wget -qO /dev/null "$HEALTH_URL" 2>/dev/null && { ready=1; break; }
  fi
  i=$((i + 1))
  printf '%s    ...still starting (%ss)%s\r' "$DIM" "$((i * 2))" "$RST"
  sleep 2
done
printf '\r%*s\r' 40 ''   # clear the progress line

if [ -z "$ready" ]; then
  # `up -d` returns 0 even if a container then crash-loops, so "no answer" can
  # mean still-pulling OR genuinely broken. Point at both ps and logs, and exit
  # non-zero so an automated caller (`install.sh && …`) doesn't read this as success.
  warn "DBTrail didn't answer at ${CONSOLE_URL} within ~3 minutes."
  say  "It may still be pulling images, or a container may have failed. Check both:"
  say  "    ${B}cd ${DIR} && ${COMPOSE} ps${RST}"
  say  "    ${B}cd ${DIR} && ${COMPOSE} logs -f bintrail${RST}"
  say  "Once DBTrail answers, open ${CONSOLE_URL}"
  exit 1
fi

# ── 5. next steps — the whole point of this script ──────────────────────
# The same five steps as the start page, ending on the heavy query (#1807,
# #2637), and only what the release this installs does: in it the MySQL port
# starts off, and step 4 still reads right once it starts on, since the button
# is pressed only when the port is off. No port number is said: 3309 can be
# taken, and then it is not published. The sentences a person reads here follow the
# first run's closed word list (test/installer reads it from the walk's
# scoreboard); commands may say anything, since they are copied, not read.
say ""
fg 14 170 110 32; printf '%s✓ DBTrail is up.%s\n' "$B" "$RST"   # green check
say ""
say "Have at hand: the ${B}host and port${RST} of your MySQL server,"
say "and ${B}a MySQL login that can create users${RST} and grant them privileges."
say ""
say "${B}Next steps${RST}"
say "  ${B}1. Sign in.${RST} Open ${B}${CONSOLE_URL}${RST} and create a username and password."
say "  ${B}2. Connect.${RST} Click + Add server, fill in"
say "     the host and port of your MySQL server and press Find it. The form then"
say "     shows the SQL that creates a user for DBTrail: run it on your MySQL with"
say "     that login, then press I ran it. DBTrail checks the rest by itself."
say "     Your MySQL runs on this same machine? Use host ${B}host.docker.internal${RST}"
say "     (on Linux, that MySQL must listen on more than 127.0.0.1)."
say "  ${B}3. First copy.${RST} On Snapshots, press Read database now. DBTrail reads every"
say "     table once. Writes may wait while it starts, so pick a quiet time."
say "  ${B}4. MySQL port.${RST} On Settings, MySQL clients, press Turn on if the port is off."
say "     Copy the mysql line it shows, and keep the password: it is shown only once."
if [ -n "${SQLPORT_SKIP:-}" ]; then
  say "     Port 3309 was taken, so this machine does not publish it yet. In the router's"
  say "     'ports:' line, publish another free port, such as 127.0.0.1:3310:3309,"
  say "     then use that number after -P in the mysql line."
fi
say "  ${B}5. Heavy query.${RST} Run that mysql line in a terminal, then a total or a ranking"
say "     over one of your big tables. DBTrail answers it from the copy, without"
say "     slowing down your MySQL. The start page below has one to try."
say ""
if [ -n "$CLI" ]; then
  say "${B}Or from this terminal${RST}, with no browser. The same steps as commands:"
  say "  ${DIR}/dbtrail server add -h <host> -u dbtrail -p   ${DIM}# connect your MySQL${RST}"
  say "  ${DIR}/dbtrail snapshot run <name>                  ${DIM}# first copy${RST}"
  say "  ${DIR}/dbtrail port on                              ${DIM}# the MySQL port${RST}"
  say "  ${DIR}/dbtrail --help"
  say ""
fi
say "Your change history lives on this machine, in this stack's Docker volumes,"
say "with your login and your saved servers. Back them up. To keep the history on"
say "a MySQL server of your own instead, set INDEX_DSN in ${DIR}/.env."
say ""
say "From ${B}${DIR}${RST}:"
say "  ${COMPOSE} down                 ${DIM}# stop DBTrail; your history stays in the volumes${RST}"
say "  ${COMPOSE} up -d                ${DIM}# start it again${RST}"
say "  ${COMPOSE} logs -f bintrail     ${DIM}# see what it is doing${RST}"
if [ -n "$CLI" ]; then
  say "  ./dbtrail user set-password     ${DIM}# reset the login${RST}"
else
  say "  ${COMPOSE} exec -it bintrail bintrail-console user set-password  ${DIM}# reset the login${RST}"
fi
say "Adding -v to down deletes the volumes, and your history with them."
say ""
# Which version runs, and how to change it, in the words of docs/upgrade.md.
if [ -n "$VERSION" ]; then
  if [ "$PIN_FROM" = "this shell" ]; then
    say "This is version ${B}${VERSION}${RST}, set by BINTRAIL_TAG in this shell, for this run only."
  else
    say "This is version ${B}${VERSION}${RST}, set by BINTRAIL_TAG in ${DIR}/.env."
  fi
  say "To upgrade, set BINTRAIL_TAG to the new version, then run ${COMPOSE} pull && ${COMPOSE} up -d"
  say "(a release that changes docker-compose.yml needs its new file too: docs/upgrade.md)."
elif [ -n "$PIN" ]; then
  say "This stack runs the images tagged ${PIN}, as BINTRAIL_TAG says, not a version number."
  say "To stay on one version, set BINTRAIL_TAG to it in ${DIR}/.env (docs/upgrade.md)."
else
  say "This stack runs the newest images, tagged latest, and no version is set."
  say "To stay on one version, set BINTRAIL_TAG in ${DIR}/.env (docs/upgrade.md)."
fi
say ""
say "Every step in detail: ${B}${START_PAGE}${RST}"

# ── 6. best-effort: open the browser ────────────────────────────────────
if [ "${DBTRAIL_NO_OPEN:-}" != "1" ]; then
  if command -v open >/dev/null 2>&1; then
    open "$CONSOLE_URL" >/dev/null 2>&1 || true        # macOS
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$CONSOLE_URL" >/dev/null 2>&1 || true    # Linux desktop
  fi
fi
