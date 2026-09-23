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
#   DBTRAIL_REF      git ref for the compose file  (default: main)
#   DBTRAIL_PORT     host port the console answers  (default: 8090)
#   DBTRAIL_METRICS_PORT  host port for Prometheus /metrics (default: 9090,
#                    or the next free one when 9090 is taken)
#   DBTRAIL_NO_OPEN  set to 1 to NOT open a browser (default: opens best-effort)
#
# No root needed beyond whatever your Docker setup already requires.

set -eu

DIR="${DBTRAIL_DIR:-./dbtrail}"
REF="${DBTRAIL_REF:-main}"
PORT="${DBTRAIL_PORT:-8090}"
MPORT="${DBTRAIL_METRICS_PORT:-9090}"
COMPOSE_URL="https://raw.githubusercontent.com/dbtrail/dbtrail/${REF}/docker-compose.yml"
HEALTH_URL="http://127.0.0.1:${PORT}/api/healthz"
CONSOLE_URL="http://127.0.0.1:${PORT}"
START_PAGE="https://www.dbtrail.com/docs/quickstart/"
# The promise, word for word the same in this banner, the README and the start
# page (#1807); test/installer pins it.
PROMISE="DBTrail keeps every change on your MySQL server, before and after, and writes the SQL that undoes the ones you didn't want."

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
  "The Docker daemon isn't running. Start Docker and re-run this installer."

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
  envs="DBTRAIL_PORT=$1"
  [ "$DIR" != "./dbtrail" ] && envs="DBTRAIL_DIR=$DIR $envs"
  [ "$REF" != "main" ] && envs="DBTRAIL_REF=$REF $envs"
  printf 'curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/%s/install.sh | %s sh' "$REF" "$envs"
}

if [ ! -f "$DIR/docker-compose.yml" ]; then
  if port_in_use "$PORT"; then
    free=$(first_free_port 8091 8099 "$PORT") || free=8091
    die "Port ${PORT} is already in use on this machine, so the console can't use it.
    Run the installer with the console on port ${free} instead:
        $(rerun_cmd "$free")"
  fi
  # The stack also publishes Prometheus /metrics on 9090, which is also
  # Prometheus's own default port. Metrics are secondary, so a taken 9090 moves
  # them to the next free port instead of failing on Docker's raw bind error;
  # a port the operator chose explicitly is theirs and is only checked.
  if [ "$MPORT" = "$PORT" ] || port_in_use "$MPORT"; then
    if [ -n "${DBTRAIL_METRICS_PORT:-}" ]; then
      die "The metrics port ${MPORT} is already in use (or is the console's port).
    Pick another one with DBTRAIL_METRICS_PORT, or leave it unset to have one chosen."
    fi
    MPORT=$(first_free_port 9091 9099 "$PORT") || die \
      "Ports 9090 to 9099 are all in use, so there is nowhere to publish metrics.
    Choose one with DBTRAIL_METRICS_PORT."
    MPORT_MOVED=1
  fi
fi

# Need curl or wget to fetch the compose file.
if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -qO "$2" "$1"; }
else
  die "Need curl or wget to download the compose file."
fi
say "${DIM}    docker ✓   ${COMPOSE} ✓   Docker running ✓${RST}"

# ── 2. download the compose file into a self-contained directory ────────
step "Setting up the stack in ${B}${DIR}${RST}"
mkdir -p "$DIR"
cd "$DIR"

if [ -f docker-compose.yml ]; then
  warn "docker-compose.yml already exists here, so it was left alone (delete it to re-fetch)."
  warn "An existing file is never upgraded, and volumes and mounts can only come from it. If this is an upgrade, save your edits, delete the file, and re-run: docs/docker.md 'Upgrading the stack'."
  [ "$PORT" != "8090" ] && warn \
    "DBTRAIL_PORT=${PORT} ignored — reusing the existing docker-compose.yml (edit its ports: line by hand)."
  [ -n "${DBTRAIL_METRICS_PORT:-}" ] && warn \
    "DBTRAIL_METRICS_PORT=${DBTRAIL_METRICS_PORT} ignored: the existing docker-compose.yml is reused (edit its ports: line by hand)."
else
  fetch "$COMPOSE_URL" docker-compose.yml \
    || die "Failed to download $COMPOSE_URL"
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
      "Couldn't set the console port to ${PORT} — the compose file's published-port
    line isn't what this installer expected. Edit the 'ports:' line in
    ${DIR}/docker-compose.yml by hand, or report it."
    # A compose file from before #1784 (an older DBTRAIL_REF) has no banner
    # address to move; only the banner in its logs is then off, so that is
    # not worth failing the install over.
    if grep -q "BINTRAIL_CONSOLE_URL:" docker-compose.yml; then
      grep -q "BINTRAIL_CONSOLE_URL: http://127.0.0.1:${PORT}/" docker-compose.yml || die \
        "Couldn't point the console's startup banner at port ${PORT}: the
    BINTRAIL_CONSOLE_URL line in ${DIR}/docker-compose.yml isn't what this
    installer expected. Edit it by hand, or report it."
    fi
    say "${DIM}    console port set to ${PORT}${RST}"
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

# ── 3. bring it up ──────────────────────────────────────────────────────
step "Starting containers (the first run downloads images, which can take a minute)"
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
# The same four steps, in the same words, as the start page (#1807), and only
# what the merged code does. The sentences a person reads here follow the
# first run's closed word list (test/installer reads it from the walk's
# scoreboard); commands may say anything, since they are copied, not read.
say ""
fg 14 170 110 32; printf '%s✓ DBTrail is up.%s\n' "$B" "$RST"   # green check
say ""
say "Have at hand: the ${B}host and port${RST} of your MySQL server, and ${B}a MySQL login that can create users${RST}."
say ""
say "${B}Next steps${RST}"
say "  ${B}1. Sign in.${RST} Open ${B}${CONSOLE_URL}${RST} and create a username and password."
say "  ${B}2. Connect.${RST} Click ${B}+ Add server${RST} and fill in the host and port of your MySQL"
say "     server. The form suggests a user and password for DBTrail and shows the"
say "     SQL that creates that user: run it on your MySQL with the login that can"
say "     create users, then press Save."
say "     Your MySQL runs on this same machine? Use host ${B}host.docker.internal${RST}"
say "     (on Linux, that MySQL must listen on more than 127.0.0.1)."
say "  ${B}3. First change.${RST} Change a row on your MySQL. It shows on the Overview"
say "     within a minute, with an Undo that writes the SQL to reverse it."
say "  ${B}4. First snapshot.${RST} Take one on the Snapshots page. With it, DBTrail can"
say "     rebuild a whole table as it was at a past moment."
say ""
say "Your change history lives on this machine, in this stack's Docker volumes,"
say "with your login and your saved servers. Back them up. To keep the history on"
say "a MySQL server of your own instead, set INDEX_DSN in ${DIR}/.env."
say ""
say "From ${B}${DIR}${RST}:"
say "  ${COMPOSE} down                 ${DIM}# stop DBTrail; your history stays in the volumes${RST}"
say "  ${COMPOSE} up -d                ${DIM}# start it again${RST}"
say "  ${COMPOSE} logs -f bintrail     ${DIM}# see what it is doing${RST}"
say "  ${COMPOSE} exec -it bintrail bintrail-console user set-password  ${DIM}# reset the login${RST}"
say "Adding -v to down deletes the volumes, and your history with them."
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
