# Docker Deployment

dbtrail ships a multi-stage Dockerfile that produces a minimal image containing both `bintrail` and `bintrail-mcp` binaries.

## Building the image

```bash
docker build -t bintrail .
```

Inject version metadata at build time:

```bash
docker build \
  --build-arg VERSION=$(git describe --tags --always) \
  --build-arg COMMIT=$(git rev-parse --short HEAD) \
  --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  -t bintrail .
```

### Multi-architecture builds

Build for both `amd64` and `arm64` using Docker Buildx:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t your-registry/bintrail:latest \
  --push .
```

> **Note:** CGO cross-compilation for `arm64` requires the `aarch64-linux-gnu-gcc` toolchain. Docker Buildx handles this automatically with QEMU emulation, though native compilation is faster.

## Pre-built images (GHCR)

Each tagged release publishes a multi-arch image (`linux/amd64` + `linux/arm64`) to GitHub Container Registry, so you don't need a Go toolchain or a local build:

```bash
docker pull ghcr.io/dbtrail/bintrail:latest          # core CLI + MCP server
docker pull ghcr.io/dbtrail/bintrail-console:latest  # web console (serve/watch)
docker pull ghcr.io/dbtrail/bintrail-pg:latest       # PostgreSQL-source capturer
docker pull ghcr.io/dbtrail/bintrail:v0.7.12         # a specific version
```

`bintrail-console` is its own image (and GHCR package — it needs the same
one-time public-visibility flip described below): a single binary with
entrypoint `bintrail-console`, used by the Compose quickstart to run `watch`.
The cosign verification below applies to it equally
(`cosign verify ghcr.io/dbtrail/bintrail-console:latest …`).

The images and the release checksums are signed with [cosign](https://github.com/sigstore/cosign) (keyless, via GitHub OIDC). Verify an image before running it:

```bash
cosign verify ghcr.io/dbtrail/bintrail:latest \
  --certificate-identity-regexp "https://github.com/dbtrail/dbtrail/.*" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

Every image also carries per-architecture SPDX SBOMs, attached as cosign
attestations under the same keyless OIDC identity — the input a CVE scanner
needs to answer "are we affected?" for the statically linked dependencies
(DuckDB, Arrow, the AWS SDK, …). Verify, and optionally extract, them:

```sh
# Verify the SBOM attestations (works against :latest, a version tag, or a digest)
cosign verify-attestation ghcr.io/dbtrail/bintrail:latest \
  --type spdxjson \
  --certificate-identity-regexp "https://github.com/dbtrail/dbtrail/.*" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"

# Extract the raw SPDX documents (one per architecture)
cosign download attestation ghcr.io/dbtrail/bintrail:latest \
  | jq -r '.payload | @base64d | fromjson | .predicate'
```

The same applies to `bintrail-console` and `bintrail-pg`. (You can also scan
an image directly — `syft ghcr.io/dbtrail/bintrail:latest` or `docker scout
sbom` — without trusting our attestation.) SBOM files for the release
tarballs **and** the `.deb`/`.rpm` packages (`<artifact>.sbom.json`) are
attached to each release on the GitHub Releases page.

The demo image (`bintrail-demo`) embeds its SBOM and provenance as BuildKit
attestations in the image index instead — read them with:

```sh
docker buildx imagetools inspect ghcr.io/dbtrail/bintrail-demo:latest \
  --format '{{ json .SBOM }}'
```

> **Just evaluating?** `ghcr.io/dbtrail/bintrail-demo` is a zero-setup,
> single-container demo (MySQL + dbtrail + ProxySQL + traffic generator,
> evaluation-only) — see [demo.md](demo.md). It is a separate GHCR
> package and needs the same one-time public-visibility flip described below.

> **Maintainer note (one-time):** the first release creates the GHCR package as **private**. Anonymous `docker pull` only works once the package is made public: in the org's *Packages → bintrail → Package settings*, set visibility to **Public** and link it to this repository. Until then the `pull` commands above require `docker login ghcr.io`.

## Running with docker run

### One-off commands

```bash
# Initialize the index database
docker run --rm bintrail init \
  --index-dsn "root:password@tcp(mysql-host:3306)/bintrail_index"

# Take a schema snapshot
docker run --rm bintrail snapshot \
  --source-dsn "bintrail:password@tcp(source-host:3306)/" \
  --index-dsn  "root:password@tcp(mysql-host:3306)/bintrail_index" \
  --schemas    "myapp"

# Query indexed events
docker run --rm bintrail query \
  --index-dsn "root:password@tcp(mysql-host:3306)/bintrail_index" \
  --schema myapp --table users --limit 10
```

### Long-running stream

```bash
docker run -d \
  --name bintrail-stream \
  --restart always \
  bintrail stream \
    --index-dsn  "root:password@tcp(mysql-host:3306)/bintrail_index" \
    --source-dsn "bintrail:password@tcp(source-host:3306)/" \
    --server-id  1234 \
    --schemas    "myapp" \
    --metrics-addr ":9090" \
    --log-format json
```

### Running the MCP server

The image includes `bintrail-mcp` for use with Claude Code or Claude Desktop:

```bash
docker run -d \
  --name bintrail-mcp \
  -p 8080:8080 \
  -e BINTRAIL_INDEX_DSN="root:password@tcp(mysql-host:3306)/bintrail_index" \
  --entrypoint bintrail-mcp \
  bintrail --http :8080
```

## Docker Compose

The `docker-compose.yml` at the repository root is the zero-friction setup:
an index MySQL (persisted in a named volume) plus `bintrail-console watch` —
preflight checks, index tables, automatic schema snapshot, the live binlog
stream, **and the web console**, in one `up -d`. It pulls the published
`ghcr.io/dbtrail/bintrail-console` image; no Go toolchain or local build
needed.

### Quick start

No clone, no config — the compose file is self-contained and the servers
to watch are added from the console UI afterwards:

```bash
curl -fsSLO https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml
docker compose up -d
```

Open **http://127.0.0.1:8090** — on first run the console serves a **"create
your console password"** screen. Set it once; every later visit is a normal
sign-in.

Optional knobs go in a `.env` next to the file: `SOURCE_DSN` to start
streaming one source immediately at boot, `INDEX_DSN` to bring your own index
MySQL, `CONSOLE_TOKEN` for an opt-in API-automation token (humans use the
password). (From a source checkout, `cp .env.example .env` gives you the
annotated template.)

Notes:

- `SOURCE_DSN` is the MySQL you want to watch. The user needs
  `REPLICATION SLAVE`, `REPLICATION CLIENT`, and `SELECT`, plus `LOCK TABLES`
  if you want baselines (point-consistent by default; on RDS/Aurora also set
  `BASELINE_LOCK_MODE=lock-all`). A MySQL on the
  same machine is reachable from inside Docker as `host.docker.internal`.
- The console is published on the **host loopback only** (`127.0.0.1:8090`),
  which is why first-run browser setup is allowed (the compose sets
  `BINTRAIL_CONSOLE_ALLOW_SETUP` because the container itself binds `0.0.0.0`).
  The setup screen self-disables the moment a password exists. To reach the
  console from another machine, **set the password from the host shell first**
  (`docker compose exec -it bintrail bintrail-console user set-password`) —
  until you do, the compose stack would serve the create-password screen on
  whatever you publish (a loud startup warning fires while it is open). Then
  change the port mapping to `"8090:8090"`, ideally behind TLS
  (`BINTRAIL_CONSOLE_TLS_CERT`/`_TLS_KEY` or a TLS-terminating proxy).
- The credential file lives at `/var/lib/bintrail/console-auth.yaml` in the
  `bintrail-state` volume. **Forgot the password?** Reset it with
  `docker compose exec -it bintrail bintrail-console user set-password`
  (overwrites, no data lost). There is deliberately no password environment
  variable — `docker inspect` must never print a credential.
- `bintrail-console watch` is idempotent: restarts resume the stream from
  its saved checkpoint. The preflight (`doctor`) failing prints
  copy-pasteable remediation in the logs and the container retries.
- Saved console connections (the Servers menu) persist in the
  `bintrail-state` volume.
- The managed MCP token (**Settings → Connect AI**) lives at
  `/var/lib/bintrail/console-mcp-token.yaml` in the same volume, so an AI
  client you connected keeps working across restarts. An older copy of this
  compose file kept it inside the container, where recreating the container
  wiped it: take the updated file (see
  [Upgrading the stack](#upgrading-the-stack)) and generate the token once
  more, and it survives from then on.
- `BINTRAIL_TAG` in `.env` pins the image version (default `latest`);
  building from a source checkout instead is a comment-toggle in the
  compose file (`build:` with `dockerfile: build/Dockerfile.bintrail-console`).

### Upgrading the stack

Your stack is two things: the images, and the compose file that wires them
together. `docker compose pull && docker compose up -d` upgrades the first and
never touches the second. All three steps:

```bash
# 1. the images
docker compose pull

# 2. the compose file, which nothing else updates for you. It downloads NEXT
#    to yours, never over it, so your own edits are still there to carry over
curl -fsSL -o docker-compose.yml.new https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml
diff docker-compose.yml docker-compose.yml.new     # carry your edits into the new file
mv docker-compose.yml.new docker-compose.yml

# 3. the containers, now running both
docker compose up -d
```

Step 2 is the one people miss. `docker-compose.yml` is yours: you downloaded it
once. Volumes, mounts, published ports and profiles can only come from that
file, so a newer image cannot add them, and the guarantees you get are the ones
your file wires up. The download lands on `docker-compose.yml.new` on purpose,
so whatever you changed in your own file (a published port, the `build:`
toggle, a service you added) is still in front of you when you carry it across.
Your `.env` and every data volume carry over untouched: taking the new compose
file moves no data.

What a stale compose file costs:

| What your file is missing | What it costs | How it looks |
|---|---|---|
| the console state paths on the `bintrail-state` volume | your console username and password, the servers you added, and the AI connection token live inside the container, so the next `up -d` that recreates it deletes them | **Silent.** The console comes back asking you to create a password, exactly like a fresh install, and reports no loss. Fix this one first |
| the read-only index mount plus `BINTRAIL_INDEX_DATADIR_RO` | free disk space for the index cannot be measured | The preflight and the Retention page report it as not measurable |
| the `iceberg-export` profile and its volume | there is no one-shot Iceberg export to run | `docker compose --profile iceberg-export run ...` says the service does not exist |
| `BINTRAIL_CONSOLE_SQL_PANEL` (the current file does not set it) | nothing: the SQL page was removed in 0.75.0 | Remove the variable. It is read for one release and warns; download a DuckDB schema from **Backups** and query the same Parquet yourself |

Two things make this easier to catch:

- **The daemon says it once.** When it can see that it is running in a
  container and that wiring it would use is not there, it prints one line per
  problem at startup, with the fix, in `docker compose logs bintrail`. It says
  nothing when the stack is wired, it never repeats, and it never guesses: a
  check that cannot establish a fact stays quiet rather than sending you after
  a problem you do not have. This is the `watch` daemon, which is what this
  stack runs; a read-only `bintrail-console serve` reports nothing about the
  stack around it. One ordering to know: the settings-folder case needs a file
  to be there to see, so right after the upgrade that deleted one it has
  nothing to report and only speaks up on a later start. The table above is
  what to check before the upgrade, not after.
- **The file carries a version.** `x-bintrail-compose-version` at the top of
  `docker-compose.yml` is passed to the console service, so a newer daemon can
  tell you your file is behind and name what is not in effect, instead of
  leaving you to diff two files. A file old enough to have no version number
  gets no such line.

### Metrics and the healthcheck

The `bintrail` service serves the watch daemon's Prometheus endpoint at
**http://127.0.0.1:9090/metrics** by default (host loopback only, same
posture as the console): per-source stream metrics — every metric carries a
`source` label — plus the `bintrail_index_*` gauges. The scrape config and
alerting rules in [Deployment §7 Observability](deployment.md#7-observability)
work against this endpoint as-is. A Prometheus running as a *container* on
the same compose network scrapes `bintrail:9090` and needs no published
port. Set `METRICS_ADDR=` (empty) in `.env` to disable the endpoint. It has
no authentication — widen the port mapping beyond loopback only behind a
firewall.

The service also carries a Docker **healthcheck** probing the console's
unauthenticated `GET /api/healthz`, so `docker compose ps` shows `(healthy)`
for a live daemon and `(unhealthy)` for one whose HTTP loop has died. Two
honest limits:

- Plain Docker/Compose **does not restart** an unhealthy container —
  `restart: unless-stopped` fires on process *exit* only. Swarm, Kubernetes,
  Podman's `--health-on-failure=restart`, or an autoheal sidecar are the
  pieces that act on health status; whether to run one is your call.
- A wedged capture *stream* behind a live HTTP loop passes this probe.
  Detect that with the stream-lag/error alerting rules on the metrics
  endpoint above, or `bintrail status --fail-on-gap` from cron.

### The bundled index MySQL 8.4

The bundled index is **MySQL 8.4 LTS**, pinned to an exact minor tag. The
container holds the binary; the data lives in a separate `bintrail-index-data`
volume — bumping a minor version is "swap the container, keep the volume"
(the PMM pattern). dbtrail **ships** this MySQL but does not **operate** it:
disk, backups, and upgrades are yours (for a managed, operated index, see
[dbtrail.com](https://dbtrail.com)). Support boundary: [SUPPORT.md](./SUPPORT.md).

**Credentials** — no static default password. On the first `up`, the
one-shot `index-init` service generates a random password into the
`bintrail-index-secret` volume (or takes one from `INDEX_MYSQL_ROOT_PASSWORD`
in `.env` if you set it *before* the first boot). Both the index MySQL and
dbtrail read it from there. The password is baked into the datadir at init,
so `bintrail-index-data` and `bintrail-index-secret` are a pair: back them up
together, and changing the password later means resetting both volumes.

**Troubleshooting** — if `docker compose up` never gets the console listening,
the index MySQL likely isn't healthy yet (the `bintrail` service waits for it
via `depends_on`, so its own log stays empty until then). Check the index
directly: `docker compose logs index-init index-mysql`. A "password" or
"healthcheck" error there usually means the `bintrail-index-secret` volume was
reset out of sync with `bintrail-index-data` — reset both together.

**Disk-capacity monitoring (#948)** — `doctor`'s capacity check ([Capacity
Planning](./capacity.md)) needs an OS-level `statfs` to report the index's
free disk space, but the bundled index runs in its own `index-mysql`
container, reachable from `bintrail` only over `tcp(index-mysql:3306)` — so
the usual loopback/hostname check can never fire there. The compose file
works around this with a **read-only bind mount of the same
`bintrail-index-data` volume** into the `bintrail` service
(`/var/lib/bintrail-index-ro`), plus a `BINTRAIL_INDEX_DATADIR_RO`
environment variable that the entrypoint script sets **only** in the branch
where it also builds the bundled `tcp(index-mysql:3306)` DSN — so the mount
is trusted only when it is actually backing the index in effect, never for a
BYO `INDEX_DSN`. You shouldn't need to touch this; it's mentioned here so the
mount doesn't look mysterious in `docker compose config`. If you bring your
own index (below), the whole mechanism is inert: your `INDEX_DSN` skips the
branch that sets the env var, and `doctor` falls back to the loopback/hostname
check and then to reporting free space as not measurable (a SKIP with
guidance, never a green PASS). That guidance names what is missing: the
read-only mount and `BINTRAIL_INDEX_DATADIR_RO`, when your index answers
somewhere this process can reach locally. For an index at another address it
suggests no mount at all, since one that is not your index's would report the
wrong volume's free space; and when the address is local but the server can't
be confirmed to be this machine (a port-forward or a tunnel looks exactly like
that), it says to point the variable at the index's own data directory and
nothing else, for the same reason.

On **Docker Desktop** (macOS/Windows), named volumes are typically backed by
one shared VM disk, so the free-space number this check reports is that VM
disk's headroom — shared with `bintrail-state` and any other volumes, not
space reserved exclusively for the index datadir. On native Linux Docker,
each named volume is a directory on the host filesystem, so free space is
the host filesystem's, which is the more precise reading. Either way it's
the same number `df` on the volume's backing storage would show you — this
check doesn't invent a more precise figure than the platform actually has.

**Upgrading from a pre-8.4 bundled index** — the old eval index used a
`mysql:8.0` container on the `index-mysql-data` volume. The new compose uses a
**new** `bintrail-index-data` volume on 8.4 and leaves the old one untouched,
so by default dbtrail simply re-indexes into the fresh 8.4 volume from the
source's binlogs (the bundled index was always "volume loss = re-index").

> ⚠️ **The 8.4 datadir is non-downgradable.** A MySQL 8.4 server started on an
> 8.0 datadir runs an in-place upgrade automatically and **irreversibly** —
> you cannot go back to 8.0 afterward. That is exactly why the new volume name
> is used instead of reusing `index-mysql-data`: your old 8.0 data stays
> recoverable. To carry the old data forward deliberately, `mysqldump` from the
> old `index-mysql-data` volume and reload into the new one (a logical
> dump/restore, not an in-place datadir upgrade), or point `INDEX_DSN` at a BYO
> index instead.

**Upgrading a compose stack from before the console split** — older
`docker-compose.yml` files ran `bintrail up --console` from the
`ghcr.io/dbtrail/bintrail` image. That flag no longer exists (the combined
daemon is now `bintrail-console watch`, in its own image), so a
`docker compose pull && up -d` on the OLD file crash-loops with
`unknown flag: --console`. The fix is to re-download `docker-compose.yml`
(see [Upgrading the stack](#upgrading-the-stack)) — image and command changed,
but your `.env` and all data volumes (`bintrail-index-data`,
`bintrail-index-secret`, `bintrail-state`, including saved console servers)
carry over unchanged.

### Backing up / restoring the bundled index

The bundled `index-mysql` container is your forensic system of record
(`binlog_events` with full before/after images). dbtrail **ships** this MySQL
but does not **operate** it — backups are yours ([SUPPORT.md](./SUPPORT.md)).
Two paths:

**Logical dump (online, no downtime).** `mysqldump` runs inside the container,
reading the generated root password from the `bintrail-index-secret` volume:

```bash
docker compose exec index-mysql sh -c \
  'mysqldump -uroot -p"$(cat /run/secret/index_password)" \
     --single-transaction --routines --all-databases' > index-backup.sql
```

Restore the same way (`-T` disables the pseudo-TTY so the redirected file is
read as stdin):

```bash
docker compose exec -T index-mysql sh -c \
  'mysql -uroot -p"$(cat /run/secret/index_password)"' < index-backup.sql
```

**Offline volume snapshot (crash-consistent).** Stop the writers first, then
copy the datadir and its secret in **one** archive. The index password is baked
into the datadir at first init (see the compose file header), so
`bintrail-index-data` and `bintrail-index-secret` must always be backed up and
restored **as a pair** — a datadir paired with a mismatched secret breaks
authentication:

```bash
docker compose stop bintrail index-mysql
docker run --rm --volumes-from "$(docker compose ps -aq index-mysql)" \
  -v "$PWD":/backup alpine \
  tar czf /backup/index-volumes.tgz /var/lib/mysql /run/secret
docker compose start bintrail index-mysql
```

To restore that archive, bring the stack down (`docker compose down` keeps the
named volumes), extract the tarball back into the two volumes while nothing is
writing, then `docker compose up -d` — never reload a datadir without its
matching secret.

**Verify** either restore with `bintrail status`. The core `bintrail` binary
lives in the core image (the console image omits it), and `index-mysql`
publishes no host port, so run it as a throwaway container that shares
`index-mysql`'s network and secret:

```bash
docker run --rm \
  --network "container:$(docker compose ps -q index-mysql)" \
  --volumes-from "$(docker compose ps -q index-mysql)" \
  --entrypoint sh ghcr.io/dbtrail/bintrail:latest -c \
  'bintrail status --index-dsn "root:$(cat /run/secret/index_password)@tcp(127.0.0.1:3306)/bintrail_index"'
```

A healthy index lists its servers, a recent stream position, and its partitions.

### Connecting to an external index MySQL (bring your own)

To run your own index instead of the bundled 8.4, set `INDEX_DSN` in `.env` to a MySQL 8.0+ you operate and
remove the bundled index from the compose file: delete the `index-init` and
`index-mysql` services, the `bintrail-index-data` / `bintrail-index-secret`
volumes, the `bintrail-index-secret` mount, the `bintrail-index-data`
read-only mount (`/var/lib/bintrail-index-ro`, used only by `doctor`'s
disk-capacity check against the *bundled* index — see above; harmless to
leave but pointless once you delete the volume it points at), and the
`depends_on: index-mysql` on the `bintrail` service (the opt-in `flashback`
and `iceberg-export` services carry the same mount and dependency, so do the
same there if you use those profiles). dbtrail installs only its schema on your server;
its sizing, backups, and upgrades are yours — see
[Capacity Planning](./capacity.md), [deployment.md](./deployment.md), and
[SUPPORT.md](./SUPPORT.md). (The BYO contract floor stays MySQL **8.0+** —
only the *bundled* index is 8.4.)

### Baselines and Time-travel (the `baseline` profile)

The console's **Time-travel** surface reconstructs complete rows (baseline
snapshot + binlog deltas), so it needs **baseline Parquet snapshots** — and the
console image deliberately ships without the `dump`/`baseline` commands. The
compose file includes an opt-in one-shot profile that produces them with zero
extra installs: the official `mydumper` image dumps the source over the
network into a transient volume, then the core `bintrail` CLI image converts
the dump to Parquet inside the `bintrail-state` volume.

Run it on demand (or from cron on the host):

```sh
# Single-source stack (SOURCE_DSN set in .env): no extra config needed.
docker compose --profile baseline run --rm baseline

# Any other source (e.g. one you added from the console UI):
BASELINE_SOURCE_DSN="repl:secret@tcp(db.example.com:3306)/" \
BASELINE_SCHEMAS="shop,billing" \
  docker compose --profile baseline run --rm baseline
```

Each run creates a new snapshot under
`/var/lib/bintrail/baselines/<timestamp>/<schema>/<table>.parquet` (in the
`bintrail-state` volume), with the source's binlog coordinates embedded so
reconstruct knows where deltas begin. Then point the console at it:

- **Servers added from the UI**: Backup settings (left nav) → the
  server's row → **Backup dir** = `/var/lib/bintrail/baselines` (a
  *container* path — the `watch` daemon reads it, not your host). The
  server's Time-travel tab lights up, and its row shows a TT chip under
  Manage servers.
- **The boot `SOURCE_DSN` entry**: set `BASELINE_DIR=/var/lib/bintrail/baselines`
  in `.env` and `docker compose up -d` again.

The console also has an in-process **Create baseline** button (in the sidebar
under Protect → Baselines, for the selected server) that runs the same
dump→convert→upload pipeline without the CLI profile — it's on by default in this compose stack;
set `BASELINE_TRIGGER=0` in `.env` to disable it. The button still needs a
source DSN and a baseline dir/S3 configured on the server before it does
anything.

**Protect → Verification** carries the verification runner (also on by default;
`VERIFY_TRIGGER=0` in `.env` to disable) that runs `bintrail verify` in-process
for the selected server — trigger a run, watch per-table match/mismatch/
inconclusive results land, and drill into a mismatch — see
[console.md](console.md#running-verification-from-the-console).

**SQL over your Parquet** is not answered by the daemon. The console's SQL page
was removed in 0.75.0 (see [The SQL panel
(removed)](console.md#the-sql-panel-removed)). Open **Backups**, click
**Download views.sql** on the **Download a DuckDB schema** card below the
snapshot listing, and run the file in your own DuckDB: no row cap, no time
limit, and nothing executing inside the process that captures.

Notes:

- The dump is **point-consistent** by default (`--sync-thread-lock-mode FTWRL
  --trx-tables`, available since mydumper 0.18 — the pinned image is recent)
  — same default as `bintrail dump`. FTWRL holds one global read lock only
  long enough for every worker to open its snapshot at the same instant, and
  needs `RELOAD`/`FLUSH_TABLES` (plus `BACKUP_ADMIN` on MySQL/Percona 8.0+).
  On **managed MySQL (RDS/Aurora) `BACKUP_ADMIN` cannot be granted**, so
  `ftwrl` cannot run there: set `BASELINE_LOCK_MODE=lock-all`, which is
  equally point-consistent and needs only `LOCK TABLES`. Otherwise
  `BASELINE_LOCK_MODE=safe-no-lock` dumps without either privilege (it
  aborts rather than write a torn snapshot, so expect it to refuse on a
  write-active source) or `=no-lock` accepts a torn one. It still reads every row of the selected
  schemas; schedule it off-peak for large sources. On sources with
  non-transactional tables (MyISAM), the binlog coordinates embedded in the
  snapshot may not exactly match those tables' data — the same caveat as
  `bintrail dump`.
- If the run fails with `service "baseline-dump" didn't complete
  successfully`, the cause (a mydumper error, a FATAL from the DSN/schema
  guards) is in `docker compose --profile baseline logs baseline-dump`.
- `BASELINE_SCHEMAS` defaults to `SCHEMAS`; empty dumps **all user schemas**
  (the system schemas `mysql`/`sys`/`performance_schema`/`information_schema`
  are always excluded — a least-privilege capture user can't read the `sys`
  views, and they're useless as a baseline). Set it to snapshot only specific
  schemas.
- Take a fresh baseline after `ALTER TABLE` (reconstruct needs the snapshot
  schema to match the deltas), and periodically so the binlog window between
  baseline and "now" stays short. Old snapshots are plain directories — prune
  them by deleting `<timestamp>` dirs in the volume.

### Iceberg tables from your backups (the `iceberg-export` profile)

Once you have baselines, the compose file can turn them into Apache Iceberg
tables that Spark, Trino, Athena, Snowflake and DuckDB read directly. It is
another opt-in one-shot, on the **core** image (the console image ships
without the export commands):

```bash
docker compose --profile iceberg-export run --rm iceberg-export
```

The first run loads the newest snapshot; every run after that adds only what
changed since, so bringing the tables forward is cheap enough to schedule.
Hourly from the host's crontab:

```
17 * * * * cd /path/to/stack && docker compose --profile iceberg-export run --rm iceberg-export >> /var/log/bintrail-iceberg.log 2>&1
```

Notes:

- It needs a baseline: run the `baseline` profile first, or set `BASELINE_S3`
  to a prefix that already holds snapshots. With neither, the service exits
  with a `FATAL` naming the directory it looked in.
- Tables are written to `<schema>/<table>/` under `WAREHOUSE_DIR` (default
  `/var/lib/bintrail-iceberg`) in the `bintrail-iceberg` volume. That volume
  is an **output**, not a system of record: it is rebuilt from the index and
  the baselines, so losing it costs a re-export and nothing else.
- The index DSN is the bundled one unless you set `INDEX_DSN`, exactly like
  the other services. That is worth knowing next to the console's **Keep it
  current with Iceberg** panel: this profile always runs against the stack's
  own index and backups, so if the server you picked there is a different one
  (its own `baseline_s3`, say), set `INDEX_DSN` and `BASELINE_DIR` /
  `BASELINE_S3` here to match, or run the command the panel prints instead.
  Without that, the run succeeds and exports the wrong tables.
- Set `ICEBERG_TABLES` to a comma-separated `schema.table` list to export only
  some tables (default: every table in the newest snapshot).
- A run that dies before committing leaves the previous copy in place and
  resumes from it. A run refuses to advance a table whose columns changed, or
  whose history has a gap, and says which; to load such a table again from a
  fresh baseline, remove its directory from the warehouse.
- It never runs inside the capture daemon. The export writes a new copy of the
  data, and keeping it out of `watch` is what makes sure a long export cannot
  slow capture down. See [Iceberg export](iceberg-export.md).

### Time-travel SQL (`AS OF`) and the compose stack

The console's Time-travel tab and time-travel **SQL** (`SELECT … AS OF`) are
different surfaces. `AS OF` SQL is answered by `bintrail shim`, an in-process
MySQL-protocol server (a subcommand of the **core** `bintrail` binary; the
console image deliberately omits it).

**The `flashback` profile — a dedicated time-travel terminal (no ProxySQL).**
The compose file ships an opt-in `shim` service: point a plain `mysql` client at
it and read historical row state. It uses the core image against the bundled
index. It serves the **boot `SOURCE_DSN` source** — the one the `bintrail`
service streams into `bintrail_index` — so it needs `SOURCE_DSN` set in `.env`
**and** the main stack running (the `bintrail` service creates the index tables
and streams; the shim only reads — it never provisions). Bring it up *with* the
full stack — note `up -d`, not `up -d shim`, so `bintrail` comes along:

```sh
# in .env: SOURCE_DSN=user:pass@tcp(your-db:3306)/yourdb
SHIM_USER=analyst SHIM_PASSWORD='pick-a-strong-one' \
  docker compose --profile flashback up -d

mysql -h 127.0.0.1 -P 3308 -u analyst -p
mysql> USE yourdb;
mysql> SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
mysql> SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00';   -- full table (needs a baseline)
mysql> SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND '2026-05-02' WHERE id = 12345;
```

This is a **dedicated** terminal for time-travel: a normal `SELECT * FROM orders`
on this connection returns `ER_NOT_SUPPORTED_YET` (1235). The shim reads the
index only (it never touches your source). The shim process binds `0.0.0.0`
inside the container — and logs a non-loopback warning, which is expected — while
the `127.0.0.1:3308:3308` port mapping restricts host exposure to loopback. Notes:

- **`_snapshot.*` (the full table as it was) needs a baseline.** Run the
  `baseline` profile (above), add `BASELINE_DIR=/var/lib/bintrail/baselines` to
  `.env`, and re-run `docker compose --profile flashback up -d`. Without it,
  `_flashback.*` returns only rows with binlog activity in the retained window
  (a *partial* table), and full-table `_snapshot` degrades to that behaviour.
- **Sources added from the console UI are not served by this shim.** They stream
  into their own per-source index database (`bintrail_idx_<id>`), not
  `bintrail_index`. To time-travel one of those, point `INDEX_DSN` at that
  database (one shim per source — see [time-travel-sql.md](./time-travel-sql.md)
  "Single source MySQL per shim").
- **Use a throwaway credential.** `SHIM_PASSWORD` is passed via the environment
  (visible to `docker inspect`, like `AWS_SECRET_ACCESS_KEY` here) and written to
  `/tmp/shim.yaml` in the container — make it a dedicated analyst login, not a
  reused production password.
- **MySQL 8.4 / driver clients**: the default auth plugin
  (`mysql_native_password`) works for the `mysql` CLI; set
  `SHIM_AUTH_METHOD=caching_sha2_password` for a driver that requires it.
- **Bring-your-own index**: like the `bintrail` service, the `shim` service
  mounts `bintrail-index-secret` and depends on `index-mysql` — adjust both if
  you set `INDEX_DSN` and remove the bundled index.

**Transparent routing (ProxySQL).** To let an application's *normal* connection
mix live queries and `AS OF` queries on the same endpoint, put ProxySQL in front
— it routes virtual-schema queries to the shim and everything else to your real
MySQL. That is the full walkthrough in
[time-travel-sql.md](./time-travel-sql.md); for a zero-setup taste of the SQL
surface, use the demo image ([demo.md](./demo.md)).

## Environment variables

| Variable | Used by | Description |
|----------|---------|-------------|
| `SOURCE_DSN` | compose (optional) | DSN for a source MySQL to start watching at boot (empty = add servers from the console UI) |
| `INDEX_DSN` | compose (optional) | Bring-your-own index MySQL (default: the bundled container) |
| `SCHEMAS` | compose (optional) | Comma-separated schemas to track (empty = all user schemas) |
| `CONSOLE_TOKEN` | compose (optional) | Opt-in static API-automation token (default: none — humans sign in with the console password) |
| `METRICS_ADDR` | compose (optional) | Container-side bind for the watch daemon's Prometheus `/metrics` (default `:9090`, published on the host loopback as `127.0.0.1:9090`; set empty to disable) |
| `INDEX_MYSQL_ROOT_PASSWORD` | compose (optional) | Pin the bundled index root password (set *before* first boot; default: randomly generated into the `bintrail-index-secret` volume) |
| `BINTRAIL_TAG` | compose (optional) | Image tag to run (default `latest`) |
| `BASELINE_SOURCE_DSN` | compose `baseline` profile | Source MySQL to snapshot (default: `SOURCE_DSN`) |
| `BASELINE_SCHEMAS` | compose `baseline` profile | Comma-separated schemas to snapshot (default: `SCHEMAS`; empty = all user schemas, system schemas excluded) |
| `BASELINE_DIR` | compose (optional) | Baseline dir for the boot `SOURCE_DSN` entry — set `/var/lib/bintrail/baselines` after the first `baseline` profile run to enable Time-travel on it. Also enables full-table `_snapshot.*` on the `flashback` profile shim |
| `BASELINE_TRIGGER` | compose (optional) | Enables the console's in-process **Create baseline** button (dump→convert→upload) for a monitored server; **on by default** — set `BASELINE_TRIGGER=0` to disable |
| `VERIFY_TRIGGER` | compose (optional) | Enables the console's Storage **Verification** panel (runs `bintrail verify` in-process) for a monitored server; **on by default** — set `VERIFY_TRIGGER=0` to disable |
| `BINTRAIL_CONSOLE_SQL_PANEL` | retired | The console's SQL page and `POST /api/sql` were removed in 0.75.0. Read for one release, and warns that it does nothing |
| `WAREHOUSE_DIR` | compose `iceberg-export` profile (optional) | Directory the Iceberg tables are written under (default `/var/lib/bintrail-iceberg`, in the `bintrail-iceberg` volume) |
| `BASELINE_S3` | compose `iceberg-export` profile (optional) | S3 prefix holding the baseline snapshots to export from; wins over `BASELINE_DIR` |
| `ICEBERG_TABLES` | compose `iceberg-export` profile (optional) | Comma-separated `schema.table` list to export (default: every table in the newest snapshot) |
| `SHIM_USER` | compose `flashback` profile | Login the time-travel `mysql` terminal authenticates with (required to start the `shim` service) |
| `SHIM_PASSWORD` | compose `flashback` profile | Cleartext password for `SHIM_USER` (required) |
| `SHIM_AUTH_METHOD` | compose `flashback` profile (optional) | Client auth plugin for the shim (default `mysql_native_password`; set `caching_sha2_password` for drivers that require it) |
| `BINTRAIL_INDEX_DSN` | bintrail-mcp | Index DSN for the MCP server |
| `AWS_ACCESS_KEY_ID` | compose (optional) | Static AWS credential for S3 (Archive to S3, baselines, and reading either back). Leave empty to rely on a mounted `~/.aws` or an EC2/ECS/EKS instance role instead — see below |
| `AWS_SECRET_ACCESS_KEY` | compose (optional) | Paired with `AWS_ACCESS_KEY_ID` above |
| `AWS_SESSION_TOKEN` | compose (optional) | Only needed for temporary/STS credentials |
| `AWS_REGION` | compose (optional) | Region for the S3 bucket(s) used by Archive to S3 / baselines |

(`SERVER_ID` is no longer needed — `bintrail-console watch` derives a stable
one from the source DSN.)

### S3 credentials (Archive to S3 / baselines)

Both **Archive to S3** (per-source, set from the console UI) and reading
**baseline** snapshots back from `s3://` go through the `bintrail` service's
ambient AWS credential chain — there's no per-source credential field. Set
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` /
`AWS_REGION` in `.env`, then `docker compose up -d` to recreate the container
with them (setting them in your host shell after the container is already
running does nothing — env vars only apply at container creation). Skip this
entirely if the host already provides credentials another way (a mounted
`~/.aws`, or an EC2/ECS/EKS instance role reachable from inside the
container) — the chain tries those too.

Missing/invalid credentials show up as:

- In `docker compose logs -f bintrail`: `duckdb: AWS credential chain
  resolved no usable credentials for S3 reads` (DuckDB, the console's Parquet
  query engine, couldn't resolve anything from the chain).
- In the console UI: `Could not list baselines: ... HTTP 403 Forbidden ...
  No credentials are provided`, or an equivalent 403 on Archive to S3 uploads.

Both point at the same root cause — no usable AWS credentials reached the
container — not an IAM permissions problem. Once credentials resolve, the IAM
principal still needs S3 permissions on the bucket: see [S3 IAM
Policy](s3-iam-policy.md) for a copy-paste policy covering Archive to S3,
baselines, and reading either back.

## Image details

- **Base**: `debian:bookworm-slim` (glibc required by DuckDB)
- **Binaries**: `/usr/local/bin/bintrail`, `/usr/local/bin/bintrail-mcp`
- **Entrypoint**: `bintrail` (pass subcommands as arguments)
- **No shell scripts or init systems** — the container runs a single binary

The `ghcr.io/dbtrail/bintrail-console` image follows the same contract with
one binary: entrypoint `bintrail-console`, uid 999 pinned (the compose secret
volume is chowned to it), `/var/lib/bintrail` pre-created for the server
registry. Build it from source with
`docker build -f build/Dockerfile.bintrail-console -t bintrail-console .`

### Why not Alpine?

dbtrail depends on DuckDB (`duckdb-go`) for querying Parquet archives. DuckDB's Go bindings include pre-compiled C libraries linked against glibc. Alpine uses musl libc, which is binary-incompatible and would cause runtime failures.

## Full demo

For a complete demo stack with traffic generation, Prometheus, and Grafana dashboards, see `demo/compose.yml` and `demo/README.md`.

## Usage telemetry

Official release images report metadata-only usage statistics (command name,
version, OS/arch, error class — never your data). Disable it for a stack in
`docker-compose.yml`:

```yaml
services:
  bintrail:
    environment:
      BINTRAIL_TELEMETRY: "off"     # or DO_NOT_TRACK=1
```

The **demo image** (`ghcr.io/dbtrail/bintrail-demo`) never reports, regardless
of configuration — it is hard-disabled in the image and asserted by its smoke
test. See [TELEMETRY.md](./TELEMETRY.md).
