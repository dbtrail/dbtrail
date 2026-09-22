# Installing DBTrail

Every way to install and first-run DBTrail, from the zero-friction Docker
Compose stack to building from source. If you just want the fastest path,
it's the first section — the same four lines as the README.

> **Naming note:** the project is **DBTrail**; the binaries, packages, and
> images keep the original engine name **`bintrail`** (`bintrail`,
> `bintrail-console`, `ghcr.io/dbtrail/bintrail`, `BINTRAIL_*` env vars).
> Existing installs, scripts, and services stay valid as-is.

## Requirements

- A **source MySQL 8.0+** with `binlog_format = ROW` and
  `binlog_row_image = FULL`. Don't guess: `bintrail doctor` checks everything
  and prints copy-pasteable remediation for whatever is missing.
- A MySQL user on the source with `REPLICATION SLAVE`, `REPLICATION CLIENT`,
  and `SELECT` — plus, if you want baselines (point-consistent by default),
  `RELOAD` (and `BACKUP_ADMIN` on MySQL/Percona 8.0+) and `SHOW VIEW`; see
  [streaming.md](./streaming.md#if-you-also-want-baselines-add-a-lock-privilege-and-show-view).
- An **index MySQL 8.0+** database for DBTrail's data (the Compose stack
  bundles one).
- **Other sources:** besides MySQL, DBTrail can also capture from **MariaDB**
  ([alpha](./mariadb.md) — 10.6+, 11.4 is the CI-tested target) and
  **PostgreSQL** ([GA](./postgres.md) — 14+). Both are first-class sources in
  the web console (**+ Add server** → pick the source type); PostgreSQL also
  ships a standalone `bintrail-pg` binary for headless/CLI deployments. Each has
  its own prerequisites; see the linked guide.
- Go 1.25+ (the module targets `go 1.25.11`) — only when building from source. The default `GOTOOLCHAIN=auto` fetches the right toolchain for you.

## Docker Compose (the bundled default)

One file, zero config — an index MySQL (persisted in a volume) and
`bintrail-console watch` in source-less daemon mode: the console plus the
control plane, waiting for you to add servers from the UI.

The shortest path is the install script — it does the two commands below
*and* waits for the console to actually answer before telling you where to
go next (and opens it in your browser when it can):

```sh
curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | sh
```

It drops the stack in `./dbtrail` (override with `DBTRAIL_DIR`). If port 8090
is already taken, it stops before touching anything and prints the command to
run instead, on a port it found free:

```sh
curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | DBTRAIL_PORT=8091 sh
```

The variables go on `sh`, after the pipe: that is the side that runs the
installer. `DBTRAIL_PORT` also points the console's startup banner (in
`docker compose logs`) at that port. If you change the `ports:` line of
`docker-compose.yml` yourself, change `BINTRAIL_CONSOLE_URL` beside it too. The stack also publishes Prometheus metrics on 9090, which is
Prometheus's own default port; when 9090 is taken the installer moves the
metrics to the next free port and says so. Choose one yourself with
`DBTRAIL_METRICS_PORT`. Prefer to drive Compose yourself?

```sh
curl -fsSLO https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml
docker compose up -d
docker compose logs -f bintrail
```

The logs print the console URL:

```
The DBTrail web interface is running: open it and add the MySQL servers to watch:

    http://127.0.0.1:8090/

First run: open the URL and create your username and password.
```

Open it and use **+ Add server** (the Servers screen opens itself on a
fresh install): pick the **source type** (MySQL, MariaDB, or PostgreSQL) and
paste the database to watch — host, user, password, optional schema filter
(a PostgreSQL source adds database/slot/publication fields, see
[postgres.md](./postgres.md)). DBTrail runs the preflight (failures come
back as remediation cards), provisions a dedicated index for that source, and
starts streaming.

A database on this same machine is `host.docker.internal` from inside Docker.
On Linux the compose maps that name with `host-gateway`, which needs Docker
Engine 20.10 or later running as root; with rootless Docker, or Podman before
4.7, set `HOST_GATEWAY` in `.env` to this machine's address. That database
must also listen on more than `127.0.0.1`, and the user in the grant the
add-server form shows (`'dbtrail'@'%'`) must be allowed from other hosts. Repeat per server; everything you add resumes automatically when
the container restarts.

Prefer to start streaming one source immediately at boot? Set `SOURCE_DSN`
in a `.env` next to the compose file — that's optional now, not required.

**The bundled index is a pinned MySQL 8.4** with a generated password — it
holds the forensic record, so **it is your system of record, not a throwaway:
back up its volumes** (`bintrail-index-data` + `bintrail-index-secret`
together; volume loss means re-indexing). DBTrail **ships** that MySQL but
does not **operate** it — disk, backups, and upgrades are yours, as is sizing
(see [Capacity Planning](./capacity.md)). The ship-vs-operate boundary triage
cites is [SUPPORT.md](./SUPPORT.md). See [docker.md](./docker.md) for the
credential mechanism and the `8.0→8.4` upgrade note.

**Bring your own index MySQL** (co-equal path, not an afterthought): set
`INDEX_DSN` in `.env` to a MySQL 8.0+ you operate, and remove the bundled
`index-init` + `index-mysql` services. Same split — DBTrail installs and
migrates only its schema on whatever server you point it at; the contract
floor stays MySQL 8.0+ (only the *bundled* index is 8.4). Want it operated for
you? That's the managed service at [dbtrail.com](https://dbtrail.com).

All the optional knobs (pinned console token, schema filter, image tag)
live in
[`.env.example`](https://github.com/dbtrail/dbtrail/blob/main/.env.example);
the full walkthrough is in [docker.md](./docker.md).

**Upgrading later takes three commands, not two.** `docker compose pull` and
`docker compose up -d` bring new images and run them against the compose file
you already have, which nothing updates for you. Volumes, mounts, ports and
profiles can only come from that file, so the binaries move forward and the
stack around them does not. Re-download it as part of every upgrade:

```sh
docker compose pull
curl -fsSL -o docker-compose.yml.new https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml
diff docker-compose.yml docker-compose.yml.new   # carry your edits into the new file
mv docker-compose.yml.new docker-compose.yml
docker compose up -d
```

The download lands beside your file, not on top of it, so any edits of your own
(a published port, an extra service) are still there to carry across.
Your `.env` and every data volume carry over untouched. The one to know
about: without the current file's console state paths, your console username
and password, the servers you added, and the AI connection token live inside
the container and are deleted the next time it is recreated, with nothing
saying so. [Upgrading the stack](./docker.md#upgrading-the-stack) lists what
else a stale file costs.

## Try it without installing anything (30 seconds)

Want to *feel* time-travel SQL before wiring anything up? The demo image
bundles MySQL, bintrail, ProxySQL, and a traffic generator in one
evaluation-only container:

```sh
docker run --rm -p 6033:6033 ghcr.io/dbtrail/bintrail-demo
```

Wait for the banner, give the traffic a minute to build history, then query a
row "as of" a minute ago over port 6033. The full walkthrough, credentials,
and more queries are in [demo.md](./demo.md). (Stateless, evaluation-only,
multi-arch — runs natively on amd64 and arm64, including Apple Silicon and
Graviton.)

## Docker image (without Compose)

```sh
docker pull ghcr.io/dbtrail/bintrail:latest
docker run --rm ghcr.io/dbtrail/bintrail:latest --version
```

Multi-arch (`linux/amd64` + `linux/arm64`), signed with cosign, with
per-architecture SPDX SBOMs attached to the image as cosign attestations. The image bundles both `bintrail` and `bintrail-mcp`; the
web console ships as its own image, `ghcr.io/dbtrail/bintrail-console`
(`serve` = read-only console, `watch` = stream + console daemon — what the
Compose stack runs). The PostgreSQL-source binary ships as its own image,
`ghcr.io/dbtrail/bintrail-pg`. See [docker.md](./docker.md) for signature verification,
`docker run` recipes, and the long-running stream container.

## Linux packages

`.deb` and `.rpm` for amd64 and arm64 are attached to every
[release](https://github.com/dbtrail/dbtrail/releases):

```sh
# Debian/Ubuntu
curl -fsSLO https://github.com/dbtrail/dbtrail/releases/latest/download/bintrail_VERSION_linux_amd64.deb
sudo dpkg -i bintrail_*_linux_amd64.deb

# RHEL/Fedora
sudo rpm -i bintrail_VERSION_linux_amd64.rpm
```

(Replace `VERSION` with the release version; `checksums.txt` is cosign-signed.
Each `.deb`/`.rpm`, like each tarball, has a syft SPDX SBOM sidecar —
`<artifact>.sbom.json` — attached to the release.)

### Verify a download

`checksums.txt` is signed keylessly with cosign; the signature is attached to
the release as a Sigstore **bundle** — `checksums.txt.sigstore.json`
(certificate, signature and transparency-log entry in one file). Verifying it
needs **cosign v3.0 or newer** (`cosign version`; on older cosign, `--bundle`
names a different legacy format and verification fails on a genuinely valid
artifact). With the bundle, `checksums.txt`, and your downloaded artifacts in
one directory:

```sh
cosign verify-blob \
  --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/dbtrail/dbtrail/\.github/workflows/release\.yaml@refs/tags/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt
sha256sum --check --ignore-missing checksums.txt   # macOS: shasum -a 256 --check
```

The first command proves `checksums.txt` was produced by this repository's
release workflow on a tag; the second proves your downloads match what the
release shipped. (Releases up to v0.53.0 attached the older
`checksums.txt.sig` + `checksums.txt.pem` pair instead — verify those with
`--signature`/`--certificate` in place of `--bundle`.) Container images are
signed separately — see [docker.md](./docker.md).

The `bintrail` package carries the core CLI + `bintrail-mcp`; the web console
is a separate `bintrail-console` package — install it only where an operator
wants the UI. PostgreSQL-source capture is a separate `bintrail-pg`
package — install it only on hosts that capture from PostgreSQL.

## Go install

```sh
go install github.com/dbtrail/dbtrail/cmd/bintrail@latest
```

Requires CGO (DBTrail embeds DuckDB for Parquet archive queries).

## Build from source

```sh
git clone https://github.com/dbtrail/dbtrail
cd dbtrail
go build ./cmd/bintrail
```

`make build` builds both `bintrail` and `bintrail-mcp` with version metadata;
`make build-console` builds the `bintrail-console` web-console binary;
`make build-pg` builds the `bintrail-pg` PostgreSQL-source binary.

> macOS binaries and a Homebrew tap are tracked in
> [#349](https://github.com/dbtrail/dbtrail/issues/349) — today the
> supported paths on macOS are Docker (works great on Apple Silicon) and
> `go install`/source builds.

## First run with the binary

Two commands from zero to streaming:

```sh
# 1. Verify prerequisites and get copy-pasteable remediation for anything missing
bintrail doctor \
  --source-dsn "user:pass@tcp(source:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"

# 2. Initialize and start streaming (idempotent — safe to re-run)
bintrail up \
  --source-dsn "user:pass@tcp(source:3306)/" \
  --index-dsn  "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

`bintrail up` runs preflight + creates index tables + auto-snapshots + starts
streaming, all in one. It resumes from the last checkpoint on restart and
auto-derives a unique `server-id` from your source DSN. Want the web UI in
the same process? Run `bintrail-console watch` (same flags) instead — it is
`up` plus the console and the multi-server control plane.

Once it's running, the [Quickstart](quickstart.md) covers querying the index
and generating reversal SQL (`bintrail query` / `bintrail recover`) with worked
examples.

> **Managed MySQL (RDS, Aurora, Cloud SQL)?** `bintrail up` connects over the
> replication protocol — no disk access to binlogs required. See
> [streaming.md](./streaming.md).

### Step-by-step setup

Prefer running each phase explicitly (e.g. to deploy init and stream on
separate hosts)? The underlying commands remain available:

```sh
# 1. Verify prereqs (same as `bintrail up` Phase 1)
bintrail doctor --source-dsn "$SRC" --index-dsn "$IDX"

# 2. Create index tables
bintrail init --index-dsn "$IDX"

# 3. Snapshot schema metadata
bintrail snapshot --source-dsn "$SRC" --index-dsn "$IDX"

# 4. Either: stream live (recommended)
bintrail stream --source-dsn "$SRC" --index-dsn "$IDX" --server-id 12345

# Or: index from binlog files on disk
bintrail index --binlog-dir /var/lib/mysql --source-dsn "$SRC" --index-dsn "$IDX" --all
```

For cron, systemd units, and Ansible recipes, see [deployment.md](./deployment.md).

## Every command

| Command | Description |
|---|---|
| `doctor` | Diagnose source MySQL prerequisites and emit copy-pasteable remediation |
| `up` | One command: preflight + init + stream (the friction-free quickstart) |
| `init` | Create index tables in the target MySQL database |
| `snapshot` | Capture table and column metadata from the source server |
| `index` | Parse binlog files from disk and write row events to the index |
| `stream` | Connect as a replica and index row events in real-time |
| `agent` | Connect to DBTrail and listen for commands |
| `query` | Search the index with flexible filters (schema, table, PK, time range, GTID) |
| `recover` | Generate reversal SQL for matching events |
| `recover-cascade` | Generate reversal SQL for rows hit by a foreign-key ON DELETE / ON UPDATE CASCADE / SET NULL |
| `reconstruct` | Rebuild row state at a point in time from baselines + binlog events |
| `verify` | Verify that a recovery would reproduce the source |
| `rotate` | Drop old partitions, add new ones, optionally archive to Parquet |
| `archive reconcile` | Re-sync archive_state with the Parquet files actually on disk / in S3 |
| `status` | Show indexed files, partition sizes, and event counts |
| `dump` | Invoke mydumper to create a logical dump of the source server |
| `baseline` | Convert mydumper output to Parquet snapshots |
| `upload` | Upload local Parquet files to S3 |
| `config init` | Generate a `.bintrail.env` configuration file |
| `init-shim` | Generate a `shim.yaml` for the time-travel SQL shim |
| `proxysql-config` | Generate ProxySQL setup SQL for time-travel SQL routing |
| `shim` | Run the in-process MySQL-protocol server for `_flashback`/`_diff`/`_snapshot` queries |
| `profile` | Manage RBAC access profiles for query and recover |
| `flag` | Label tables and columns (e.g. `pii`, `sensitive`) for access rules |
| `access` | Link flags to profiles with allow/deny permissions |
| `generate-key` | Generate an AES-256 encryption key for dump encryption |

All commands accept `--log-level` (default `info`) and `--log-format`
(default `text`). See each command's `--help` for flags and usage.

The web console lives in the separate `bintrail-console` binary —
`serve` (read-only UI over an index) and `watch` (stream + console +
control plane in one daemon). See [console.md](./console.md).
