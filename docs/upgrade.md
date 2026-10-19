# Upgrading DBTrail

How to move an existing install to a newer DBTrail version, whichever way you
installed it. The short version: **pull the new binaries/images, restart —
the index schema migrates itself.** The rest of this page covers what's
automatic, what to check first, and the handful of version-specific gotchas
worth knowing before you upgrade.

## 1. Check your current version

```sh
bintrail --version
bintrail-console --version
```

Both print the tag, commit, and build date (`-X main.Version=...` set at
build time by GoReleaser; `git describe` when built from source). Compare
against the [latest release](https://github.com/dbtrail/dbtrail/releases/latest).

## 2. Read the changelog before you upgrade

[CHANGELOG.md](CHANGELOG.md) documents every release, in order, newest
first. Skim every version **between** your current one and the target —
entries marked **BREAKING** call out a behavior change that needs a manual
step (a flag/env var rename, a default that flipped, an auth model change).
Recent examples: the 0.11.0 web interface auth rework (token → username+password),
the 0.8.x switch to rotation-on-by-default. If you're several versions
behind, read all of them — breaking changes don't repeat themselves in later
entries.

## 3. Docker Compose (the bundled default)

```sh
docker compose pull

# The compose file is yours and nothing updates it for you. Take the current
# one BESIDE it, carry your own edits across, then move it into place.
curl -fsSL -o docker-compose.yml.new https://raw.githubusercontent.com/dbtrail/dbtrail/main/docker-compose.yml
diff docker-compose.yml docker-compose.yml.new
mv docker-compose.yml.new docker-compose.yml

docker compose up -d
docker compose logs -f bintrail-console   # or `bintrail` on an older split-console file
```

`docker-compose.yml` pins images to `${BINTRAIL_TAG:-latest}` — with no
`BINTRAIL_TAG` set in `.env`, `pull` always grabs the newest release. Pin a
specific version instead (`BINTRAIL_TAG=v0.26.0` in `.env`) if you want
upgrades to be a deliberate, one-line edit rather than whatever `latest`
currently points to — useful once you're past initial eval and want
predictable upgrades.

**`docker compose pull` only updates images — it never touches
`docker-compose.yml` itself.** A release that adds a new service, env var, or
default (e.g. the `VERIFY_TRIGGER` default-on wiring in 0.26.0) only takes
effect if your copy of `docker-compose.yml` actually has that line — an
older file on disk keeps running with whatever it already had, silently,
with no error. If a newer feature seems missing or a documented default
doesn't match what you see, take the current `docker-compose.yml` with the
three lines above and diff it against your customized copy before `up -d`, or
add the specific new line by hand if you've heavily customized the file. The
`watch` daemon also reports the two gaps it can prove at startup (see
[Upgrading the stack](./docker.md#upgrading-the-stack) for the full list of
what a stale file costs).

Data volumes (`bintrail-index-data`, `bintrail-index-secret`,
`bintrail-state`, including the servers saved in the web interface) carry over unchanged
across a `pull && up -d` — only the images and `docker-compose.yml` itself
change. Two known transitions that need more than `pull`:

- **A compose file from before the web interface moved to its own image** (pre-#374) ran
  `bintrail up --console` from the single `ghcr.io/dbtrail/bintrail` image;
  that flag no longer exists. Take the current `docker-compose.yml` (the three
  lines above) rather than just `pull`-ing — see
  [docker.md](./docker.md#the-bundled-index-mysql-84) for the exact symptom
  (`unknown flag: --console`, crash-loop) and confirmation that volumes and
  `.env` survive the swap untouched.
- **The bundled index MySQL major-version bump (8.0 → 8.4)** is
  **non-downgradable** — an 8.4 server started on an 8.0 datadir auto-upgrades
  it irreversibly. This only bites if you're still on a pre-#418 compose file
  pointed at the old `index-mysql-data` volume name; a fresh install already
  uses the new volume name and isn't affected. See
  [docker.md's warning](./docker.md#the-bundled-index-mysql-84) for the
  mysqldump-forward escape hatch if you need to carry old data across
  deliberately.

## 4. Standalone binaries (deb/rpm/tarball)

There is **no apt/dnf repository** — `.deb`/`.rpm`/`.tar.gz` are downloadable
assets attached to each [GitHub release](https://github.com/dbtrail/dbtrail/releases),
not a continuously-updatable repo. Each upgrade is a manual fetch + install:

```sh
# .deb (bintrail core; bintrail-console and bintrail-pg are separate packages)
curl -fsSLO https://github.com/dbtrail/dbtrail/releases/download/vX.Y.Z/bintrail_X.Y.Z_linux_amd64.deb
sudo dpkg -i bintrail_X.Y.Z_linux_amd64.deb

# .rpm
curl -fsSLO https://github.com/dbtrail/dbtrail/releases/download/vX.Y.Z/bintrail_X.Y.Z_linux_amd64.rpm
sudo rpm -Uvh bintrail_X.Y.Z_linux_amd64.rpm

# tarball (any platform GoReleaser builds for)
curl -fsSLO https://github.com/dbtrail/dbtrail/releases/download/vX.Y.Z/bintrail_X.Y.Z_<os>_<arch>.tar.gz
tar xzf bintrail_X.Y.Z_*.tar.gz
sudo install bintrail /usr/local/bin/bintrail
```

Every release also ships `checksums.txt`, a cosign signature, and per-artifact
SPDX SBOMs (`*.sbom.json`, tarballs and `.deb`/`.rpm` alike) — verify
before installing on anything you don't trust the download channel for.
`bintrail-console` and `bintrail-pg` are separate `.deb`/`.rpm` packages and
separate tarball archives; upgrade each you have installed.

Restart whichever long-running process you have (`stream`, `agent`,
`bintrail-console watch`, a systemd unit, etc.) after replacing the binary —
nothing hot-swaps.

## 5. Building from source

```sh
git pull
make all        # builds bintrail, bintrail-mcp, bintrail-console, bintrail-pg (`make build` alone builds only bintrail)
```

Requires Go 1.25+ (`GOTOOLCHAIN=auto` fetches the right toolchain) and
`CGO_ENABLED=1` (DuckDB). Each target's `-ldflags` inject the version from
`git describe`, so `--version` reports something meaningful even off a
non-tagged commit.

## 6. What migrates automatically — and what doesn't

**Automatic:** the index schema. Every command that opens the index via a
CLI-typed `--index-dsn` (`init`, `index`, `stream`, `agent`, `query`,
`recover`, `rotate`, `bintrail-console serve`/`watch` against their own
`--index-dsn`) calls `EnsureSchema` on startup — including the read-only
commands (`query`, `recover`, `verify`, `reconstruct`, `shim`, the MCP tools) — it adds any columns/tables
introduced since your index was created, idempotently (checks
`information_schema` first, never re-runs a migration). No `bintrail init`
re-run needed; just start the new binary against the same index.

**Not automatic: the web interface's multi-server registry.** A server added
through the web interface's "+ Add server" form is **never** schema-migrated by the
daemon itself (only the CLI-typed boot DSN gets `EnsureSchema`; a registry
DSN is deliberately never `ALTER`'d by a read-mostly web process). If a
registry server's index predates a column the new version expects
(e.g. `connection_id`), the web interface shows an actionable 422 instead of a
raw SQL error, telling you to run a writer command against it once:

```sh
bintrail query --index-dsn "<that server's DSN>" --limit 1
```

`query` is read-only and needs nothing beyond `--index-dsn`, but still runs
`EnsureSchema` on startup like every other CLI-typed-DSN command — the
cheapest way to migrate an index without touching real data. (`status` is
the one exception: it deliberately does **not** migrate, so it can read a
pre-migration index's state — don't use it for this.) Practically: any index
a `stream`/`agent`/`index` process writes to picks up new columns
automatically on its own next run; the web interface only *reads* registry
servers, so a registry-only index needs this one manual touch after an
upgrade that adds a column. This is the same architectural boundary described in
[console.md](./console.md) (`connManager` never runs `EnsureSchema` on a
registry DSN).

### The event-type index (`idx_type_lookup`)

`binlog_events` has a fourth secondary index, `idx_type_lookup (event_type,
schema_name, table_name, event_timestamp)`, which makes a filter on the event
type (the console's Type filter, `query --event-type`) read only the rows of
that type instead of every row. A new index gets it from `bintrail init`. An
existing one does **not** get it from `EnsureSchema`: adding a column is
instant, building an index reads every row the index keeps, and `EnsureSchema`
runs at the start of every read command and of the console. It is built:

- **by the capture daemon** (`stream`, `bintrail-console watch`, `bintrail-pg
  stream`) when it starts, in the background, once per process. Capture starts
  at once and keeps writing during the build (`ALGORITHM=INPLACE, LOCK=NONE`:
  the server refuses rather than lock the table). The log says when it starts,
  on about how many rows, and how long it took: `building index
  idx_type_lookup on binlog_events ...` then `built index idx_type_lookup ...
  took=...`;
- **or by `bintrail init --index-dsn <the same DSN>`**, at once, if you prefer
  to choose the hour. `init` leaves the rest of an existing index as it is.

What to expect:

- **Time.** Measured at 25 s for 3 million events on a 2-vCPU MySQL 8.0 with
  the data mostly in memory, while capture kept inserting (slowest insert 114
  ms). An index much larger than its buffer pool reads from disk and takes
  longer; until it finishes, event-type filters work as before (slow), and
  nothing else changes.
- **Disk.** About 40 bytes per event the index keeps (5 to 6% on narrow rows),
  plus temporary space for the sort while it builds.
- **Rows written during the build.** MySQL keeps them in a log bounded by
  `innodb_online_alter_log_max_size` (128 MB by default). Each event costs
  it roughly 50 bytes (estimated from the 41-byte index entry, not measured
  at scale), so at 6 million events an hour it fills in about 25 minutes, and a build that outlasts it fails at the end with error 1799.
  Capture is not affected, the daemon logs the cause and does not try again
  until it restarts. On a large, busy index, raise that server variable for the
  upgrade, or run `bintrail init` at a quiet hour.
- **A daemon stopped mid-build.** The server finishes the build on its own
  (stopping the client does not stop the statement), and a daemon started in
  the meantime sees it running and does not start a second one. To stop it,
  `KILL` the thread id the log names.
- **Rotation during the build** cannot get the table: the online build holds
  a lock on `binlog_events` for its whole run (measured: adding and dropping
  partitions both failed every attempt while a build ran). Each rotation cycle
  meanwhile gives up after a few short tries and logs that `binlog_events` was
  in use, naming the `idx_type_lookup` build, its thread id, and how many hours
  the partitions ahead still cover; the next cycle after the build does the
  work. With the default three hourly partitions of headroom,
  a build shorter than about three hours never lets rows reach `p_future`.
- **A failed build never fails capture or `init`.** It is logged with its
  cause and fix, and event-type filters keep working without the index.

### The MySQL port moved to the router

Before, the capture process (`bintrail-console watch`) could open the MySQL
port itself. Now only the router (`bintrail-console router`) serves it, on
`3309`. What to do depends on how you run DBTrail:

- **Docker Compose:** take the current `docker-compose.yml` (step 3). Under an
  older file nothing answers on `3309`, and the capture process says so when
  it starts.
- **deb/rpm:** nothing. The package starts `bintrail-router` on the first
  upgrade that brings it. It listens on this machine only (`127.0.0.1:3309`);
  to answer other machines, set `BINTRAIL_ROUTER_LISTEN` in
  `/etc/bintrail/bintrail-console.env`.
- **Your own setup:** run `bintrail-console router` beside the capture
  process, with the same servers file, and remove `--flashback-listen`.

See [the upgrade notes in time-travel-sql.md](time-travel-sql.md) for the
settings that changed.

## 7. Upgrading a live `stream`/`watch` daemon

Binlog position (or GTID) is checkpointed to `stream_state` on the
ticker/shutdown interval, so a clean stop-upgrade-restart loses nothing:

1. Stop the process (`SIGTERM`/`docker compose stop` — both drain and
   checkpoint before exiting; avoid `SIGKILL`, which skips the final
   checkpoint and just replays a few extra events on restart instead of
   losing any).
2. Replace the binary or pull the new image.
3. Start it again with the same flags/env. It resumes from the last
   checkpoint automatically — no `--start-file`/`--start-gtid` needed on a
   resume, only on a true first run.

For the compose stack this is `docker compose pull && docker compose up -d`
(Compose stops-and-recreates the container, preserving the mounted volumes the
checkpoint lives in), plus the compose-file step in
[section 3](#3-docker-compose-the-bundled-default) if you have not taken it
yet.

## 8. Downgrading

Not supported. Schema migrations are additive and idempotent forward, but
there's no reverse migration — running an older binary against an
already-migrated (newer) index schema is untested and may misbehave in ways
that are hard to predict (a newer column an older binary doesn't know about
is simply ignored on read, which is usually fine, but any code path that
built column lists positionally rather than by name would not be). If you
need a rollback option, keep the previous binary/image tag around and pin
`BINTRAIL_TAG` (or your package version) rather than relying on `latest`.

## Getting help

[SUPPORT.md](./SUPPORT.md) is the canonical scope statement — the schema
and its migrations are in scope; operating the index server (disk, backups,
running the actual `apt`/`dnf`/`docker` upgrade commands, sizing) is the
operator's, per the ship-vs-operate boundary. `bintrail doctor` is a good
first step after any upgrade if something looks off — it re-checks the
source preflight and prints copy-pasteable remediation.
