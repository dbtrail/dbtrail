# DBTrail demo — 30-second evaluation

`ghcr.io/dbtrail/bintrail-demo` is a **single-container, evaluation-only**
demo: Percona Server 8.0 (a drop-in MySQL), DBTrail, ProxySQL, and a traffic
generator, preconfigured so your first Time Travel SQL query is one
`docker run` away.

```sh
docker run --rm -p 6033:6033 ghcr.io/dbtrail/bintrail-demo
```

Wait for the banner (~30 s boot), give the traffic generator a minute to build
history, then query the past:

```sh
mysql -h 127.0.0.1 -P 6033 -u demo -pdemo demo \
  -e "SELECT * FROM orders WHERE id = 1 AS OF '1 minute ago'"
```

Compare with the live row — the traffic generator mutates `orders id=1` every
few seconds, so the two always differ:

```sh
mysql -h 127.0.0.1 -P 6033 -u demo -pdemo demo \
  -e "SELECT * FROM orders WHERE id = 1"
```

> **Evaluation only.** Stateless by design — `docker stop` + `docker run` =
> fresh demo. No tuning, no persistence, no security hardening. For real
> deployments see [deployment.md](deployment.md) and
> [time-travel-sql.md](time-travel-sql.md).

> **Multi-arch.** The image is published for both `linux/amd64` and
> `linux/arm64`, so `docker run` works natively on Apple Silicon and ARM64
> hosts (AWS Graviton, etc.) — no emulation.

## What's inside

One container, five processes, supervised by the entrypoint (if any one dies,
the container exits — a half-working demo never lingers):

| Component | Role |
|---|---|
| Percona Server 8.0 | A drop-in MySQL (arm64-capable). Source **and** index in one instance: the `demo` schema takes writes; `bintrail_index` holds the row-event index. Binlog preconfigured (`ROW`, `FULL`, GTID). |
| `bintrail up` | Streams the `demo` schema's binlog into `bintrail_index` — the same one-command quickstart you'd run against your own MySQL. |
| `bintrail shim` | Answers `_flashback`/`_snapshot`/`_diff` time-travel queries over the MySQL wire protocol (loopback `:3308`). |
| ProxySQL | The front door on `:6033`. Routes time-travel shapes to the shim and everything else straight to MySQL — one connection, both worlds. |
| Traffic generator | Mixed INSERT/UPDATE/DELETE chaos on the `demo` schema, including a deterministic `orders id=1` mutation every cycle so the example query always has history. |

## Ports and credentials

| Port | What | Credentials |
|---|---|---|
| `6033` | ProxySQL — Time Travel SQL entry point | `demo` / `demo` |
| `3306` | Raw MySQL (optional `-p 3306:3306`) | `demo` / `demo` (DML on `demo.*`), `root` (no password, in-container only) |

## Queries to try

The shim accepts these shapes through port 6033 (see
[time-travel-sql.md](time-travel-sql.md) for the full grammar):

```sql
-- Row state at a point in time (relative or absolute)
SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 1;
SELECT * FROM _flashback.orders AS OF '2026-06-05 14:00:00' WHERE id = 1;

-- Bare form on the real table name — AS OF must end the statement
SELECT * FROM orders WHERE id = 1 AS OF '5 minutes ago';

-- Optimizer-hint form: time-travel a real table name (ORM-friendly)
SELECT /*+ DBTRAIL_AT='5 minutes ago' */ * FROM orders WHERE id = 1;

-- Every change to a row between two instants, one row per event
SELECT * FROM _diff.orders BETWEEN '10 minutes ago' AND 'now' WHERE id = 1;

-- Full-table state at an instant (rows with binlog activity)
SELECT * FROM _flashback.orders AS OF '2 minutes ago';
```

Make your own history: the `demo` user can write, so UPDATE a row through port
6033, wait a few seconds, and flashback to before your change.

## Building locally

```sh
docker build -f demo/image/Dockerfile -t bintrail-demo .
docker run --rm -p 6033:6033 bintrail-demo
```

`demo/image/smoke-test.sh` builds, boots, and asserts the acceptance flow
(time-travel returns a previous `orders id=1` state distinct from the live
row). It needs Docker and ~3 minutes; it is not part of `go test ./...`.
CI runs this same script against each architecture's pushed image before
the demo image is tagged and signed in GHCR (the `smoke` job in
`.github/workflows/demo-image.yml`), so a published demo image is one
that has actually booted.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `Empty set` from an `AS OF` query | The timestamp predates the stream start (history only accumulates while the demo runs) or the row wasn't touched yet — wait a minute and retry. |
| `ERROR 1064` | The query shape isn't in the shim grammar — see the supported forms above (the hint and bare-AS-OF forms only support `SELECT *`, and bare `AS OF` must end the statement). The same query without time-travel syntax goes straight to MySQL. |
| `ERROR 1045` | Wrong credentials — port 6033 takes `demo` / `demo`. |
| Container exits during boot | A component failed; `docker logs` shows which. The demo dies loudly rather than half-working. |
