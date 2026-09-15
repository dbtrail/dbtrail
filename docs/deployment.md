# DBTrail Production Deployment Guide

This guide covers everything needed to run DBTrail in production: infrastructure sizing, network topology, deployment options, observability, and operational procedures.

## 1. Architecture Overview

```
┌──────────────┐    replication     ┌─────────────────┐
│ Source MySQL  │◄──────────────────│ bintrail stream  │
│ (binlogs)    │    protocol        │ (long-running)   │──── :9090 /metrics
└──────────────┘                    └────────┬─────────┘
                                             │ SQL writes
                                     ┌───────▼────────┐
                                     │  Index MySQL   │
                                     │ (binlog_events │
                                     │  partitioned)  │
                                     └────────────────┘
                                             ▲
┌──────────────┐   scrape :9090     ┌────────┴─────────┐
│  Prometheus  │◄───────────────────│ bintrail stream  │
└──────┬───────┘                    └─────────────────-┘
       │ query
┌──────▼───────┐
│   Grafana    │  ← dashboard provisioned from JSON
└──────────────┘
       │ alerts
┌──────▼───────┐
│ Alertmanager │  (optional)
└──────────────┘

On-demand (DBA workstation):
  bintrail query   ──► Index MySQL ──► stdout
  bintrail recover ──► Index MySQL ──► .sql file
  bintrail rotate  ──► Index MySQL ──► partition maintenance
```

### Component summary

| Component | Role | Always-on? |
|-----------|------|-----------|
| Source MySQL | Origin database being tracked | Yes (pre-existing) |
| Index MySQL | Stores `binlog_events`, `stream_state`, `schema_snapshots` | Yes |
| `bintrail stream` | Replication client; writes events to index | Yes |
| Prometheus | Scrapes `/metrics` from stream process | Yes |
| Grafana | Dashboards and alerting | Yes |
| Alertmanager | Routes alert notifications | Optional |
| `bintrail query` / `recover` | DBA tools; read from index | On-demand |
| `bintrail rotate` | Drops old partitions; adds future ones | Scheduled (hourly or daily cron) |

## 2. Source MySQL Requirements

### Version and configuration

- MySQL 8.0 or later
- `binlog_format = ROW` (required — DBTrail refuses to index non-ROW binlogs)
- `binlog_row_image = FULL` (required — DBTrail validates this on startup).
  This must be set **server-wide**, not just at the session level. The startup
  check reads the value on bintrail's own connection, but `binlog_row_image` is
  settable per-session, and bintrail can't see what other application sessions
  do. A session that runs `SET SESSION binlog_row_image = MINIMAL` writes partial
  images that DBTrail indexes as if complete: under `MINIMAL`, unchanged columns
  are absent and the after-image primary key is omitted, so `recover` emits NULLs
  for unchanged columns and its `WHERE` clause matches nothing. `NOBLOB` is
  likewise unsupported — it drops unchanged `BLOB`/`TEXT` columns, so a reversal
  can overwrite them with NULL. Keep every session on `FULL`.
- `binlog_row_value_options` must **not** include `PARTIAL_JSON`. With partial
  JSON updates enabled, an `UPDATE` logs only a JSON *diff*, not the full
  document, so there is no complete after-image to recover from.
- GTID mode strongly recommended for reliable resume after restarts:
  ```
  gtid_mode = ON
  enforce_gtid_consistency = ON
  ```

> These are source-configuration requirements you own. Data captured under a
> non-`FULL` row image or `PARTIAL_JSON` is **out of support** — see
> [SUPPORT.md](./SUPPORT.md).

### Replication user

`bintrail` connects to the source as a replication client. The required grants (`REPLICATION SLAVE`, `REPLICATION CLIENT`, plus `SELECT` for schema snapshots and preflight checks) and the minimal-permissions guidance are in [streaming.md → The Source MySQL User](streaming.md#the-source-mysql-user). Baselines need `RELOAD` (plus `BACKUP_ADMIN` on MySQL/Percona 8.0+) and `SHOW VIEW` on top of those, or `LOCK TABLES` and `SHOW VIEW` with the `lock-all` mode on managed MySQL — capture works without them, only the snapshot is refused.

### Managed MySQL (RDS / Aurora / Cloud SQL)

| Platform | Notes |
|----------|-------|
| AWS RDS MySQL 8.0 | Enable automated backups (required for binlog). Set `binlog_format=ROW`, `binlog_row_image=FULL` in parameter group. Use `rds_replication` role instead of `REPLICATION SLAVE`. |
| AWS Aurora MySQL | Same parameter group requirements. Binlog retention: `CALL mysql.rds_set_configuration('binlog retention hours', 168)`. |
| Google Cloud SQL | Enable binary logging in instance settings. Create user with `REPLICATION SLAVE` via IAM or native auth. |
| Azure Database for MySQL | Enable `binlog_row_image=FULL` in server parameters. Flexible Server supports replication. |

## 3. Index MySQL Requirements

### Version

MySQL 8.0 or later. The `binlog_events` table uses `RANGE (TO_SECONDS(...))` partitioning and generated stored columns — both require MySQL 8.0+.

### Separate server recommended

Run the index database on a separate MySQL instance from the source. This provides:
- **Failure isolation**: index failures don't affect the source
- **Write amplification**: DBTrail generates significant write traffic — separate I/O from application queries
- **Security**: index credentials don't grant access to application data

### Sizing

Budget roughly **1.2–1.6 KB per indexed event** (INSERT/DELETE ≈ 1.2 KB, UPDATE ≈ 1.6 KB — an UPDATE stores both row images); narrow rows ~0.9 KB, wide/update-heavy several times higher. The full model — the `daily_events × retain_days × avg_event_bytes` formula, measured per-event-type sizes, worked examples, retention/Parquet-tiering math, multi-source sizing, and monitoring — is in [Capacity Planning](capacity.md).

### InnoDB tuning

The **bundled** index (the `index-mysql` service in `docker-compose.yml`) already applies the write-throughput overrides below except `innodb_buffer_pool_size`, which is host-RAM dependent and left to you. The settings here are for a **BYO** index (`INDEX_DSN` pointing at your own MySQL):

```ini
[mysqld]
innodb_buffer_pool_size = 70%           # 50-70% of available RAM (the one you must size)
innodb_flush_log_at_trx_commit = 2      # acceptable for index (not source)
innodb_redo_log_capacity = 2G           # 8.4 default is 100M — small for bursty large-row writes
innodb_flush_method = O_DIRECT          # 8.4 still defaults to fsync
skip_log_bin                            # the index is a write-only sink (see below)
max_allowed_packet = 1G                 # full JSON row images per INSERT — 64M default rejects large events
sort_buffer_size = 4M                   # wide-row ORDER BY overflows the 256K default → Error 1038
```

`innodb_flush_log_at_trx_commit = 2` trades a tiny recovery window (up to 1s of events) for significantly better write throughput. Since DBTrail can replay from the binlog position in `stream_state`, this tradeoff is acceptable.

`skip_log_bin` disables the index server's **own** binary log. The index is a write-only sink — nothing replicates from it, and DBTrail reconstructs by re-streaming from the source, never from the index's binlog. Leaving it on writes a second full copy of every row image and (with the default `sync_binlog = 1`) fsyncs per commit, which cancels much of the `innodb_flush_log_at_trx_commit = 2` benefit. Only disable it on a dedicated index instance with `gtid_mode = OFF` (a GTID-enabled server rejects `skip_log_bin`).

> **`sync_binlog` on the *source* is a different, more fundamental concern than the index-server note above.** If the source runs with `sync_binlog` at anything other than `1`, an OS crash can drop already-committed transactions from the binlog's tail before bintrail's stream ever reads them — the data never reaches the binlog, so there is nothing for bintrail to capture, index, or later recover. This is not detectable at capture time; `bintrail doctor` WARNs when the source isn't `sync_binlog=1`, and `bintrail verify` is the only way to later notice the resulting gap (as a MISMATCH). It is a durability/throughput tradeoff the source operator makes deliberately in many high-write environments — bintrail can only make sure you know the tradeoff exists.

`max_allowed_packet` and `sort_buffer_size` matter specifically for the DBTrail index workload and are easy to miss: each `binlog_events` INSERT carries the full before/after JSON row images (base64-inflated ~1.33×), so a large row event can exceed MySQL's 64M default and be **rejected** (silent large-event loss, [#652](https://github.com/dbtrail/dbtrail/issues/652)); and an `ORDER BY` over wide rows overflows the 256K `sort_buffer_size` default with `Error 1038 (Out of sort memory)` ([#608](https://github.com/dbtrail/dbtrail/issues/608)). The bundled Compose index sets both already — a **BYO** index needs them set explicitly.

> **Note on MySQL 8.4 defaults:** `innodb_log_file_size` was replaced by `innodb_redo_log_capacity` in 8.0.30+. MySQL 8.4 LTS already ships several former hand-tunings as defaults (`innodb_io_capacity = 10000`, `innodb_log_buffer_size = 64M`, `innodb_flush_neighbors = 0`, change buffer and adaptive hash index off), so they no longer need setting.

## 4. Network Topology

### Required connections

| From | To | Port | Protocol |
|------|----|------|----------|
| `bintrail stream` | Source MySQL | 3306 | MySQL replication (COM_BINLOG_DUMP_GTID) |
| `bintrail stream` | Index MySQL | 3306 | Standard MySQL |
| Prometheus | `bintrail stream` | 9090 | HTTP GET /metrics |
| Grafana | Prometheus | 9090 | HTTP PromQL API |
| DBA workstation | Index MySQL | 3306 | Standard MySQL (for query/recover/rotate) |

### Firewall rules

```
# bintrail host outbound
TCP 3306 → source MySQL host
TCP 3306 → index MySQL host

# bintrail host inbound
TCP 9090 ← Prometheus host (metrics scrape)

# Prometheus host outbound
TCP 9090 → bintrail host

# Grafana host outbound
TCP 9090 → Prometheus host

# DBA workstation outbound
TCP 3306 → index MySQL host
```

The DBTrail metrics port (9090) does not need to be exposed to the public internet.

## 5. Deployment Options

### systemd (recommended)

Create `/etc/systemd/system/bintrail-stream.service`:

```ini
[Unit]
Description=Bintrail stream replication indexer
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=bintrail
Group=bintrail
EnvironmentFile=/etc/bintrail/stream.env
ExecStart=/usr/local/bin/bintrail stream \
    --index-dsn  "${INDEX_DSN}" \
    --source-dsn "${SOURCE_DSN}" \
    --server-id  "${SERVER_ID}" \
    --batch-size "${BATCH_SIZE:-500}" \
    --checkpoint "${CHECKPOINT:-10}" \
    --schemas    "${SCHEMAS}" \
    --metrics-addr "${METRICS_ADDR:-:9090}" \
    --log-format json
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=bintrail-stream

[Install]
WantedBy=multi-user.target
```

`/etc/bintrail/stream.env` (mode 0600, owned by root):

```bash
INDEX_DSN=bintrail:password@tcp(index-mysql:3306)/bintrail_index
SOURCE_DSN=bintrail:password@tcp(source-mysql:3306)/
SERVER_ID=1234
SCHEMAS=myapp,myapp_archive
METRICS_ADDR=:9090
```

```bash
systemctl daemon-reload
systemctl enable --now bintrail-stream
journalctl -u bintrail-stream -f
```

### Docker

[docker.md](docker.md) is the canonical home for the image, `docker run`, and the compose stack. For production, harden that base:
- Use Docker secrets or environment files instead of inline credentials
- Pin image versions (`FROM golang:1.25.11-bookworm` in your Dockerfile — the DuckDB Go bindings link glibc, so an Alpine/musl base breaks at runtime)
- Mount a named volume for any persistent state (the index MySQL is the real persistent state — DBTrail itself is stateless)
- Set resource limits (`mem_limit`, `cpus`) on the bintrail container

```yaml
services:
  bintrail:
    image: your-registry/bintrail:v1.2.3
    env_file: ./stream.env
    restart: always
    deploy:
      resources:
        limits:
          memory: 512M
```

### Kubernetes

Minimal Deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bintrail-stream
spec:
  replicas: 1   # must be 1 — multiple replicas would create duplicate events
  selector:
    matchLabels:
      app: bintrail-stream
  template:
    metadata:
      labels:
        app: bintrail-stream
    spec:
      containers:
        - name: bintrail
          image: your-registry/bintrail:v1.2.3
          args:
            - stream
            - --index-dsn=$(INDEX_DSN)
            - --source-dsn=$(SOURCE_DSN)
            - --server-id=$(SERVER_ID)
            - --metrics-addr=:9090
            - --log-format=json
          envFrom:
            - secretRef:
                name: bintrail-stream
          ports:
            - containerPort: 9090
              name: metrics
          resources:
            requests:
              memory: "128Mi"
              cpu: "100m"
            limits:
              memory: "512Mi"
```

For Prometheus Operator, add a `ServiceMonitor`:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: bintrail-stream
spec:
  selector:
    matchLabels:
      app: bintrail-stream
  endpoints:
    - port: metrics
      interval: 15s
```

> **Important:** Always run exactly one replica of `bintrail stream` per source MySQL. Multiple replicas would index duplicate events with different `server_id` values. If you need HA, use a leader-election sidecar or rely on the systemd/Kubernetes restart mechanism.


### When the host dies

This applies to `bintrail-console watch`, the daemon that captures and serves the console. (Standalone `bintrail stream` has no lock at all; see the Kubernetes note above.)

A **process** crash recovers on its own: the daemon restarts (systemd `Restart=`, compose `restart: unless-stopped`) and every stream resumes from its checkpoint in the index. Losing the **host** does not: DBTrail has no automatic failover today, and someone starts it on another machine by hand. What that takes, and what to plan for:

- **Keep the index off the host.** The index MySQL holds the capture state (events, checkpoints, archive and snapshot bookkeeping). On the same machine it is lost with it.
- **Move the console's files.** A new host needs the server registry (`console-servers.yaml`), console auth (`console-auth.yaml`) and the MCP token (`console-mcp-token.yaml`). The binary keeps them under `~/.config/bintrail/`; the compose stack keeps them under `/var/lib/bintrail` in the `bintrail-state` volume. On DBTrail EE the users, roles and access-rules stores stay in the console user's home directory (`~/.config/bintrail/`) even under compose, in a separate volume, so move that one too: without it every console user is gone. Without the registry the new host starts with no servers to monitor.
- **The previous snapshot can come from the bucket.** The new host still needs a local Backup dir configured, because updates are written there, but with a Backup S3 set it reads the previous snapshot from the bucket.
- **Source binlog retention decides what is lost.** The new host resumes from the checkpoint in the index. If the source purged those binlogs while nobody was capturing, capture continues from the oldest binlog the source still has, the changes in between are permanently lost, and the server shows **LOST POSITION** in the console. That badge stays across restarts until you press **Stop** and then **Start** on the server; it does not mean capture is still broken.
- **Make sure the old host is really off before starting the new one.** Servers added in the console are guarded by a MySQL lock on the index (`GET_LOCK`): a second daemon marks such a server `failed` ("another bintrail process is already monitoring this server") instead of capturing twice. The hover text on that badge says it retries; for this refusal it does not. Press **Start** on the server, or restart the daemon, once the first host is gone. The source passed with `--source-dsn` (compose `SOURCE_DSN`) takes no lock at all, so two hosts started with it both capture into the same index.
- **A dead host can hold that lock for hours.** The lock lives in the dead daemon's idle session on the index, and MySQL drops an idle session only after `wait_timeout` (28800 seconds by default) or its TCP keepalive. To take over sooner, list the lock holders on the index:

  ```sql
  SELECT m.OBJECT_NAME, t.PROCESSLIST_ID
  FROM performance_schema.metadata_locks m
  JOIN performance_schema.threads t ON t.THREAD_ID = m.OWNER_THREAD_ID
  WHERE m.OBJECT_TYPE = 'USER LEVEL LOCK'
    AND m.OBJECT_NAME LIKE 'bintrail\_monitor\_%';
  ```

  and `KILL <PROCESSLIST_ID>` (on RDS or Aurora, `CALL mysql.rds_kill(<PROCESSLIST_ID>)`). Run it as the account the daemon uses, or one with `CONNECTION_ADMIN`; any other account gets `ERROR 1095`. Only do this once the old host is powered off or cut off from both the source and the index: the daemon never checks the lock again after taking it, so a host that was only unreachable keeps capturing when it comes back, alongside the new one. Do not lower `wait_timeout` instead: the lock session is idle on a healthy daemon too, so MySQL would drop it and let a second daemon capture the same source.

Automatic failover (a lease the standby can take over, with writes from a stale holder rejected) is not built. The design discussion, including why the lock alone cannot do it, is in [#1648](https://github.com/dbtrail/dbtrail/issues/1648).

## 6. Initial Setup Procedure

Bring-up order (the [Quickstart](quickstart.md) shows `init`/`snapshot` with expected output):

1. `bintrail init` — provision the index database (**run once**; `--partitions 30` for 30 hourly partitions + `p_future`).
2. `bintrail snapshot` — capture schema metadata (**re-run after every schema change**).
3. Note the starting position (`SELECT @@global.gtid_executed` on the source) — optional, since `stream` auto-discovers the current head when `--start-gtid` is omitted.
4. Start `bintrail stream` — the systemd unit in [§5](#5-deployment-options) is the production form.
5. `bintrail status --index-dsn "$INDEX_DSN"` to verify.

After the first successful checkpoint, restart without `--start-gtid` — the position is persisted in `stream_state` and used automatically.

## 7. Observability

The metrics endpoint is opt-in for a standalone binary — pass
`--metrics-addr :9090` (env `BINTRAIL_METRICS_ADDR`) to `bintrail stream` or
`bintrail-console watch`. The [compose quickstart](docker.md#docker-compose)
serves it out of the box on the host loopback (`127.0.0.1:9090`; a Prometheus
container on the same compose network scrapes `bintrail:9090`), so the scrape
config and alerting rules below apply to it unchanged.

### Prometheus scrape config

```yaml
# /etc/prometheus/prometheus.yml
global:
  scrape_interval: 15s     # 15s for production (5s is demo only)
  evaluation_interval: 15s

scrape_configs:
  - job_name: bintrail-stream
    static_configs:
      - targets: ['bintrail-host:9090']
    # If on Kubernetes, use kubernetes_sd_configs instead
```

### Alerting rules

```yaml
# /etc/prometheus/rules/bintrail.yml
groups:
  - name: bintrail
    rules:
      # The recoverability alert. Every lag gauge below is moved only BY
      # TRAFFIC and freezes at its last value under an idle source — so a dead
      # stream reads identical to a healthy one. Dividing on the flush
      # timestamp is immune to that: it stops advancing either way.
      - alert: BintrailNothingBecomingRecoverable
        expr: time() - bintrail_stream_last_flush_timestamp_seconds > 300
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Bintrail has not committed a batch to the index in 5 minutes"
          description: "Changes since the last flush are NOT queryable and NOT recoverable."

      # Read→queryable, both ends on one clock (skew-free, sub-second).
      - alert: BintrailCommitLatencyHigh
        expr: histogram_quantile(0.99, rate(bintrail_stream_index_commit_latency_seconds_bucket[5m])) > 30
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Bintrail p99 read→queryable latency is over 30s"
          description: "The window in which a change exists but is not yet recoverable is widening."

      # RECEIVE-time: says the daemon is behind the source, NOT that data is
      # unrecoverable — it is set before batching.
      - alert: BintrailReplicationLagHigh
        expr: bintrail_stream_replication_lag_seconds > 60
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Bintrail replication lag is high"
          description: "Lag is {{ $value | humanizeDuration }} — stream may be falling behind."

      - alert: BintrailReplicationLagCritical
        expr: bintrail_stream_replication_lag_seconds > 300
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Bintrail replication lag is critical"
          description: "Lag is {{ $value | humanizeDuration }} — stream is severely behind or stalled."

      - alert: BintrailStreamErrors
        expr: rate(bintrail_stream_errors_total[1m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Bintrail stream is experiencing errors"
          description: "Error type {{ $labels.type }} — check logs for details."

      - alert: BintrailStreamDown
        expr: up{job="bintrail-stream"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Bintrail stream process is unreachable"
          description: "Prometheus cannot scrape bintrail metrics endpoint."
```

### Grafana dashboard

The demo ships a pre-built dashboard at `demo/grafana/dashboards/bintrail-stream.json`. To use it in your production Grafana:

1. Go to **Dashboards → Import**
2. Upload `bintrail-stream.json` or paste its contents
3. Select your Prometheus datasource
4. Adjust the replication lag thresholds (currently 5s/30s) to match your SLO

### Log aggregation

Run with `--log-format json` for structured output compatible with log aggregators:

```bash
# Loki / Promtail: configure to scrape from journald or container logs
journalctl -u bintrail-stream -o json | promtail --stdin ...

# CloudWatch Logs (ECS/EC2)
# Set awslogs log driver — structured JSON maps to CloudWatch Log Insights

# ELK / OpenSearch
# Filebeat with JSON input reads from the log file or journald
```

Key log fields emitted by bintrail commands:

| Field | Type | Description |
|-------|------|-------------|
| `level` | string | `info`, `warn`, `error` |
| `msg` | string | Log message |
| `files_processed` | int | (`index` complete) |
| `events_indexed` | int | (`index`/`stream` complete) |
| `results` | int | (`query` complete) |
| `statements` | int | (`recover` complete) |
| `duration_ms` | int | Operation duration |

## 8. Partition Rotation

Run `bintrail rotate` hourly (cron or systemd timer) so old partitions are dropped and future ones stay provisioned — otherwise the catch-all `p_future` accumulates every new event. The cron/timer setup, the `--retain`/`--add-future` semantics, and monitoring `p_future` via `bintrail status` are in [rotation-and-status.md → Automating Rotation](rotation-and-status.md#automating-rotation). (`bintrail up`/`bintrail-console watch` also run this loop built-in.)

### S3-compatible stores

Archives and baselines can live in MinIO, Wasabi, LocalStack or any other
S3-compatible store: set `BINTRAIL_S3_ENDPOINT` (and, for a store that only
serves virtual-hosted URLs, `BINTRAIL_S3_PATH_STYLE=false`) on every bintrail
process, including the console. The bundled Compose file passes both through.
Details and the full list of surfaces the endpoint covers are in
[upload.md → S3-compatible stores](upload.md#s3-compatible-stores-minio-wasabi-localstack).
When servers keep their buckets in different stores, the console sets the
endpoint, addressing style and region **per server** instead; see
[upload.md → A store per server](upload.md#a-store-per-server-from-the-console).

### S3 archive bucket: abort orphaned multipart uploads

Archive and baseline uploads stream through the AWS multipart Uploader, which
automatically splits objects larger than S3's 5 GiB single-PUT ceiling into
parts. An upload interrupted mid-stream (crash, network loss, `SIGKILL`) can
leave orphaned parts that you keep paying storage for but never see as a
completed object. Attach an `AbortIncompleteMultipartUpload` lifecycle rule to
the archive bucket to reap them automatically:

```json
{
  "Rules": [{
    "ID": "abort-incomplete-multipart",
    "Status": "Enabled",
    "Filter": {},
    "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
  }]
}
```

Additionally, grant `s3:AbortMultipartUpload` in the bucket IAM policy (see
[s3-iam-policy.md](s3-iam-policy.md)) so the SDK can abort a failed multipart
upload immediately when the process survives the failure; the lifecycle rule
remains the backstop for uploads the process didn't get to clean up.

## 9. Security

### Credential management

Never pass DSN credentials as CLI arguments — they appear in `ps aux` and process tables. Use:
- **bintrail env file**: `bintrail config init` generates a `.bintrail.env` template (mode 0600). Set `BINTRAIL_INDEX_DSN` and `BINTRAIL_SOURCE_DSN` there — all commands load it automatically. Use `--global` for `~/.config/bintrail/config.env`.
- **systemd**: `EnvironmentFile=/etc/bintrail/stream.env` (mode 0600, owned root:root)
- **Docker**: `env_file:` with a secrets-managed file, or Docker Swarm/BuildKit secrets
- **Kubernetes**: `envFrom.secretRef` pointing to a `Secret` object

### TLS for database connections

Append `?tls=true` (or `?tls=skip-verify` for self-signed certs in dev) to both DSNs:

```bash
INDEX_DSN="bintrail:password@tcp(index-mysql:3306)/bintrail_index?tls=true"
SOURCE_DSN="bintrail:password@tcp(source-mysql:3306)/?tls=true"
```

For richer TLS on the streaming daemon (`bintrail stream`/`up`/`bintrail-console watch`) — CA verification or mutual TLS on **both** the source and index connections — use the dedicated `--ssl-mode`/`--ssl-ca`/`--ssl-cert`/`--ssl-key` flags; `--ssl-mode required` (or stricter) makes TLS mandatory on both. The DSN `?tls=` parameter above still works, takes precedence when set, and is the only TLS knob for the offline read commands (`query`/`recover`/`reconstruct`/`verify`/`shim`), which have no `--ssl-mode`. See [streaming.md → TLS/SSL for managed MySQL](streaming.md#tlsssl-for-managed-mysql-rds-aurora-cloud-sql).

### Metrics endpoint security

The `/metrics` endpoint has no built-in authentication. Bind it to an internal interface:

```bash
--metrics-addr "10.0.0.5:9090"   # internal IP only
```

Or use a reverse proxy (nginx, Caddy) with basic auth in front if the endpoint must be accessible across network boundaries.

## 10. Schema Change Workflow

When you run `ALTER TABLE` on the source:

1. Run `ALTER TABLE` on the source as normal — the stream will continue.
2. Re-run `bintrail snapshot` to update the schema snapshot in the index:
   ```bash
   bintrail snapshot \
       --source-dsn "$SOURCE_DSN" \
       --index-dsn  "$INDEX_DSN" \
       --schemas    "myapp"
   ```
3. No stream restart is needed. The new snapshot is used for all subsequent queries and recovery.

> **Note:** during streaming, DBTrail logs a `column count mismatch` warning for events on a changed table until the snapshot is updated. Those events are skipped — they are not indexed. Take the snapshot promptly after DDL changes.
>
> During file-based indexing, `bintrail index` instead **fails loud**: when rows for a snapshot-eligible table are skipped (table absent from the snapshot, or diverging column count) and the event is at-or-after the snapshot time, the file errors with `schema gap: … the schema snapshot is stale` and is marked `failed` rather than silently completing with a gap. Remediation: re-run `bintrail snapshot`, then `bintrail index --all` — a failed file re-indexes from the start. Snapshot-excluded system schemas (`information_schema`, `performance_schema`, `mysql`, `sys` — e.g. `mysql.rds_heartbeat2` on RDS binlogs) stay warn-and-skip and never fail the file. Tables a **degraded snapshot excluded by validation** (no explicit primary key / non-InnoDB) likewise stay warn-and-skip: re-running `bintrail snapshot` would exclude them again, so instead of failing the file the log names the real remediation — add an explicit primary key and use InnoDB, then re-run `bintrail snapshot`; or leave the table out of `--schemas`/`--tables`.

## 11. Backup and Recovery

### Index database backups

The index database is reconstructable by re-indexing from binlogs, but this is slow (hours for large histories). Schedule regular backups:

```bash
# Logical backup (small-to-medium indexes)
mysqldump --single-transaction --databases bintrail_index \
  | gzip > /backups/bintrail-index-$(date +%Y%m%d).sql.gz

# Physical backup (large indexes, minimal downtime)
xtrabackup --backup --target-dir=/backups/bintrail-$(date +%Y%m%d)/
```

### stream_state is critical

The `stream_state` table contains the resume position (GTID set or file+position). Without it, you must specify `--start-gtid` manually on the next start, or stream will start from the beginning of available binlogs.

Back up `stream_state` before any index database maintenance:

```bash
mysqldump --single-transaction bintrail_index stream_state \
  > /backups/stream_state-$(date +%Y%m%d-%H%M%S).sql
```

### Reconstructing the index

If the index is lost and binlogs are still available on the source:

```bash
# 1. Re-init
bintrail init --index-dsn "$INDEX_DSN" --partitions 90

# 2. Re-snapshot
bintrail snapshot --source-dsn "$SOURCE_DSN" --index-dsn "$INDEX_DSN" --schemas "myapp"

# 3. Re-index from all available binlog files
bintrail index \
    --index-dsn   "$INDEX_DSN" \
    --source-dsn  "$SOURCE_DSN" \
    --binlog-dir  /var/lib/mysql \
    --all \
    --schemas     "myapp"
```

`bintrail index --all` processes every binlog file in `--binlog-dir` in order. This is I/O intensive — run during off-peak hours.

## 12. Troubleshooting

See also: `docs/guide.md` for scenario-based walkthroughs and a detailed FAQ.

### High replication lag

```bash
# Check current lag
curl -s 'localhost:9090/api/v1/query?query=bintrail_stream_replication_lag_seconds' | jq '.data.result[0].value[1]'

# Check event throughput
bintrail status --index-dsn "$INDEX_DSN"

# Check index MySQL write latency
mysql -h index-mysql -e "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'"
```

Causes: index MySQL under heavy load, batch size too small (increase `--batch-size`), network latency to source.

### Disk full on index MySQL

```bash
# Check partition sizes
bintrail status --index-dsn "$INDEX_DSN"

# Emergency: rotate with shorter retention
bintrail rotate --index-dsn "$INDEX_DSN" --retain 7d

# Reclaim space immediately (InnoDB does not auto-shrink)
# For each dropped partition, space is reclaimed automatically (DROP PARTITION is O(1) for InnoDB)
```

### Stream process crash recovery

With systemd `Restart=always`, the process restarts automatically. DBTrail resumes from the GTID in `stream_state`. Check:

```bash
journalctl -u bintrail-stream --since "5 minutes ago"
```

If `stream_state` is empty (fresh install or lost), provide `--start-gtid` explicitly. The safest value is the `gtid_executed` from the source at the time of the last `bintrail snapshot`.

### "column count mismatch" warnings

Expected after `ALTER TABLE`. Run `bintrail snapshot` to update the schema. During streaming, events for that table are skipped (warn-and-skip) until the snapshot is updated. File-based `bintrail index` instead fails loud with `schema gap: … the schema snapshot is stale` and marks the file `failed` — see the note in [Schema Change Workflow](#10-schema-change-workflow).

### GTID gaps / duplicate server-id

If you see GTID-related errors:
- Ensure `--server-id` is unique across all MySQL replication clients connected to the source
- Do not use server IDs in the range the source MySQL uses (check `SHOW VARIABLES LIKE 'server_id'` on source)
- For RDS/Aurora: use server IDs > 1000000 to avoid conflicts with AWS-managed replicas

## Usage telemetry

Official release builds report metadata-only usage statistics (command name,
version, OS/arch, error class — never your data). Long-running units send at
most one beacon per UTC day and log one line at startup when reporting is on.

To disable it across a deployment, set it in the unit file rather than per
host:

```ini
[Service]
Environment=BINTRAIL_TELEMETRY=off
```

Note that telemetry state otherwise lives in a home directory, so **a setting
baked into a golden image travels with the image** — every host cloned from it
inherits the choice made on the machine the image was built from. Setting the
environment variable in the unit is what makes that explicit.

A binary you build yourself has no reporting address compiled in and cannot
send anything at all. See [TELEMETRY.md](./TELEMETRY.md).
