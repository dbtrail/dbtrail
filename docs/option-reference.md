# Every option of dbtrail.cnf

<!-- Generated from internal/settings (docs.go). Do not edit by hand: change docs.go, then run
     go test ./internal/settings -run TestGeneratedFilesAreCurrent -update -->

Every setting DBTrail reads, as a line of [`/etc/dbtrail/dbtrail.cnf`](option-file.md) writes it. The same setting can be given as an environment variable, named under each option. A command-line flag comes first, then the environment, then `.bintrail.env` or `~/.config/bintrail/config.env`, then the file; for index rotation and the backup settings, a value saved in the web interface comes before all of these. `dbtrail config show` lists what is in effect and where each value comes from.

`bintrail-mcp` does not read the file: an option it alone reads is set in the environment the AI client starts it with.

## Capture, the index and their connections

### `index-dsn`

Where the index lives: the connection string (DSN) of the MySQL database that DBTrail writes captured changes into and reads them back from. Every program that records, queries or recovers needs it.

- Default: none: watch, bintrail stream and most bintrail commands refuse to start without it; serve can start without it if its server list has entries; bintrail-mcp then needs index_dsn on each tool call.
- Example: `index-dsn = dbtrail:password@tcp(127.0.0.1:3306)/binlog_index`
- Environment variable: `BINTRAIL_INDEX_DSN`
- Read by: watch, serve, bintrail (stream, up, index, init, snapshot, doctor, agent, access, flag, profile, upload, baseline refresh, export iceberg, query, recover, recover-cascade, reconstruct, status, rotate, archive reconcile, restore-index, drill, shim), bintrail-pg (stream, flashback, reset, and the same shared query/recover/rotate commands), bintrail-mcp. `bintrail-mcp` takes it from its own environment, never from the file.
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

### `source-dsn`

The connection string of the source MySQL or MariaDB server whose changes are recorded. In watch it is the one server given at startup; leave it out and watch starts with no server, and servers are added in the web interface.

- Default: none: watch starts with no main server; bintrail stream, up, snapshot, doctor and dump refuse to start without it.
- Example: `source-dsn = repl:password@tcp(db1.internal:3306)/`
- Environment variable: `BINTRAIL_SOURCE_DSN`
- Read by: watch, bintrail (stream, up, index, snapshot, doctor, dump, dump wait-quiet, agent, init-shim, proxysql-config), bintrail-console sql-compare, bintrail-mcp (only for extension tools that open the source). `bintrail-mcp` takes it from its own environment, never from the file.
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

### `source-flavor`

Says whether the source server is MySQL or MariaDB. Normally left empty so DBTrail asks the server; if set and the server says otherwise, capture refuses to start.

- Default: empty: detected from the server.
- Example: `source-flavor = mariadb`
- Environment variable: `BINTRAIL_SOURCE_FLAVOR`
- Read by: watch, bintrail (stream, up, agent).

### `schemas`

A comma-separated list of databases (schemas) to work on. For capture it limits which databases' changes are recorded; for snapshot, doctor and dump it limits which databases are read or checked.

- Default: all user databases.
- Example: `schemas = shop,billing`
- Environment variable: `BINTRAIL_SCHEMAS`
- Read by: watch (main server only), bintrail (stream, up, index, snapshot, doctor, dump, agent), bintrail-pg (stream, baseline, doctor).

### `tables`

A comma-separated list of tables, written as database.table. For capture it limits which tables' changes are recorded. It also becomes the table list of every other command that has a --tables option, such as reconstruct, drill, baseline and export iceberg, so setting it for capture also narrows those.

- Default: all tables.
- Example: `tables = shop.orders,shop.customers`
- Environment variable: `BINTRAIL_TABLES`
- Read by: watch (main server only), bintrail (stream, up, index, agent, baseline, baseline refresh, dump, export iceberg, reconstruct, drill), bintrail-pg (stream, baseline, doctor, and the shared reconstruct and drill).

### `id`

The installation's identity, a UUID. It names the folder its archives go in (bintrail_id=&lt;uuid&gt;/...) and signs the snapshots it writes. Commands that read archives need it to find them.

- Default: none: rotate uses the identity saved in the index; query needs it only when an archive location is given; baseline writes an unsigned snapshot; bintrail-mcp finds archives from the index instead.
- Example: `id = 6f1c2e4a-9b3d-4c8e-a1f0-2d7e5b9c3a10`
- Environment variable: `BINTRAIL_ID`
- Read by: bintrail (query, rotate, baseline), bintrail-pg (baseline, and the shared query and rotate), bintrail-mcp (together with BINTRAIL_ARCHIVE_S3). `bintrail-mcp` takes it from its own environment, never from the file.

### `server-id`

The replica ID DBTrail uses when it connects to the source to read its changes, like a MySQL replica's server_id. It must differ from every other replica of that source.

- Default: watch and bintrail up: a number derived from the source's host, user and database name; bintrail stream and bintrail-pg stream: none, required.
- Example: `server-id = 4200001`
- Environment variable: `BINTRAIL_SERVER_ID`
- Read by: watch (main server only), bintrail (stream, up, agent, init-shim), bintrail-pg stream.

### `server-uuid`

Only for bintrail agent: the UUID of a server already registered in the hosted service, so the agent attaches to that record instead of creating a new one.

- Default: empty: the hosted service creates a new server record.
- Example: `server-uuid = 0b7d3f52-8e14-4a6c-9c2e-71f5a0d4b8e3`
- Environment variable: `BINTRAIL_SERVER_UUID`
- Read by: bintrail agent.

### `batch-size`

How many captured row changes are written to the index in one INSERT. Larger batches mean fewer round trips; values above MySQL's placeholder limit are lowered to it.

- Default: 1000 (bintrail restore-index: 5000).
- Example: `batch-size = 2000`
- Environment variable: `BINTRAIL_BATCH_SIZE`
- Read by: watch, bintrail (stream, up, index, agent, restore-index), bintrail-pg stream.

### `commit-within`

How long finished source transactions may wait in memory for more rows to share a batch before they are written to the index anyway. It bounds how late a change becomes visible on a quiet source; a busy source fills batches and never waits this long. off or 0 turns the wait off, so changes are written when a batch fills or at the checkpoint.

- Default: 1s.
- Example: `commit-within = 500ms`
- Environment variable: `BINTRAIL_COMMIT_WITHIN`
- Read by: watch (all MySQL and PostgreSQL servers it records), bintrail (stream, up), bintrail-pg stream.

### `start-gtid`

A GTID set to start reading the source's changes from. Used only when there is no saved position yet (first run, or after a reset); once DBTrail has a checkpoint it continues from there.

- Default: empty: start from the beginning of the binlog given by --start-file, or from the saved checkpoint.
- Example: `start-gtid = 3e11fa47-71ca-11e1-9e33-c80aa9429562:1-12345`
- Environment variable: `BINTRAIL_START_GTID`
- Read by: bintrail (stream, up, agent).

### `stream-gap-timeout`

How many seconds the startup checks for missing binlog ranges may take (SHOW BINARY LOGS and the GTID variables). Raise it on managed servers with many binlog files.

- Default: 30 (seconds).
- Example: `stream-gap-timeout = 120`
- Environment variable: `BINTRAIL_STREAM_GAP_TIMEOUT`
- Read by: bintrail (stream, up).

### `ssl-mode`

Whether connections use TLS (encryption) and how strictly the server's certificate is checked: disabled, preferred, required, verify-ca or verify-identity. In watch and bintrail stream it applies to both the source and the index connection.

- Default: preferred.
- Example: `ssl-mode = verify-identity`
- Environment variable: `BINTRAIL_SSL_MODE`
- Read by: watch (main server), bintrail (stream, and up through stream), bintrail-mcp (only for extension tools that open the source). `bintrail-mcp` takes it from its own environment, never from the file.

### `ssl-ca`

Path to the CA certificate file used to check the source server's TLS certificate.

- Default: empty: the system's trusted CAs.
- Example: `ssl-ca = /etc/dbtrail/tls/ca.pem`
- Environment variable: `BINTRAIL_SSL_CA`
- Read by: watch (main server), bintrail (stream, up), bintrail-mcp (extension tools only). `bintrail-mcp` takes it from its own environment, never from the file.

### `ssl-cert`

Path to the client certificate DBTrail presents to the source when the server requires mutual TLS (both sides show a certificate). Set together with BINTRAIL_SSL_KEY.

- Default: empty: no client certificate.
- Example: `ssl-cert = /etc/dbtrail/tls/client-cert.pem`
- Environment variable: `BINTRAIL_SSL_CERT`
- Read by: watch (main server), bintrail (stream, up), bintrail-mcp (extension tools only). `bintrail-mcp` takes it from its own environment, never from the file.

### `ssl-key`

Path to the private key that goes with BINTRAIL_SSL_CERT, for mutual TLS to the source.

- Default: empty: no client key.
- Example: `ssl-key = /etc/dbtrail/tls/client-key.pem`
- Environment variable: `BINTRAIL_SSL_KEY`
- Read by: watch (main server), bintrail (stream, up), bintrail-mcp (extension tools only). `bintrail-mcp` takes it from its own environment, never from the file.

### `metrics-addr`

The address where capture publishes Prometheus metrics (a /metrics page a monitoring system reads). Empty means no metrics page. The router has its own setting, BINTRAIL_ROUTER_METRICS_ADDR.

- Default: empty: no metrics.
- Example: `metrics-addr = 127.0.0.1:9090`
- Environment variable: `BINTRAIL_METRICS_ADDR`
- Read by: watch, bintrail (stream, up), bintrail-pg stream.

### `metrics-scrape-interval`

How often, in seconds, the index size and status numbers on the metrics page are refreshed. Only matters when metrics are published.

- Default: 60 (seconds).
- Example: `metrics-scrape-interval = 30`
- Environment variable: `BINTRAIL_METRICS_SCRAPE_INTERVAL`
- Read by: watch, bintrail (stream, up).

### `rotate-retain`

How long captured changes stay in the index: hourly partitions older than this are dropped by the built-in rotation. Written as days or hours (7d, 48h); off turns dropping off and the index grows without limit.

- Default: 48h.
- Example: `rotate-retain = 7d`
- Environment variable: `BINTRAIL_ROTATE_RETAIN`
- Read by: watch, bintrail up, bintrail-pg stream.

### `rotate-interval`

How often the built-in rotation runs (drops old partitions and adds new ones).

- Default: 1h.
- Example: `rotate-interval = 30m`
- Environment variable: `BINTRAIL_ROTATE_INTERVAL`
- Read by: watch, bintrail up, bintrail-pg stream.

### `rotate-add-future`

How many empty hourly partitions the built-in rotation keeps ready ahead of the current hour, so new changes always have a partition to go into.

- Default: 3.
- Example: `rotate-add-future = 6`
- Environment variable: `BINTRAIL_ROTATE_ADD_FUTURE`
- Read by: watch, bintrail up, bintrail-pg stream.

### `status-log-interval`

How often a one-line status summary is written to the service's own log: in watch, how many changes were recorded and how far behind it is; in the router, how many statements it saw and who answered them. off or 0 writes none; values under a second become one second.

- Default: 1m.
- Example: `status-log-interval = 5m`
- Environment variable: `BINTRAIL_STATUS_LOG_INTERVAL`
- Read by: watch, router.

## Archives, snapshots and S3

### `archive-dir`

A local folder of Parquet archives: where bintrail rotate writes the hours it removes from the index before dropping them, and where query, archive reconcile, restore-index and the agent read archived hours from.

- Default: none: rotate drops old partitions without archiving them; readers use only the index and archive locations recorded in it.
- Example: `archive-dir = /var/lib/dbtrail/archives`
- Environment variable: `BINTRAIL_ARCHIVE_DIR`
- Read by: bintrail (rotate, query, archive reconcile, restore-index, agent), bintrail-pg (same shared commands).

### `archive-s3`

An S3 location (s3://bucket/prefix/) for Parquet archives: bintrail rotate uploads archived hours there (it needs BINTRAIL_ARCHIVE_DIR too), and query, archive reconcile, restore-index, doctor and the agent read from or check it. bintrail-mcp uses it, together with BINTRAIL_ID, to find archives.

- Default: none: archives stay local, and readers use archive locations recorded in the index.
- Example: `archive-s3 = s3://acme-dbtrail/archives/`
- Environment variable: `BINTRAIL_ARCHIVE_S3`
- Read by: bintrail (rotate, query, archive reconcile, restore-index, doctor, agent), bintrail-pg (same shared commands), bintrail-mcp. `bintrail-mcp` takes it from its own environment, never from the file.

### `archive-s3-region`

The AWS region of the BINTRAIL_ARCHIVE_S3 bucket.

- Default: the bucket's own region, found automatically (with a custom S3 endpoint: AWS_REGION or ~/.aws/config).
- Example: `archive-s3-region = us-west-2`
- Environment variable: `BINTRAIL_ARCHIVE_S3_REGION`
- Read by: bintrail (rotate, doctor), bintrail-pg rotate.

### `baseline-dir`

Only for bintrail-mcp: a local folder of snapshots (baselines) that the reconstruct and recover_cascade tools use when the call does not name one. The CLI and the console use --baseline-dir or BINTRAIL_CONSOLE_BASELINE_DIR instead.

- Default: none: those tools then need baseline_dir or baseline_s3 in the call.
- Example: `baseline-dir = /var/lib/dbtrail/baselines`
- Environment variable: `BINTRAIL_BASELINE_DIR`
- Read by: bintrail-mcp. `bintrail-mcp` takes it from its own environment, never from the file.

### `baseline-s3`

Only for bintrail-mcp: an S3 location of snapshots (baselines) for the reconstruct and recover_cascade tools, used when the call names none and BINTRAIL_BASELINE_DIR is not set.

- Default: none.
- Example: `baseline-s3 = s3://acme-dbtrail/baselines/`
- Environment variable: `BINTRAIL_BASELINE_S3`
- Read by: bintrail-mcp. `bintrail-mcp` takes it from its own environment, never from the file.

### `baseline-retain`

Only for bintrail baseline with --upload: local snapshots older than this (7d, 48h) are deleted once a copy is safely in S3. The newest snapshot of each table and the only copy of anything are never deleted. watch uses BINTRAIL_CONSOLE_BASELINE_RETAIN instead.

- Default: empty: nothing is deleted.
- Example: `baseline-retain = 14d`
- Environment variable: `BINTRAIL_BASELINE_RETAIN`
- Read by: bintrail baseline.

### `baseline-refresh-interval`

How often watch refreshes each server's newest snapshot from the index (30m, 6h, 1d). Each refresh writes a new snapshot without reading the source again.

- Default: off: no scheduled refresh from this setting (refreshes started or scheduled in the web interface still run).
- Example: `baseline-refresh-interval = 6h`
- Environment variable: `BINTRAIL_BASELINE_REFRESH_INTERVAL`
- Read by: watch.

### `baseline-table-deltas`

When watch refreshes a snapshot, a table that changed keeps its previous file and gets a small pair of files with only the changes, instead of being written again in full; the table is rewritten in full when the changes grow past half its size or are a day old. Set false to always rewrite changed tables. DuckDB views made before turning it on must be generated again.

- Default: on.
- Example: `baseline-table-deltas = false`
- Environment variable: `BINTRAIL_BASELINE_TABLE_DELTAS`
- Read by: watch.

### `baseline-carry-forward-unchanged`

When watch refreshes a snapshot and a table had no changes, the new snapshot reuses the previous file (a hard link where possible) instead of writing it again. Disk usage figures then count shared space. Set false to stop this; to rewrite every table, BINTRAIL_BASELINE_TABLE_DELTAS must be off too.

- Default: on.
- Example: `baseline-carry-forward-unchanged = false`
- Environment variable: `BINTRAIL_BASELINE_CARRY_FORWARD_UNCHANGED`
- Read by: watch.

### `key-index`

During a snapshot refresh that writes table deltas, finds the changed rows through a small key index file kept beside each table (&lt;table&gt;.keyidx), building it when missing, instead of scanning the whole table file. 0, false, no or off turns it off: every refresh scans the file and no key index is built.

- Default: on.
- Example: `key-index = off`
- Environment variable: `BINTRAIL_KEY_INDEX`
- Read by: watch, bintrail baseline refresh.

### `pg-table-deltas`

For PostgreSQL servers only: when a snapshot is updated, a changed table keeps its previous file and gets small files with its changes beside it, instead of being written again in full. Everything that reads the copy on this machine reads those files with the table's file. 0, false, no or off turns it off: every changed table is written in full at every update.

- Default: on.
- Example: `pg-table-deltas = off`
- Environment variable: `BINTRAIL_PG_TABLE_DELTAS`
- Read by: watch (PostgreSQL servers).

### `s3-bucket`

An S3 bucket name. For bintrail init: the bucket to create for archives (it gets a rule that deletes objects after 365 days). For bintrail agent: the bucket where captured row data is written in bring-your-own-storage mode.

- Default: none: init creates no bucket; the agent does not write to S3.
- Example: `s3-bucket = acme-dbtrail-archive`
- Environment variable: `BINTRAIL_S3_BUCKET`
- Read by: bintrail (init, agent).

### `s3-region`

The AWS region of the BINTRAIL_S3_BUCKET bucket: where bintrail init creates it, or where the agent writes.

- Default: bintrail init: us-east-1; bintrail agent: the bucket's own region, found automatically.
- Example: `s3-region = eu-west-1`
- Environment variable: `BINTRAIL_S3_REGION`
- Read by: bintrail (init, agent).

### `s3-arn`

Only for bintrail init: the ARN of an existing S3 bucket to use for archives, instead of creating one. Cannot be combined with BINTRAIL_S3_BUCKET.

- Default: none.
- Example: `s3-arn = arn:aws:s3:::acme-dbtrail-archive`
- Environment variable: `BINTRAIL_S3_ARN`
- Read by: bintrail init.

### `s3-prefix`

Only for bintrail agent: the folder (key prefix) inside BINTRAIL_S3_BUCKET under which the agent writes row data.

- Default: bintrail/.
- Example: `s3-prefix = dbtrail/prod/`
- Environment variable: `BINTRAIL_S3_PREFIX`
- Read by: bintrail agent.

### `s3-endpoint`

Sends every S3 read and write to an S3-compatible store (MinIO, Wasabi, LocalStack) instead of AWS. Written as scheme://host[:port] with no path. A malformed value is an error, never a silent fallback to AWS.

- Default: AWS S3 (or the AWS SDK's own AWS_ENDPOINT_URL_S3 / AWS_ENDPOINT_URL when set).
- Example: `s3-endpoint = http://minio.internal:9000`
- Environment variable: `BINTRAIL_S3_ENDPOINT`
- Read by: every program that reads or writes S3: watch, serve, router, bintrail, bintrail-pg, bintrail-mcp. `bintrail-mcp` takes it from its own environment, never from the file.

### `s3-path-style`

Whether S3 addresses put the bucket in the path (http://host/bucket/key) instead of in the host name. true or false. Set it to false for a store that only accepts bucket-in-host addresses.

- Default: on when BINTRAIL_S3_ENDPOINT is set; otherwise the AWS SDK's default (bucket in the host name).
- Example: `s3-path-style = false`
- Environment variable: `BINTRAIL_S3_PATH_STYLE`
- Read by: every program that reads or writes S3: watch, serve, router, bintrail, bintrail-pg, bintrail-mcp. `bintrail-mcp` takes it from its own environment, never from the file.

### `iceberg-warehouse`

Only for bintrail export iceberg: the local folder the Iceberg tables are written under.

- Default: none: required by export iceberg.
- Example: `iceberg-warehouse = /var/lib/dbtrail/iceberg`
- Environment variable: `BINTRAIL_ICEBERG_WAREHOUSE`
- Read by: bintrail export iceberg.

### `iceberg-catalog-credential`

Only for bintrail export iceberg with --catalog rest: the OAuth client credential (client_id:client_secret) used to log in to the Iceberg REST catalog. Use one way to log in: this, a bearer token, or AWS signing.

- Default: none: no client credential.
- Example: `iceberg-catalog-credential = my-client-id:my-client-secret`
- Environment variable: `BINTRAIL_ICEBERG_CATALOG_CREDENTIAL`
- Read by: bintrail export iceberg.
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

### `iceberg-catalog-token`

Only for bintrail export iceberg with --catalog rest: a bearer token for the Iceberg REST catalog. Sent only over https, or plain http to this machine.

- Default: none: no token.
- Example: `iceberg-catalog-token = <catalog-token>`
- Environment variable: `BINTRAIL_ICEBERG_CATALOG_TOKEN`
- Read by: bintrail export iceberg.
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

## Queries, recovery and DuckDB

### `column-eq`

A filter of the form column=value applied to bintrail query and bintrail recover: only changes where that column had that value before or after are returned. Set in the file, it filters every query and recover run.

- Default: none: no column filter.
- Example: `column-eq = customer_id=42`
- Environment variable: `BINTRAIL_COLUMN_EQ`
- Read by: bintrail (query, recover), bintrail-pg (same shared query, recover).

### `ultrafast`

Lets DuckDB (the engine that reads the Parquet archive files) use all the machine's cores and about 80% of its memory, and read S3 archives directly in one parallel scan, instead of the small safe budget of 2 threads and 4 GB. Meant for a big dedicated machine running one-off query, recovery or reconstruct commands, not for small containers.

- Default: off (2 threads, 4GB).
- Example: `ultrafast = true`
- Environment variable: `BINTRAIL_ULTRAFAST`
- Read by: bintrail (query, recover, recover-cascade, reconstruct, drill, baseline refresh, export iceberg); bintrail-pg (query, recover, recover-cascade, reconstruct, drill). Not verify: it has the flag but does not read the variable..

### `duckdb-threads`

How many threads DuckDB (the engine that reads the Parquet archive files) may use in the one-off read commands. 0 means one per CPU core. It wins over BINTRAIL_ULTRAFAST.

- Default: 2 (all cores when BINTRAIL_ULTRAFAST is on).
- Example: `duckdb-threads = 8`
- Environment variable: `BINTRAIL_DUCKDB_THREADS`
- Read by: bintrail (query, recover, recover-cascade, reconstruct, drill, baseline refresh, export iceberg); bintrail-pg (query, recover, recover-cascade, reconstruct, drill).

### `duckdb-memory-limit`

The most memory DuckDB (the engine that reads the Parquet archive files) may use in the one-off read commands, as a size with a unit. A value without a unit, or zero, is refused with an error. It wins over BINTRAIL_ULTRAFAST.

- Default: 4GB (about 80% of RAM when BINTRAIL_ULTRAFAST is on).
- Example: `duckdb-memory-limit = 16GB`
- Environment variable: `BINTRAIL_DUCKDB_MEMORY_LIMIT`
- Read by: bintrail (query, recover, recover-cascade, reconstruct, drill, baseline refresh, export iceberg); bintrail-pg (query, recover, recover-cascade, reconstruct, drill).

### `duckdb-no-aws-ext`

Any non-empty value stops DuckDB from downloading and loading its AWS extension before reading files in S3, so AWS credentials from profiles or instance roles are not used and only AWS key variables work. For networks whose proxy silently swallows that download, which can otherwise stall each read for minutes. A custom S3 endpoint is still applied.

- Default: off (the AWS extension is loaded when an S3 read needs it).
- Example: `duckdb-no-aws-ext = 1`
- Environment variable: `BINTRAIL_DUCKDB_NO_AWS_EXT`
- Read by: every program that reads S3 through DuckDB: watch, serve, router, pg-router, bintrail, bintrail-pg, bintrail-mcp. `bintrail-mcp` takes it from its own environment, never from the file.

### `recover-max-bytes`

The recover command refuses to write a reversal (undo) SQL script whose estimated row data is bigger than this, so a recovery over large BLOB or TEXT columns does not run the machine out of memory. 0 means no limit. The web interface has its own fixed limit and ignores this.

- Default: 2GB.
- Example: `recover-max-bytes = 4GB`
- Environment variable: `BINTRAIL_RECOVER_MAX_BYTES`
- Read by: bintrail (recover); bintrail-pg (recover).

### `reconstruct-warn-events`

When rebuilding a whole table, log a memory warning if one table has more changes to replay than this number. It only warns, it never stops the run. In reconstruct the number is divided by how many tables are rebuilt at once. 0 turns the warning off.

- Default: 5000000.
- Example: `reconstruct-warn-events = 10000000`
- Environment variable: `BINTRAIL_RECONSTRUCT_WARN_EVENTS`
- Read by: bintrail (reconstruct in full-table mode, baseline refresh); bintrail-pg (reconstruct).

### `reconstruct-fetch-batch`

How many change events are read per page when a whole table is rebuilt. Lower it to use less memory on a small machine; raise it to make fewer trips to the archives. With S3 archives, a value far below your busiest hour's event count makes each hour's file be downloaded many times. 0 uses the built-in value.

- Default: 100000.
- Example: `reconstruct-fetch-batch = 50000`
- Environment variable: `BINTRAIL_RECONSTRUCT_FETCH_BATCH`
- Read by: bintrail (reconstruct in full-table mode, baseline refresh, export iceberg); bintrail-pg (reconstruct).

### `mcp-query-max-limit`

The most rows the query tool of the standalone MCP server (the program an AI assistant talks to) returns for one request, whatever limit the assistant asks for. An empty, invalid or zero value keeps the default; the ceiling cannot be turned off. The MCP endpoint inside the web interface has its own limit and ignores this.

- Default: 1000000.
- Example: `mcp-query-max-limit = 100000`
- Environment variable: `BINTRAIL_MCP_QUERY_MAX_LIMIT`
- Read by: bintrail-mcp. `bintrail-mcp` takes it from its own environment, never from the file.

## The agent and its buffer

### `api-key`

The API key the agent uses to log in to the hosted DBTrail service. Only for the agent mode that connects this machine to the hosted service. Secret.

- Default: none: required, the agent does not start without it.
- Example: `api-key = ak_REPLACE_WITH_YOUR_KEY`
- Environment variable: `BINTRAIL_API_KEY`
- Read by: bintrail (agent).
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

### `agent-endpoint`

The WebSocket address of the hosted DBTrail service the agent connects to.

- Default: none: required, the agent does not start without it.
- Example: `agent-endpoint = wss://api.dbtrail.io/v1/agent`
- Environment variable: `BINTRAIL_AGENT_ENDPOINT`
- Read by: bintrail (agent).

### `agent-max-reconnect-attempts`

After this many reconnect failures in a row to the hosted service, the agent exits with an error so a service manager such as systemd can restart it. The count resets whenever a connection stays up for a while. 0 means retry forever.

- Default: 10.
- Example: `agent-max-reconnect-attempts = 20`
- Environment variable: `BINTRAIL_AGENT_MAX_RECONNECT_ATTEMPTS`
- Read by: bintrail (agent).

### `buffer-retain`

How long the agent keeps captured changes in its in-memory buffer before dropping them. Only used when the agent captures the source itself (bring-your-own-storage mode, with a source DSN and server id). Written in hours or days.

- Default: 6h.
- Example: `buffer-retain = 24h`
- Environment variable: `BINTRAIL_BUFFER_RETAIN`
- Read by: bintrail (agent, bring-your-own-storage mode).

### `buffer-max-events`

The most changes the agent keeps in its in-memory buffer; past it the oldest are dropped first. Only used in the agent's bring-your-own-storage mode. 0 means no count limit.

- Default: 0 (no limit).
- Example: `buffer-max-events = 1000000`
- Environment variable: `BINTRAIL_BUFFER_MAX_EVENTS`
- Read by: bintrail (agent, bring-your-own-storage mode).

### `buffer-max-bytes`

The approximate most memory the agent's in-memory buffer may hold; past it the oldest changes are dropped first. Only used in the agent's bring-your-own-storage mode. 0 means no size limit.

- Default: 0 (no limit).
- Example: `buffer-max-bytes = 1GB`
- Environment variable: `BINTRAIL_BUFFER_MAX_BYTES`
- Read by: bintrail (agent, bring-your-own-storage mode).

### `flush-interval`

The longest time the agent waits before sending what it has captured: the metadata to the hosted service and the row data to your own S3 bucket. A full batch is sent sooner. Only used in the agent's bring-your-own-storage mode.

- Default: 5s.
- Example: `flush-interval = 10s`
- Environment variable: `BINTRAIL_FLUSH_INTERVAL`
- Read by: bintrail (agent, bring-your-own-storage mode).

## The time-travel shim

### `auth-method`

The MySQL login method the time-travel shim (a MySQL-protocol server that answers queries about past data) offers to clients. Set caching_sha2_password or sha256_password for MySQL 8.4 and later, where mysql_native_password is off by default; that needs ProxySQL 2.7 or later in front.

- Default: mysql_native_password.
- Example: `auth-method = caching_sha2_password`
- Environment variable: `BINTRAIL_AUTH_METHOD`
- Read by: bintrail (shim); bintrail-pg (shim).

### `shim-query-timeout`

The longest one query to the time-travel shim may run, including the wait for a free full-table slot; past it the client gets MySQL error 1317. 0 means no limit. bintrail-pg flashback (the PostgreSQL time-travel server) takes this as its per-query deadline too.

- Default: 5m for shim; 30s for bintrail-pg flashback.
- Example: `shim-query-timeout = 10m`
- Environment variable: `BINTRAIL_SHIM_QUERY_TIMEOUT`
- Read by: bintrail (shim); bintrail-pg (shim, flashback).

### `shim-max-connections`

The most clients that may be connected to the time-travel shim at once; one more is refused with MySQL error 1040, Too many connections. 0 means no limit. bintrail-pg flashback takes it as its own connection cap too. The PostgreSQL router does not read it.

- Default: 100 for shim; 0 (no limit) for bintrail-pg flashback.
- Example: `shim-max-connections = 200`
- Environment variable: `BINTRAIL_SHIM_MAX_CONNECTIONS`
- Read by: bintrail (shim); bintrail-pg (shim, flashback).

### `shim-max-fulltable-queries`

How many whole-table time-travel queries (the heaviest kind) the shim runs at once. The rest wait for a slot up to the query timeout, then fail with MySQL error 1203. 0 means no limit.

- Default: 4.
- Example: `shim-max-fulltable-queries = 2`
- Environment variable: `BINTRAIL_SHIM_MAX_FULLTABLE_QUERIES`
- Read by: bintrail (shim); bintrail-pg (shim).

## The web console and capture (bintrail-console serve and watch)

### `console-listen`

The address and port the web interface listens on. The dbtrail terminal commands also use it to find the running DBTrail when they log in with BINTRAIL_CONSOLE_TOKEN instead of the file DBTrail writes at start.

- Default: 127.0.0.1:8090.
- Example: `console-listen = 0.0.0.0:8090`
- Environment variable: `BINTRAIL_CONSOLE_LISTEN`
- Read by: watch, serve, dbtrail.

### `console-token`

A fixed access token of your own choosing, for scripts and automation; people log in with the password. On the web interface it lets API calls in and counts as authentication when listening on a non-local address. On the MySQL and PostgreSQL routers it is one more password for the port, but does not turn the port on. The dbtrail terminal commands send it when they cannot use the file DBTrail writes at start. Secret.

- Default: none (no token; never generated).
- Example: `console-token = REPLACE_WITH_A_LONG_RANDOM_TOKEN`
- Environment variable: `BINTRAIL_CONSOLE_TOKEN`
- Read by: watch, serve, router, pg-router, dbtrail.
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

### `console-url`

The address people actually open the web interface at, when it is not the address it listens on (for example a container listening on 8090 behind a different published port). It only changes the address printed in the startup message; nothing listens, redirects or builds links from it.

- Default: none: the startup message shows the listen address.
- Example: `console-url = http://dbtrail.example.com:8080`
- Environment variable: `BINTRAIL_CONSOLE_URL`
- Read by: watch, serve.

### `console-servers`

Path to the file where the web interface saves the list of database servers it watches and their settings. The routers only read it.

- Default: ~/.config/bintrail/console-servers.yaml (in the home folder of the account DBTrail runs as).
- Example: `console-servers = /var/lib/bintrail/console-servers.yaml`
- Environment variable: `BINTRAIL_CONSOLE_SERVERS`
- Read by: watch, serve, router, pg-router.

### `console-auth`

Path to the file that holds the web interface's login (user names and passwords). The routers read it only so a SQL statement cannot read it through a folder that contains it. The dbtrail terminal commands look beside it for the file DBTrail writes at start, and the bintrail-console user commands write passwords into it.

- Default: ~/.config/bintrail/console-auth.yaml (in the home folder of the account DBTrail runs as).
- Example: `console-auth = /var/lib/bintrail/console-auth.yaml`
- Environment variable: `BINTRAIL_CONSOLE_AUTH`
- Read by: watch, serve, router, pg-router, dbtrail (also bintrail-console user).

### `console-mcp-token-file`

Path to the file where the web interface keeps the token made in Settings, MCP Server (what an AI assistant uses to connect). Point it at lasting storage when DBTrail runs in a container. The routers read it only so a SQL statement cannot read it through a folder that contains it.

- Default: ~/.config/bintrail/console-mcp-token.yaml (in the home folder of the account DBTrail runs as).
- Example: `console-mcp-token-file = /var/lib/bintrail/console-mcp-token.yaml`
- Environment variable: `BINTRAIL_CONSOLE_MCP_TOKEN_FILE`
- Read by: watch, serve, router, pg-router.

### `console-tls-cert`

A certificate file (PEM format) so the web interface is served over HTTPS. Needs BINTRAIL_CONSOLE_TLS_KEY too.

- Default: none: plain HTTP.
- Example: `console-tls-cert = /etc/dbtrail/tls/cert.pem`
- Environment variable: `BINTRAIL_CONSOLE_TLS_CERT`
- Read by: watch, serve.

### `console-tls-key`

The private key file (PEM format) that goes with BINTRAIL_CONSOLE_TLS_CERT, for serving the web interface over HTTPS.

- Default: none: plain HTTP.
- Example: `console-tls-key = /etc/dbtrail/tls/key.pem`
- Environment variable: `BINTRAIL_CONSOLE_TLS_KEY`
- Read by: watch, serve.

### `console-allowed-hosts`

Extra host names, separated by commas, that the web interface accepts in requests, for example the name of a reverse proxy in front of it. IP addresses and localhost are always accepted.

- Default: none (only IP addresses and localhost).
- Example: `console-allowed-hosts = dbtrail.example.com,dbtrail.internal`
- Environment variable: `BINTRAIL_CONSOLE_ALLOWED_HOSTS`
- Read by: watch, serve.

### `console-allow-setup`

Set to 1 or true to let the first password be created from the browser even when the web interface listens on a non-local address. Use it only when that address is protected some other way, for example published only on the host's own loopback. Other values are ignored.

- Default: off.
- Example: `console-allow-setup = 1`
- Environment variable: `BINTRAIL_CONSOLE_ALLOW_SETUP`
- Read by: watch, serve.

### `console-startup-page`

When DBTrail fails at startup because of its configuration, permissions or a missing database, it keeps the web interface port open for 5 minutes with one page that says why, then exits so its service manager restarts it. Set to 0, false, off or no to exit at once instead, for example if your alerting watches for a stopped process.

- Default: on.
- Example: `console-startup-page = off`
- Environment variable: `BINTRAIL_CONSOLE_STARTUP_PAGE`
- Read by: watch.

### `console-baseline-dir`

A local folder of baseline snapshots (full copies of tables in Parquet files). With it, the web interface can show the whole state of a table or row at a past moment (the Reconstruct page).

- Default: none.
- Example: `console-baseline-dir = /var/lib/bintrail/baselines`
- Environment variable: `BINTRAIL_CONSOLE_BASELINE_DIR`
- Read by: watch, serve.

### `console-baseline-s3`

An S3 location of baseline snapshots (full copies of tables in Parquet files). With it, the web interface can show the whole state of a table or row at a past moment (the Reconstruct page).

- Default: none.
- Example: `console-baseline-s3 = s3://my-bucket/baselines/`
- Environment variable: `BINTRAIL_CONSOLE_BASELINE_S3`
- Read by: watch, serve.

### `console-baseline-retain`

The older way to limit local baseline snapshots, still honored: delete local snapshots older than this, but only once a copy is safely in S3, and never the only copy or the newest snapshot of a table. Written in days or hours. The per-server Keep by count setting on the Snapshots page is the main control now.

- Default: none (no deletion by age).
- Example: `console-baseline-retain = 14d`
- Environment variable: `BINTRAIL_CONSOLE_BASELINE_RETAIN`
- Read by: watch.

### `console-baseline-staging`

The working folder where backups started from DBTrail write their dump before it is converted and uploaded; full reads write there too. Point it at a disk with room. Read at startup, so a change needs a restart. The web interface has had no setting for it since 0.89.0. A folder saved there by 0.85 to 0.88 is still kept and wins over this: dbtrail config check names this line when that happens, and PUT /api/backup-settings/daemon/staging_dir with {"use_startup":true} removes the saved folder. Restart DBTrail after that.

- Default: a folder named baseline-staging beside DBTrail's data (the servers file), or the system temp folder when that one cannot be used.
- Example: `console-baseline-staging = /data/dbtrail-work`
- Environment variable: `BINTRAIL_CONSOLE_BASELINE_STAGING`
- Read by: watch.

### `console-baseline-trigger`

Set to 1 or true to let the web interface start new baseline backups itself. It needs mydumper installed and connects to the source database to dump it.

- Default: off.
- Example: `console-baseline-trigger = 1`
- Environment variable: `BINTRAIL_CONSOLE_BASELINE_TRIGGER`
- Read by: watch.

### `console-baseline-lock-mode`

How a MySQL or MariaDB backup started from DBTrail makes all its threads see one moment: ftwrl (a short global read lock), lock-all (locks the dumped tables; needs only LOCK TABLES), safe-no-lock (no extra privilege, refuses instead of producing an inconsistent copy) or no-lock (accepts an inconsistent copy). A mode saved in the web interface, which had this setting from 0.85 to 0.88, wins over this; PUT /api/backup-settings/daemon/lock_mode with {"use_startup":true} removes it. An invalid value disables MySQL backups but never capture. No effect on PostgreSQL.

- Default: automatic: lock-all on RDS or Aurora, ftwrl elsewhere with one retry in lock-all if ftwrl is refused.
- Example: `console-baseline-lock-mode = lock-all`
- Environment variable: `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE`
- Read by: watch.

### `console-archive-staging`

A local folder where old index partitions are written as Parquet files before they are uploaded to a server's archive S3 bucket and then deleted locally.

- Default: the system temp folder, under bintrail-archive-staging.
- Example: `console-archive-staging = /data/dbtrail-archive-staging`
- Environment variable: `BINTRAIL_CONSOLE_ARCHIVE_STAGING`
- Read by: watch.

### `console-verify-trigger`

Set to 1 or true to let the web interface run verification on demand (a check that the captured data rebuilds to the right table contents). Not needed when BINTRAIL_CONSOLE_VERIFY_INTERVAL is set, which turns it on too.

- Default: off (the bundled Docker Compose file turns it on).
- Example: `console-verify-trigger = 1`
- Environment variable: `BINTRAIL_CONSOLE_VERIFY_TRIGGER`
- Read by: watch.

### `console-verify-interval`

Run verification on a schedule for every server in the list, this often, and keep the results in the history. Written in days or hours.

- Default: off.
- Example: `console-verify-interval = 24h`
- Environment variable: `BINTRAIL_CONSOLE_VERIFY_INTERVAL`
- Read by: watch.

### `console-verify-tables`

Limits the scheduled verification to these tables, as a comma-separated list of schema.table. A list saved in the web interface, which had this setting from 0.85 to 0.88, wins over this; PUT /api/backup-settings/daemon/verify_tables with {"use_startup":true} removes it.

- Default: all tables.
- Example: `console-verify-tables = shop.orders,shop.customers`
- Environment variable: `BINTRAIL_CONSOLE_VERIFY_TABLES`
- Read by: watch.

### `console-notify-webhook`

A web address that receives a JSON message when capture stops, capture continuity is lost, verification finds a problem, partition rotation is unhealthy, or snapshots get too old to restore from. Secret, since such addresses often carry a key.

- Default: off.
- Example: `console-notify-webhook = https://hooks.example.com/services/REPLACE_ME`
- Environment variable: `BINTRAIL_CONSOLE_NOTIFY_WEBHOOK`
- Read by: watch.
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

### `console-sql-memory`

How much memory each SQL statement answered from the copy may use; past it the statement spills to a temporary folder, up to four times this size. At least 512MB. When set, it wins over the value saved in the web interface (Settings, MySQL clients). The routers read the saved value only at start.

- Default: the value saved in the web interface, else 2GB.
- Example: `console-sql-memory = 4GB`
- Environment variable: `BINTRAIL_CONSOLE_SQL_MEMORY`
- Read by: watch, serve, router, pg-router.

### `console-sql-max-in-flight`

How many SQL statements on the copy the web interface's SQL card runs at once; one more waits up to 30 seconds for a free slot, then is refused. They run on the machine that captures, so raise it only with spare cores and memory. The MySQL router ignores it and says so; it sizes itself.

- Default: 2.
- Example: `console-sql-max-in-flight = 4`
- Environment variable: `BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT`
- Read by: watch.

### `console-router-url`

Address where the MySQL router (bintrail-console router) publishes its numbers, so the web interface can show them on its MySQL clients page: which side answered each server's statements, the age of the copy, the worker pool. An address that cannot be used does not stop the start: it prints a warning and the page shows the problem instead of the numbers.

- Default: none: the web interface shows nothing about the router.
- Example: `console-router-url = http://127.0.0.1:9091`
- Environment variable: `BINTRAIL_CONSOLE_ROUTER_URL`
- Read by: watch.

### `console-pg-router-url`

Address where the PostgreSQL router (bintrail-console pg-router) serves its status (the router's status-addr), so the web interface's Snapshots page can say, for each PostgreSQL server, whether the router answers it in fresh or fast mode. An unusable address only prints a warning; capture starts as usual.

- Default: none: the page says the mode cannot be shown.
- Example: `console-pg-router-url = http://127.0.0.1:9092`
- Environment variable: `BINTRAIL_CONSOLE_PG_ROUTER_URL`
- Read by: watch.

### `console-flashback-listen`

Old setting that used to open the MySQL-protocol port inside watch. It no longer opens anything: the port is served by the router and turned on or off in the web interface. Its one remaining effect, in watch, is that if the port has no saved setting yet it is saved as on, so an older installation keeps its port; the router only prints a warning that it is not applied. Remove it and use router-listen instead.

- Default: none.
- Example: `console-flashback-listen = 127.0.0.1:3307`
- Environment variable: `BINTRAIL_CONSOLE_FLASHBACK_LISTEN`
- Read by: watch (one-time migration of the port setting); router (only warns it is ignored).

### `memory-share`

Fraction of the machine's memory (or of the container's memory limit) this process plans its memory with, from above 0 up to 1. Use it when several DBTrail processes share one host, so their sizing adds up instead of each assuming it has the whole machine; it scales the snapshot jobs, the copy's in-memory change buffers and the SQL worker pool. A value that is not a fraction prints a warning and the whole host is used.

- Default: 1 (the whole host or container limit).
- Example: `memory-share = 0.4`
- Environment variable: `BINTRAIL_MEMORY_SHARE`
- Read by: watch, router, pg-router (any bintrail-console process that sizes memory; serve too where it runs SQL).

## The MySQL port (bintrail-console router)

### `router-listen`

Address (host:port) the MySQL-protocol port listens on. This is the port SQL clients connect to; it is served by the router process.

- Default: 127.0.0.1:3309.
- Example: `router-listen = 0.0.0.0:3309`
- Environment variable: `BINTRAIL_ROUTER_LISTEN`
- Read by: router.

### `router-metrics-addr`

Address where the router exposes Prometheus metrics and /status (its numbers as JSON, which the web interface reads through console-router-url). Neither asks for a password. If the address cannot be opened, the MySQL port is still served, without numbers.

- Default: none: no metrics or status are served.
- Example: `router-metrics-addr = 127.0.0.1:9091`
- Environment variable: `BINTRAIL_ROUTER_METRICS_ADDR`
- Read by: router.

### `router-concurrency`

How many statements run on the copy at the same time, 1 to 64 (similar to innodb_thread_concurrency). Empty or 0 lets the router work it out from the cores and memory it may use, one statement per two cores; the rest wait their turn.

- Default: worked out from the machine (one statement per two cores, limited by memory).
- Example: `router-concurrency = 8`
- Environment variable: `BINTRAIL_ROUTER_CONCURRENCY`
- Read by: router.

### `router-max-connections`

How many clients may be connected to the MySQL port at once, 1 to 1024. One more is refused with error 1040 (Too many connections), as a MySQL server at its max_connections does.

- Default: 256.
- Example: `router-max-connections = 512`
- Environment variable: `BINTRAIL_ROUTER_MAX_CONNECTIONS`
- Read by: router.

### `router-max-wait`

How long a statement waits for a free worker on the copy when all are busy, written with a unit like 30s or 2m. Past it the client gets an error (1040 on a routed connection, 1203 otherwise). A bare number without a unit is refused; empty or 0 means it waits as long as the client stays connected.

- Default: no limit.
- Example: `router-max-wait = 30s`
- Environment variable: `BINTRAIL_ROUTER_MAX_WAIT`
- Read by: router.

### `router-statement-table`

When true, the router keeps a table of every statement shape each side answered (how many times, how long, how many failed; values blanked but table and column names kept) and publishes it on the metrics address. That address has no password, so anyone who reaches it can read the table and column names your clients query.

- Default: off.
- Example: `router-statement-table = true`
- Environment variable: `BINTRAIL_ROUTER_STATEMENT_TABLE`
- Read by: router.

### `router-tls-cert`

Certificate file (PEM, with its chain after it if any) the MySQL port offers to clients that ask for encryption, so they can verify it (ssl-mode VERIFY_CA or VERIFY_IDENTITY). Must be set together with router-tls-key; read only when the router starts.

- Default: none: clients that ask for encryption get a certificate made up at start that they cannot verify.
- Example: `router-tls-cert = /etc/dbtrail/tls/router.crt`
- Environment variable: `BINTRAIL_ROUTER_TLS_CERT`
- Read by: router.

### `router-tls-key`

Private key file (PEM, not encrypted) that goes with router-tls-cert for the MySQL port. Must be set together with the certificate, or the router refuses to start.

- Default: none.
- Example: `router-tls-key = /etc/dbtrail/tls/router.key`
- Environment variable: `BINTRAIL_ROUTER_TLS_KEY`
- Read by: router.

## The PostgreSQL port (bintrail-console pg-router)

### `pg-router-listen`

Address (host:port) the PostgreSQL-protocol port listens on, where psql and PostgreSQL drivers connect.

- Default: 127.0.0.1:5433.
- Example: `pg-router-listen = 0.0.0.0:5433`
- Environment variable: `BINTRAIL_PG_ROUTER_LISTEN`
- Read by: pg-router.

### `pg-router-tls-cert`

Certificate file (PEM, with its chain after it if any) the PostgreSQL port offers to clients that ask for TLS, so they can verify it (sslmode verify-ca or verify-full). The same file used by the MySQL port works here. Must be set together with pg-router-tls-key; read only at start.

- Default: none: clients that ask for TLS get a certificate made up at start that they cannot verify.
- Example: `pg-router-tls-cert = /etc/dbtrail/tls/router.crt`
- Environment variable: `BINTRAIL_PG_ROUTER_TLS_CERT`
- Read by: pg-router.

### `pg-router-tls-key`

Private key file (PEM, not encrypted) that goes with pg-router-tls-cert for the PostgreSQL port. Must be set together with the certificate.

- Default: none.
- Example: `pg-router-tls-key = /etc/dbtrail/tls/router.key`
- Environment variable: `BINTRAIL_PG_ROUTER_TLS_KEY`
- Read by: pg-router.

### `pg-router-workers`

How many statements the PostgreSQL router runs on the copy at the same time, 1 to 16. The rest wait their turn.

- Default: 4.
- Example: `pg-router-workers = 8`
- Environment variable: `BINTRAIL_PG_ROUTER_WORKERS`
- Read by: pg-router.

### `pg-router-threads`

How many CPU threads each statement on the copy may use in the PostgreSQL router, 1 to 16.

- Default: 2.
- Example: `pg-router-threads = 4`
- Environment variable: `BINTRAIL_PG_ROUTER_THREADS`
- Read by: pg-router.

### `pg-router-max-connections`

How many clients may be connected to the PostgreSQL port at once, 1 to 1024. One more is refused with an error.

- Default: 256.
- Example: `pg-router-max-connections = 512`
- Environment variable: `BINTRAIL_PG_ROUTER_MAX_CONNECTIONS`
- Read by: pg-router.

### `pg-router-max-source-connections`

How many connections to your own PostgreSQL servers the PostgreSQL router holds at once, across all servers, 1 to 1024. A client uses one while a statement the source answers (or its transaction) runs there; past the limit that statement waits for one to be given back, up to 30 seconds, and is then refused with error 53300. This keeps the port from using up the source's own connection slots.

- Default: 16.
- Example: `pg-router-max-source-connections = 32`
- Environment variable: `BINTRAIL_PG_ROUTER_MAX_SOURCE_CONNECTIONS`
- Read by: pg-router.

### `pg-router-cost-threshold`

For a server with a forwarding user: a statement the copy answers as PostgreSQL does goes to the copy only when it is expensive on your PostgreSQL. This is the plan cost that counts as expensive: the Total Cost at the top of the plan EXPLAIN gives on the source, asked before the statement runs. Below it, and with no sequential scan over BINTRAIL_PG_ROUTER_SCAN_ROWS rows, the source answers, by its indexes (a read by a primary key costs about 1 to 12). 0 turns this rule off.

- Default: 50000.
- Example: `pg-router-cost-threshold = 100000`
- Environment variable: `BINTRAIL_PG_ROUTER_COST_THRESHOLD`
- Read by: pg-router.

### `pg-router-scan-rows`

For a server with a forwarding user: a statement the copy answers as PostgreSQL does goes to the copy when the plan EXPLAIN gives on the source has a sequential scan (any Seq Scan, at any depth) estimated at this many rows or more, whatever its cost. 0 turns this rule off; it cannot be 0 together with BINTRAIL_PG_ROUTER_COST_THRESHOLD.

- Default: 1000000.
- Example: `pg-router-scan-rows = 500000`
- Environment variable: `BINTRAIL_PG_ROUTER_SCAN_ROWS`
- Read by: pg-router.

### `pg-router-max-wait`

How long a statement waits for a free worker in the PostgreSQL router when all are busy, written with a unit like 30s or 2m; past it the client gets an error. Empty or 0 sets no limit of its own, and the five minutes a single statement may take (wait included) is what ends it.

- Default: no limit of its own (the 5-minute statement limit applies).
- Example: `pg-router-max-wait = 30s`
- Environment variable: `BINTRAIL_PG_ROUTER_MAX_WAIT`
- Read by: pg-router.

### `pg-router-status-addr`

Address (host:port) where the PostgreSQL router serves /status (JSON saying, per server, which side answered each statement and why) and /metrics (Prometheus). It asks for no password. The web interface reads it through console-pg-router-url.

- Default: none: no status or metrics are served.
- Example: `pg-router-status-addr = 127.0.0.1:9092`
- Environment variable: `BINTRAIL_PG_ROUTER_STATUS_ADDR`
- Read by: pg-router.

### `pg-router-status-statements`

When true, the PostgreSQL router keeps a row per statement shape and side in /status (values blanked, table and column names kept). Anyone who reaches the status address can then read the names of the tables and columns your clients query.

- Default: off.
- Example: `pg-router-status-statements = true`
- Environment variable: `BINTRAIL_PG_ROUTER_STATUS_STATEMENTS`
- Read by: pg-router.

## SQL on the copy: routing and fresh mode, both ports

### `console-sql-port-max-rows`

The most rows one statement on the SQL port may return. A result with more rows is refused with an error (1104 on the MySQL port), never silently cut. It does not change the SQL page of the web interface, which shows 1,000 rows.

- Default: 10000.
- Example: `console-sql-port-max-rows = 50000`
- Environment variable: `BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS`
- Read by: router, pg-router.

### `console-route-read-only`

Whether the MySQL port refuses every statement that is not a read and never sends it to the source. Set 0 or false to let writes be forwarded to the source. It judges by the statement's text, so a stored function that writes is not detected: the forwarding account's grants are the real guard.

- Default: on (read-only).
- Example: `console-route-read-only = false`
- Environment variable: `BINTRAIL_CONSOLE_ROUTE_READ_ONLY`
- Read by: router.

### `console-route-cost-threshold`

A SELECT whose MySQL query plan cost is at least this number is answered from the copy instead of the source (a single-row lookup costs about 1; a full scan of 200,000 rows about 20,000). 0 turns this rule off, but it cannot be 0 together with route-scan-rows.

- Default: 10000.
- Example: `console-route-cost-threshold = 5000`
- Environment variable: `BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD`
- Read by: router.

### `console-route-scan-rows`

A SELECT whose plan reads a whole table of at least this many rows is answered from the copy instead of the source. On a MariaDB source, whose plans carry no comparable cost, large index scans, large index ranges without a limiting LIMIT, and joins reading at least this many rows in total count too. 0 turns this rule off.

- Default: 100000.
- Example: `console-route-scan-rows = 200000`
- Environment variable: `BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS`
- Read by: router.

### `sql-tails`

Forces one mode for every server on the SQL ports. on (or fresh, true, 1, yes) puts every server in fresh mode, where statements also read the changes captured since the last snapshot; off (or fast, false, 0, no) puts every server in fast mode, which reads only the snapshot. Unset, or any other value, each server uses the mode chosen for it in the web interface.

- Default: unset: each server's own choice applies (fresh unless changed in the web interface).
- Example: `sql-tails = fast`
- Environment variable: `BINTRAIL_SQL_TAILS`
- Read by: router, pg-router.

### `sql-fresh-wait`

In fresh mode on the MySQL port, makes a statement first wait until capture has caught up with where the source's binary log stands, so a row written a moment ago is visible. on waits up to 200ms, or give a time from 1ms to 10s; past it the statement answers with what is ready. Needs the REPLICATION CLIENT privilege on the source to read its position.

- Default: off (statements do not wait).
- Example: `sql-fresh-wait = 200ms`
- Environment variable: `BINTRAIL_SQL_FRESH_WAIT`
- Read by: router.

### `sql-tail-max-keys`

In fresh mode, the most changed rows (distinct keys) kept in memory per table for the copy, before the table falls back to reading the snapshot until the next refresh. A whole number above 0; a bad value prints a warning and the default is used.

- Default: 500000 per table for a MySQL or MariaDB server; 2000000 for a PostgreSQL server whose tails have a memory bound (sql-tail-max-bytes).
- Example: `sql-tail-max-keys = 1000000`
- Environment variable: `BINTRAIL_SQL_TAIL_MAX_KEYS`
- Read by: router, pg-router.

### `sql-tail-max-bytes`

In fresh mode, the most memory all of one server's in-memory change buffers may hold together, as a size like 1GiB or 512MB; 0 or off means no limit. A bad value prints a warning and the default is used.

- Default: one fifth of the memory this process may use (see memory-share); no limit if the machine's memory cannot be read.
- Example: `sql-tail-max-bytes = 2GiB`
- Environment variable: `BINTRAIL_SQL_TAIL_MAX_BYTES`
- Read by: router, pg-router.

## PostgreSQL sources (bintrail-pg)

### `pg-repl-dsn`

Connection string to the PostgreSQL source opened in replication mode (it must include replication=database), used to read changes through a logical replication slot. bintrail-pg stream requires it; bintrail-pg baseline uses it only to create the slot when it does not exist yet.

- Default: none: stream refuses to start.
- Example: `pg-repl-dsn = postgres://repl_user:password@pg-host:5432/appdb?replication=database`
- Environment variable: `BINTRAIL_PG_REPL_DSN`
- Read by: bintrail-pg (stream, baseline).
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

### `pg-query-dsn`

Ordinary connection string to the PostgreSQL source. stream uses it to look up table and primary key details; baseline runs the snapshot copy on it; doctor runs its checks on it; reset uses it to drop the replication slot.

- Default: none: stream, baseline and doctor refuse to start (reset needs it unless --index-only).
- Example: `pg-query-dsn = postgres://app_reader:password@pg-host:5432/appdb`
- Environment variable: `BINTRAIL_PG_QUERY_DSN`
- Read by: bintrail-pg (stream, baseline, doctor, reset).
- Holds a secret: `dbtrail config show` never prints it, and a file every user can read that sets it gets a warning.

### `pg-slot`

Name of the PostgreSQL logical replication slot that keeps the source's change log for DBTrail. stream creates it if missing; baseline anchors its snapshot to it; doctor inspects it; reset drops it.

- Default: none: required.
- Example: `pg-slot = dbtrail_slot`
- Environment variable: `BINTRAIL_PG_SLOT`
- Read by: bintrail-pg (stream, baseline, doctor, reset).

### `pg-publication`

Name of the PostgreSQL publication that lists which tables are captured. DBTrail only checks and uses it; it does not create it.

- Default: none: required.
- Example: `pg-publication = dbtrail_pub`
- Environment variable: `BINTRAIL_PG_PUBLICATION`
- Read by: bintrail-pg (stream, baseline, doctor).

### `pg-start-lsn`

Position in the PostgreSQL change log (an LSN, like 0/1A2B3C4) where the first run of bintrail-pg stream starts reading. Ignored once a saved checkpoint exists.

- Default: none: start where the slot stands.
- Example: `pg-start-lsn = 0/1A2B3C4`
- Environment variable: `BINTRAIL_PG_START_LSN`
- Read by: bintrail-pg (stream).

### `pg-flashback-listen`

The address (host:port) where bintrail-pg flashback (time travel for one row over the PostgreSQL protocol) listens for psql and other PostgreSQL clients.

- Default: 127.0.0.1:5433.
- Example: `pg-flashback-listen = 127.0.0.1:5434`
- Environment variable: `BINTRAIL_PG_FLASHBACK_LISTEN`
- Read by: bintrail-pg (flashback).

### `pg-flashback-tls-cert`

Certificate file (PEM, with its chain after it) bintrail-pg flashback offers to clients that ask for TLS, so they can verify it (sslmode verify-ca or verify-full). Must be set with the key; read at start.

- Default: none: clients that ask for TLS get a certificate made up at start.
- Example: `pg-flashback-tls-cert = /etc/dbtrail/tls/flashback.crt`
- Environment variable: `BINTRAIL_PG_FLASHBACK_TLS_CERT`
- Read by: bintrail-pg (flashback).

### `pg-flashback-tls-key`

Private key file (PEM, not encrypted) that goes with the bintrail-pg flashback TLS certificate.

- Default: none.
- Example: `pg-flashback-tls-key = /etc/dbtrail/tls/flashback.key`
- Environment variable: `BINTRAIL_PG_FLASHBACK_TLS_KEY`
- Read by: bintrail-pg (flashback).

### `pg-shim-config`

The path of the tenants file (shim.yaml: users, passwords and the schemas each may read) that bintrail-pg flashback checks logins against.

- Default: shim.yaml (in the working directory).
- Example: `pg-shim-config = /etc/dbtrail/shim.yaml`
- Environment variable: `BINTRAIL_PG_SHIM_CONFIG`
- Read by: bintrail-pg (flashback).

### `pg-baseline-dir`

Local folder with baseline snapshots, which lets bintrail-pg flashback answer _snapshot queries for rows that were not changed in the captured window. If baseline-s3 is also set, S3 wins.

- Default: none: _snapshot has no baseline.
- Example: `pg-baseline-dir = /var/lib/dbtrail/pg-baselines`
- Environment variable: `BINTRAIL_PG_BASELINE_DIR`
- Read by: bintrail-pg (flashback).

### `pg-baseline-s3`

S3 location (prefix) of baseline snapshots for bintrail-pg flashback's _snapshot queries. Takes precedence over the local baseline folder.

- Default: none.
- Example: `pg-baseline-s3 = s3://my-bucket/dbtrail/pg-baselines/`
- Environment variable: `BINTRAIL_PG_BASELINE_S3`
- Read by: bintrail-pg (flashback).

## Usage telemetry

### `telemetry`

Turns anonymous usage statistics (command names, version, OS, success or error class; never data, schemas, hostnames or paths) on or off. Accepts on/off, true/false, 1/0, yes/no, enabled/disabled. The --telemetry flag wins over it, DO_NOT_TRACK=1 wins over everything, and it wins over the choice saved with the telemetry command; CI environments never send.

- Default: on (unless a choice was saved with the telemetry command).
- Example: `telemetry = off`
- Environment variable: `BINTRAIL_TELEMETRY`
- Read by: bintrail (all commands), bintrail-pg, bintrail-console (watch, serve, router, pg-router); the web interface also shows the state.

### `telemetry-dir`

Folder where telemetry keeps its own files: the saved on/off choice, the queue of unsent reports and a daemon's install record. Set it in containers so an opt-out survives the container being recreated. It must be an absolute path; otherwise telemetry turns off. It never turns telemetry on.

- Default: ~/.config/bintrail.
- Example: `telemetry-dir = /var/lib/dbtrail/telemetry`
- Environment variable: `BINTRAIL_TELEMETRY_DIR`
- Read by: bintrail (all commands), bintrail-pg, bintrail-console (all commands).

### `telemetry-debug`

When set (to anything but 0), telemetry prints diagnostic lines to stderr explaining what it does and why it fails, for example an unwritable queue folder. Telemetry is otherwise silent by design.

- Default: off.
- Example: `telemetry-debug = 1`
- Environment variable: `BINTRAIL_TELEMETRY_DEBUG`
- Read by: bintrail (all commands), bintrail-pg, bintrail-console (all commands).
