# Bintrail Demo

> **Want the 30-second version?** The [demo image](../docs/demo.md)
> packs this whole rig (minus Grafana) into one `docker run` — MySQL, bintrail,
> ProxySQL and traffic, preconfigured for Time Travel SQL.


One command spins up a self-contained environment: MySQL with binlog enabled, a traffic generator, and bintrail running init → snapshot → stream. Everything is indexed live. You query from the host.

## Start

```bash
cd demo
docker compose up --build
```

Wait ~30 seconds for MySQL to initialize and bintrail to start streaming. You'll know it's ready when you see:

```
bintrail-1  | Streaming started (server-id=99999, checkpoint=5s)
bintrail-1  | Checkpoint saved: mysql-bin.000003 pos=... events=...
```

## Connect from the host

```bash
export BINTRAIL_INDEX_DSN='root:demo@tcp(127.0.0.1:3307)/bintrail_index'
```

MySQL is on port **3307** (not 3306) to avoid conflicts with a local MySQL.

## Useful queries

```bash
# How many events are indexed?
bintrail status --index-dsn "$BINTRAIL_INDEX_DSN"

# Last 20 events across all demo tables
bintrail query --index-dsn "$BINTRAIL_INDEX_DSN" --schema demo --format table --limit 20

# Watch customer deletions (explicit child-first deletes across 4 tables per cycle)
bintrail query --index-dsn "$BINTRAIL_INDEX_DSN" --schema demo --table customers --event-type DELETE --format table

# See the trigger-generated audit log
bintrail query --index-dsn "$BINTRAIL_INDEX_DSN" --schema demo --table audit_log --format json --limit 5

# PK-less tables (recovery uses all-columns WHERE fallback)
bintrail query --index-dsn "$BINTRAIL_INDEX_DSN" --schema demo --table event_log --format table

# Generate reversal SQL for deleted customers (most recent 10)
bintrail recover --index-dsn "$BINTRAIL_INDEX_DSN" --schema demo --table customers --event-type DELETE --dry-run --limit 10

# Same but save to a file
bintrail recover --index-dsn "$BINTRAIL_INDEX_DSN" --schema demo --table customers --event-type DELETE --output /tmp/undo.sql
```

## MCP tools (Claude integration)

Create a `.mcp.json` at the repo root (it is gitignored, so it stays yours) to register bintrail as an MCP server:

```json
{"mcpServers":{"bintrail":{"command":"go","args":["run","./cmd/bintrail-mcp"]}}}
```

Set the env var and the `query`, `recover`, and `status` tools work automatically in Claude Code on this machine:

```bash
export BINTRAIL_INDEX_DSN='root:demo@tcp(127.0.0.1:3307)/bintrail_index'
```

### Using from Claude Desktop on another machine on the same network

This lets a MacBook (or any machine) on the same network use the bintrail MCP tools in Claude Desktop without installing Go or cloning the repo.

**Step 1 — Start the MCP HTTP server on this machine** (in addition to the demo):

```bash
BINTRAIL_INDEX_DSN='root:demo@tcp(127.0.0.1:3307)/bintrail_index' ./bintrail-mcp --http :8080
```

> Build the binary first if needed: `go build ./cmd/bintrail-mcp`

**Step 2 — Copy the proxy script to the other machine:**

```bash
# Run this on the machine running the demo
scp ~/bintrail/cmd/bintrail-mcp/proxy.py user@<other-machine-ip>:~/proxy.py
```

**Step 3 — Test from the other machine** (before touching Claude Desktop config):

```bash
# Replace 192.168.1.37 with this machine's IP
BINTRAIL_SERVER=http://192.168.1.37:8080/mcp python3 ~/proxy.py <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
EOF
```

You should see two JSON responses. If you do, proceed.

**Step 4 — Configure Claude Desktop** on the other machine.

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "python3",
      "args": ["/Users/you/proxy.py"],
      "env": { "BINTRAIL_SERVER": "http://192.168.1.37:8080/mcp" }
    }
  }
}
```

Restart Claude Desktop. The `query`, `recover`, and `status` tools will appear automatically.

**Troubleshooting remote MCP:**

- **"Both calls failed"**: The MCP HTTP server is not running or unreachable. Verify with `curl` from the other machine (Step 3 above).
- **Tools fail after server restart**: The proxy has a stale `Mcp-Session-Id`. Fix: restart Claude Desktop on the other machine.
- **`str | None` syntax error**: Python version is older than 3.10. The proxy is written for 3.7+ and should not hit this — if you see it, you have an old version of the script; re-copy it.
- **Port blocked**: macOS Firewall may block port 8080. Go to System Settings → Network → Firewall and allow the connection, or temporarily disable it for testing.

## What the traffic generates

Every ~5 seconds the `traffic` container runs a chaos cycle:

| Step | Tables touched | Why it's interesting |
|------|---------------|----------------------|
| INSERT 5 customers | `customers` | Basic INSERTs |
| INSERT orders | `orders` + `customer_stats` | Trigger `trg_orders_after_insert` fires → stats upserted in same GTID |
| INSERT order items | `order_items` | STORED generated column `line_total` appears in binlog |
| UPDATE 3 customers | `customers` + `audit_log` | Trigger `trg_customers_after_update` fires → JSON old/new in audit_log |
| UPDATE order statuses | `orders` | Standard UPDATEs |
| INSERT products | `products` | Has a VIRTUAL column (`price_with_tax`) excluded from binlog — parser logs a column-count mismatch warning, events skipped |
| INSERT into PK-less tables | `event_log`, `metrics` | `pk_values` is empty; recovery uses all-columns WHERE clause |
| DELETE 1 random customer | `customers`, `orders`, `order_items`, `customer_stats` | Explicit child-first deletes in one transaction → 4 tables get DELETE events |
| DELETE old PK-less rows | `event_log`, `metrics` | PK-less DELETEs |

Sysbench runs `oltp_read_write` on the `sbtest` database (2 threads, indefinitely) in parallel.

## What to look for in the logs

```bash
docker compose logs bintrail -f
```

- `Checkpoint saved: ... events=N` — events indexed since stream start, grows ~12k/5s
- `column count mismatch` warnings — expected for the `products` table (VIRTUAL column)
- No other warnings = everything healthy

```bash
docker compose logs traffic -f
```

- `[traffic] Chaos cycle N` — each cycle runs all 9 steps
- sysbench throughput report every 30s

## Stop and restart

```bash
# Stop, keep data
docker compose down

# Restart (bintrail resumes from saved GTID checkpoint — no re-init needed)
docker compose up

# Stop and wipe everything
docker compose down -v
```

After a clean restart (`down` without `-v`), bintrail skips init/snapshot if the tables already exist and resumes streaming from the last checkpoint in `stream_state`.

> **Note:** `down -v` wipes the MySQL data volume. Next `up --build` re-runs from scratch (init + snapshot + stream from current GTID).

## Dashboards

Once everything is running, open **http://localhost:3000** for the Bintrail Stream dashboard (no login required). It shows replication lag, event throughput, batch sizes, and errors — all updating live every 5 seconds.

Raw metrics are also available directly:

```bash
curl -s 'localhost:9090/api/v1/query?query=bintrail_stream_replication_lag_seconds' | jq
```

## Ports and credentials

| | |
|-|-|
| MySQL host (from host machine) | `127.0.0.1:3307` |
| MySQL host (between containers) | `mysql:3306` |
| MySQL root password | `demo` |
| Index database | `bintrail_index` |
| Source databases | `demo`, `sbtest` |
| Bintrail stream server-id | `99999` |
| Grafana (dashboard) | http://localhost:3000 |
| Prometheus (metrics) | http://localhost:9090 |

## Troubleshooting

**bintrail stuck on "Waiting for demo.customers..."**
MySQL is still initializing. Wait a bit longer — init scripts take 10–20s on first run.

**"column count mismatch" warnings for `products`**
Expected. The `products` table has a VIRTUAL generated column (`price_with_tax`) that MySQL excludes from the binlog. The schema snapshot includes it; the binlog doesn't. Bintrail skips those events and logs a warning.

**traffic container restarting**
Run `docker compose logs traffic` to see the error. Most common cause: ran `docker compose up` without `-v` after a failed first start — the sysbench tables from the previous run already exist. Fixed automatically by `sysbench cleanup` before `prepare`. If it persists, `docker compose down -v && docker compose up`.

**Port 3307 already in use**
Change the port mapping in `compose.yml`: `"3308:3306"` and update `BINTRAIL_INDEX_DSN` accordingly.
