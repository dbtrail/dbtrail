# Benchmark: HTAP on MariaDB with ProxySQL, and a DuckDB-engine replica

This is the full procedure behind the rerun of the ProxySQL post
[HTAP on MariaDB with ProxySQL](https://proxysql.com/blog/htap-mariadb-proxysql). It measures
three things with the same application load:

1. **The extra hop.** The same primary-key lookups sent straight to MariaDB and through
   DBTrail's MySQL port, idle and under load, with the EXPLAIN time on the source split out.
2. **The original benchmark.** An application at a fixed 400 transactions per second with three
   heavy reports running beside it, first on MariaDB and then sent to DBTrail by ProxySQL.
3. **An async replica with DuckDB-engine tables.** A MariaDB 11.4 replica of the same source
   whose tables use MariaDB's DuckDB storage engine, given the same reports.

Run on 2026-11-16 in AWS us-west-2. Versions: MariaDB 10.11.14 (source), DBTrail 0.128.1 for the
benchmark and 0.133.0 for the hop (see [Versions](#versions)), ProxySQL 3.1.12, sysbench 1.0.20,
MariaDB 11.4.13 with `mariadb-plugin-duckdb` (DuckDB 1.5.5), and the MariaDB 11.4 branch at
commit `095cfb5e468e` built from source.

**Replace `CHANGE_ME` with a password of your own everywhere it appears,** and `SRC`, `DBT` and
`REP` with the private addresses of the source, DBTrail and replica machines.

## Results

### The extra hop

`sysbench oltp_point_select`, autocommit, no prepared statements, 1,000 lookups per second for
120 seconds. DBTrail forwarded every lookup to MariaDB (0 answered by the copy).

| Path | p50 | p95 | p99 |
|---|---|---|---|
| Idle, straight to MariaDB | 0.42 ms | 0.86 ms | 1.12 ms |
| Idle, through DBTrail | 0.97 ms | 1.44 ms | 1.70 ms |
| App at 400 tps, straight | 0.49 ms | 0.92 ms | 1.21 ms |
| App at 400 tps, through DBTrail | 1.04 ms | 1.47 ms | 1.76 ms |

About 0.55 ms more at every percentile. MariaDB's `performance_schema` puts the EXPLAIN that
DBTrail runs before deciding at 0.09 to 0.11 ms; a network round trip between the two machines
was 0.14 ms. The application's own p95 with the lookups through DBTrail was 11.87 ms, against
11.65 ms straight.

### The original benchmark (DBTrail 0.128.1)

| Run (300 s, app at 400 tps) | App p95 | Reports finished | R1 / R2 / R3 average |
|---|---|---|---|
| Reports on MariaDB | 16,819 ms | 8 | 72 / 79 / 148 s |
| Every SELECT to DBTrail | 12.08 ms | 289 | 5.4 / 0.3 / 0.5 s |
| Only the three reports to DBTrail | 11.24 ms | 272 | 5.5 / 0.5 / 0.6 s |

With the application alone its p95 was 11 to 13 ms. Checked with the application stopped: R2
byte-identical, R3 the same rows in another order among ties on `k`, R1 the same values printed
as `119` instead of MariaDB's `119.0000` (#2920).

### The DuckDB-engine replica

- **Published package (11.4.13):** the initial load works; replication stops at the first
  replicated UPDATE with `Storage engine DUCKDB of the table ... doesn't have this option,
  Error_code: 1031`.
- **11.4 branch with MDEV-40957 (read-free apply, In Testing):**

| Replica | Transactions applied per second |
|---|---|
| 1 applier thread | 48 |
| 1 thread, while answering the reports | 29 to 30 |
| 8 threads, optimistic | 213 to 216, stopped after about 10 minutes ([MDEV-42299](https://jira.mariadb.org/browse/MDEV-42299)) |
| 16 threads | 177, stopped within a minute (same error) |
| The application writes | 400 |

`innodb_flush_log_at_trx_commit` at 1, 2 and 0 gave 47, 45 and 49 with one thread. With the
applier running, the reports on the replica averaged 0.2 / 0.1 / 0.3 s and the application's p95
was 12.52 ms, on data that falls further behind for as long as the application writes at this
rate. Applying changes to rows the replica does not have raises no error
([MDEV-42300](https://jira.mariadb.org/browse/MDEV-42300)).

### Versions

The hop was measured on 0.133.0 because on 0.128.1 the same lookups fail with error 1273
(`Unknown collation: 'utf8mb4_0900_ai_ci'`): before 0.133.0 the port forwarded every statement
under a MySQL 8 collation that MariaDB 10.11 does not have (#2810). The benchmark was run on
0.128.1 because on 0.133.0 the copy does not answer a `utf8mb3` connection (#2917), which is what
the stock MariaDB client and a client behind ProxySQL that names no character set get, nor
`AVG(LENGTH(col))` under the MariaDB client's default collation (#2918).

### Not measured

DBTrail's freshness under this load (the original post measured 0.9 to 3.0 seconds behind the
source in fresh mode), the replica on a larger instance or under a lighter write load, and runs
longer than 5 minutes.

## 1. Machines

Four Ubuntu 24.04 arm64 instances in one subnet, with a security group that allows all traffic
between its members and nothing from outside:

| Role | Type | Disk |
|---|---|---|
| Source (`SRC`) | m7g.2xlarge (8 vCPU, 32 GB) | 200 GB gp3 |
| DBTrail (`DBT`) | m7g.2xlarge | 200 GB gp3 |
| Replica (`REP`) | m7g.2xlarge (built on a c7g.8xlarge for the branch, then resized) | 200 GB gp3 |
| Client: ProxySQL and sysbench | m7g.xlarge | 40 GB gp3 |

## 2. Source: MariaDB 10.11 and the data

```bash
apt-get update -y && apt-get install -y mariadb-server sysbench
cat > /etc/mysql/mariadb.conf.d/90-bench.cnf <<'C'
[mysqld]
bind-address=0.0.0.0
server_id=1
log_bin=/var/lib/mysql/mariadb-bin
binlog_format=ROW
binlog_row_image=FULL
innodb_buffer_pool_size=6G
innodb_log_file_size=2G
performance_schema=ON
max_connections=1000
character-set-server=utf8mb4
collation-server=utf8mb4_general_ci
C
systemctl restart mariadb
mariadb -e "CREATE DATABASE sbtest;
CREATE USER app@'%' IDENTIFIED BY 'app-pw'; GRANT ALL ON sbtest.* TO app@'%';
CREATE USER dbtrail@'%' IDENTIFIED BY 'CHANGE_ME';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, RELOAD, PROCESS, SHOW VIEW ON *.* TO dbtrail@'%';
CREATE USER app_fwd@'%' IDENTIFIED BY 'CHANGE_ME'; GRANT SELECT ON sbtest.* TO app_fwd@'%';
CREATE USER repl@'%' IDENTIFIED BY 'CHANGE_ME';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, RELOAD, SHOW VIEW, EVENT, TRIGGER, LOCK TABLES ON *.* TO repl@'%';
CREATE USER monitor@'%' IDENTIFIED BY 'monitor'; GRANT USAGE, REPLICATION CLIENT ON *.* TO monitor@'%';
CREATE USER bench@'%' IDENTIFIED BY 'CHANGE_ME'; GRANT ALL ON *.* TO bench@'%';"
sysbench oltp_read_write --db-driver=mysql --mysql-socket=/run/mysqld/mysqld.sock --mysql-user=root \
  --mysql-db=sbtest --tables=8 --table-size=10000000 --threads=8 prepare    # about 17 minutes
```

## 3. DBTrail

```bash
apt-get install -y docker.io docker-compose-v2 mariadb-client jq
curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | DBTRAIL_DIR=/root/dbtrail sh
cd /root/dbtrail
# Pin the version in .env: a BINTRAIL_TAG given only to the installer lasts for that run.
sed -i '/^BINTRAIL_TAG=/d' .env; echo BINTRAIL_TAG=0.128.1 >> .env
echo CONSOLE_TOKEN=$(openssl rand -hex 24) >> .env
# Publish the MySQL port to the subnet: ProxySQL runs on another machine.
sed -i 's/ports: \["127.0.0.1:3309:3309"\]/ports: ["3309:3309"]/' docker-compose.yml
docker compose down -v && docker compose up -d
printf '[client]\nhost=SRC\nuser=dbtrail\npassword=CHANGE_ME\n' > src.cnf
./dbtrail server add --defaults-file src.cnf --name sbtest
./dbtrail snapshot run sbtest                    # first copy, about 8 minutes
./dbtrail snapshot schedule sbtest --every 1h
./dbtrail port on
./dbtrail port routing on
T=$(grep '^CONSOLE_TOKEN=' .env | cut -d= -f2); A=http://127.0.0.1:8090/api
H="Authorization: Bearer $T"; J='Content-Type: application/json'
ID=$(curl -s -H "$H" $A/servers | jq -r '.servers[0].id')
curl -s -X PUT -H "$H" -H "$J" $A/servers/$ID/route       -d '{"user":"app_fwd","password":"CHANGE_ME"}'
curl -s -X PUT -H "$H" -H "$J" $A/servers/$ID/sql-mode    -d '{"fresh":true}'
curl -s -X PUT -H "$H" -H "$J" $A/servers/$ID/port-login  -d '{"user":"app","password":"app-pw"}'
curl -s -X PUT -H "$H" -H "$J" $A/flashback/monitor-login -d '{"user":"monitor","password":"monitor"}'
```

For the hop measurement on 0.133.0, set `BINTRAIL_TAG=0.133.0` instead. From 0.131.0 on,
`server add` turns the port and routing on by itself and takes the forwarding user from a
`[dbtrail-forward]` section of the same option file.

Check: `mysql -h DBT -P 3309 -u app -papp-pw -e "SHOW ROUTER STATUS"` answers for `sbtest`.

## 4. ProxySQL on the client machine

```bash
apt-get install -y sysbench mariadb-client bc
curl -fsSLO https://github.com/sysown/proxysql/releases/download/v3.1.12/proxysql_3.1.12-ubuntu24_arm64.deb
apt-get install -y ./proxysql_3.1.12-ubuntu24_arm64.deb && systemctl start proxysql
mysql -h127.0.0.1 -P6032 -uadmin -padmin -e "
UPDATE global_variables SET variable_value='monitor'
  WHERE variable_name IN ('mysql-monitor_username','mysql-monitor_password');
LOAD MYSQL VARIABLES TO RUNTIME;
INSERT INTO mysql_servers (hostgroup_id, hostname, port, comment)
  VALUES (10,'SRC',3306,'mariadb'), (20,'DBT',3309,'dbtrail'), (30,'REP',3306,'duckdb-replica');
LOAD MYSQL SERVERS TO RUNTIME;
INSERT INTO mysql_users (username, password, default_hostgroup, transaction_persistent)
  VALUES ('app','app-pw',10,1);
LOAD MYSQL USERS TO RUNTIME;"
```

`run.sh` and `hop.sh` below set the query rules for each run.

## 5. The DuckDB-engine replica

Published package:

```bash
curl -LsS https://r.mariadb.com/downloads/mariadb_repo_setup | bash -s -- --mariadb-server-version=mariadb-11.4
apt-get install -y mariadb-server mariadb-plugin-duckdb
```

Or the 11.4 branch with MDEV-40957, built on a large instance (about 20 minutes on 32 vCPU):

```bash
git clone --depth 1 -b 11.4 --recurse-submodules --shallow-submodules https://github.com/MariaDB/server.git mdb
cd mdb/storage/duckdb
sed -i 's/libxcrypt-dev/libcrypt-dev/' build.sh     # the package's name on Ubuntu 24.04
./build.sh -D -t RelWithDebInfo -d ubuntu:24.04 -S
```

Configuration (`/etc/mysql/mariadb.conf.d/90-bench.cnf` for the package, `/etc/my.cnf.d/90-bench.cnf`
for the build, which also needs `user=mysql`):

```ini
[mysqld]
bind-address=0.0.0.0
server_id=3
innodb_buffer_pool_size=2G
character-set-server=utf8mb4
collation-server=utf8mb4_general_ci
max_connections=1000
relay_log=/var/lib/mysql/relay-bin
```

Do not add `skip-ssl` here: MariaDB 11.4's own client requires TLS by default and stops
connecting to the server.

Tables, initial load and replication. The engine refuses `AUTO_INCREMENT`, so the tables are
created without it. A 10.11 source offers no TLS, hence `--skip-ssl` and `MASTER_SSL=0`:

```bash
mariadb -e "CREATE DATABASE sbtest"
for i in 1 2 3 4 5 6 7 8; do
  mariadb sbtest -e "CREATE TABLE sbtest$i (id INT NOT NULL, k INT NOT NULL DEFAULT 0,
    c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '',
    PRIMARY KEY (id), KEY k_1 (k)) ENGINE=DuckDB"
done
mariadb-dump --skip-ssl -h SRC -urepl -pCHANGE_ME --single-transaction --master-data=2 \
  --no-create-info --skip-triggers --skip-add-locks --skip-disable-keys sbtest > dump.sql
grep -m1 "CHANGE MASTER" dump.sql                  # the binlog file and position
mariadb sbtest < dump.sql                          # about 11 minutes for 80M rows
mariadb -e "CHANGE MASTER TO MASTER_HOST='SRC', MASTER_USER='repl', MASTER_PASSWORD='CHANGE_ME',
  MASTER_LOG_FILE='<file>', MASTER_LOG_POS=<pos>, MASTER_SSL=0; START SLAVE;"
mariadb -e "CREATE USER app@'%' IDENTIFIED BY 'app-pw'; GRANT SELECT ON sbtest.* TO app@'%';
  CREATE USER monitor@'%' IDENTIFIED BY 'monitor';
  GRANT USAGE, REPLICATION CLIENT, SLAVE MONITOR ON *.* TO monitor@'%';"
```

Parallel apply, and the apply rate (sysbench's `oltp_read_write` deletes one row per
transaction):

```bash
mariadb -e "STOP SLAVE SQL_THREAD; SET GLOBAL slave_parallel_threads=8;
  SET GLOBAL slave_parallel_mode='optimistic'; START SLAVE SQL_THREAD;"
a=$(mariadb -N -e "SHOW STATUS LIKE 'Duckdb_rows_delete_in_batch'" | cut -f2); sleep 60
b=$(mariadb -N -e "SHOW STATUS LIKE 'Duckdb_rows_delete_in_batch'" | cut -f2)
echo "$(( (b - a) / 60 )) transactions/s"
```

## 6. The scripts (client machine)

Save these three in `/root` and `chmod +x` them.

`lag.sh` samples the replica's lag every 5 seconds during a run:

```bash
#!/bin/bash
OUT=$1; SECS=$2; end=$(( $(date +%s) + SECS ))
echo "time,io_sql,seconds_behind" > "$OUT/lag.csv"
while [ "$(date +%s)" -lt "$end" ]; do
  s=$(mysql -hREP -umonitor -pmonitor -e "SHOW SLAVE STATUS\G" 2>/dev/null |
      awk '/Slave_IO_Running:|Slave_SQL_Running:|Seconds_Behind_Master:/{print $2}' | tr '\n' ' ')
  set -- $s
  echo "$(date -u +%T),$1/$2,$3" >> "$OUT/lag.csv"
  sleep 5
done
```

`run.sh` is one run of the benchmark: `run.sh <label> <rules> <reports 0|1> [seconds]`.

```bash
#!/bin/bash
#   none            every statement to MariaDB (hostgroup 10)
#   dbtrail         locking reads to MariaDB, every other SELECT to DBTrail (hostgroup 20)
#   dbtrail-reports only the three reports to DBTrail (hostgroup 20)
#   replica-reports only the three reports to the DuckDB replica (hostgroup 30)
set -u
LABEL=$1; RULES=$2; REPORTS=$3; SECS=${4:-300}
OUT=/root/results/$LABEL; mkdir -p "$OUT"
ADMIN="mysql -h127.0.0.1 -P6032 -uadmin -padmin -N"
APP="mysql -h127.0.0.1 -P6033 -uapp -papp-pw sbtest -N"
REP_PAT='^SELECT (k % 100 AS bucket|k % 1000 AS b,|id, k FROM sbtest3 ORDER BY k)'

$ADMIN -e "DELETE FROM mysql_query_rules;"
case "$RULES" in
  dbtrail) $ADMIN -e "INSERT INTO mysql_query_rules (rule_id, active, match_digest, destination_hostgroup, apply)
    VALUES (1, 1, '^SELECT .* FOR (UPDATE|SHARE)', 10, 1), (2, 1, '^SELECT ', 20, 1);" ;;
  dbtrail-reports) $ADMIN -e "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply)
    VALUES (1, 1, '$REP_PAT', 20, 1);" ;;
  replica-reports) $ADMIN -e "INSERT INTO mysql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply)
    VALUES (1, 1, '$REP_PAT', 30, 1);" ;;
esac
$ADMIN -e "LOAD MYSQL QUERY RULES TO RUNTIME; SELECT * FROM stats_mysql_query_digest_reset LIMIT 0;"

R1="SELECT k % 100 AS bucket, COUNT(*), AVG(LENGTH(c)) FROM sbtest1 GROUP BY bucket ORDER BY bucket"
R2="SELECT k % 1000 AS b, COUNT(*), SUM(id) FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 GROUP BY b ORDER BY b"
R3="SELECT id, k FROM sbtest3 ORDER BY k LIMIT 20 OFFSET 5000000"

report_loop() {
  local end=$(( $(date +%s) + SECS ))
  while [ "$(date +%s)" -lt "$end" ]; do
    for q in R1 R2 R3; do
      local t0=$(date +%s.%N)
      if $APP -e "${!q}" >/dev/null 2>"$OUT/err-$1.txt"; then s=ok; else s=error; fi
      echo "$q $s $(echo "$(date +%s.%N) - $t0" | bc)" >> "$OUT/reports-$1.txt"
      [ "$(date +%s)" -ge "$end" ] && break
    done
  done
}

/root/lag.sh "$OUT" "$SECS" &
if [ "$REPORTS" = 1 ]; then report_loop a & report_loop b & fi
sysbench oltp_read_write --db-driver=mysql --mysql-host=127.0.0.1 --mysql-port=6033 \
  --mysql-user=app --mysql-password=app-pw --mysql-db=sbtest --tables=8 --table-size=10000000 \
  --threads=16 --rate=400 --time="$SECS" --report-interval=10 --percentile=95 --histogram=off \
  run > "$OUT/sysbench.txt" 2>&1
wait
$ADMIN -e "SELECT hostgroup, SUM(count_star) FROM stats_mysql_query_digest GROUP BY hostgroup" > "$OUT/per-hostgroup.txt"
grep -E "transactions:|95th percentile|avg:|max:" "$OUT/sysbench.txt"
[ "$REPORTS" = 1 ] && cat "$OUT"/reports-*.txt |
  awk '{n[$1]++; s[$1]+=$3; e[$1]+=($2=="error")} END{for(q in n) printf "%s runs=%d avg=%.1fs errors=%d\n", q, n[q], s[q]/n[q], e[q]}'
cat "$OUT/per-hostgroup.txt"
```

`hop.sh` measures the hop: `hop.sh <label> <direct|dbtrail> <load 0|1> [seconds] [rate]`.

```bash
#!/bin/bash
set -u
LABEL=$1; PATH_=$2; LOAD=$3; SECS=${4:-120}; RATE=${5:-1000}
OUT=/root/results/$LABEL; mkdir -p "$OUT"
ADMIN="mysql -h127.0.0.1 -P6032 -uadmin -padmin -N"
SRCQ="mysql -hSRC -ubench -pCHANGE_ME -N"
DBTQ="mysql -hDBT -P3309 -uapp -papp-pw -N"
$ADMIN -e "DELETE FROM mysql_query_rules;"
[ "$PATH_" = dbtrail ] && $ADMIN -e "INSERT INTO mysql_query_rules (rule_id, active, match_digest, destination_hostgroup, apply)
  VALUES (1, 1, '^SELECT .* FOR (UPDATE|SHARE)', 10, 1), (2, 1, '^SELECT ', 20, 1);"
$ADMIN -e "LOAD MYSQL QUERY RULES TO RUNTIME; SELECT * FROM stats_mysql_query_digest_reset LIMIT 0;"
$SRCQ -e "TRUNCATE performance_schema.events_statements_summary_by_digest"
$DBTQ -e "SHOW ROUTER STATUS" > "$OUT/router-before.txt"
if [ "$LOAD" = 1 ]; then
  sysbench oltp_read_write --db-driver=mysql --mysql-host=127.0.0.1 --mysql-port=6033 \
    --mysql-user=app --mysql-password=app-pw --mysql-db=sbtest --tables=8 --table-size=10000000 \
    --threads=16 --rate=400 --time="$SECS" --report-interval=10 --percentile=95 --histogram=off \
    run > "$OUT/app-sysbench.txt" 2>&1 &
fi
sysbench oltp_point_select --db-driver=mysql --mysql-host=127.0.0.1 --mysql-port=6033 \
  --mysql-user=app --mysql-password=app-pw --mysql-db=sbtest --tables=8 --table-size=10000000 \
  --db-ps-mode=disable --threads=8 --rate="$RATE" --time="$SECS" --report-interval=10 \
  --histogram=on --percentile=99 run > "$OUT/point-sysbench.txt" 2>&1
wait
$DBTQ -e "SHOW ROUTER STATUS" > "$OUT/router-after.txt"
$SRCQ -e "SELECT LEFT(DIGEST_TEXT,70), COUNT_STAR, ROUND(AVG_TIMER_WAIT/1000000,1) AS avg_us
  FROM performance_schema.events_statements_summary_by_digest
  WHERE DIGEST_TEXT LIKE 'EXPLAIN%' OR DIGEST_TEXT LIKE 'SELECT \`c\` FROM \`sbtest%'
  ORDER BY COUNT_STAR DESC LIMIT 6" > "$OUT/ps-digests.txt"
# p50/p95/p99 from sysbench's latency histogram (value in ms, count per bucket).
awk '/Latency histogram/{h=1;next} h && /\|/{gsub(/\|/," "); tot+=$NF; v[n]=$1; c[n]=$NF; n++} /^SQL statistics/{h=0}
  END{split("50 95 99",P," "); for(i=1;i<=3;i++){t=tot*P[i]/100; s=0; for(j=0;j<n;j++){s+=c[j]; if(s>=t){printf "p%s=%s ms  ",P[i],v[j]; break}}} print "n=" tot}' \
  "$OUT/point-sysbench.txt"
cat "$OUT/ps-digests.txt"
paste <(grep statements_ "$OUT/router-before.txt") <(grep statements_ "$OUT/router-after.txt" | cut -f2)
if [ "$LOAD" = 1 ]; then grep -E "95th percentile|transactions:" "$OUT/app-sysbench.txt"; fi
```

## 7. The runs

Warm MariaDB's buffer pool before each measured run: right after the data is loaded or after a
run with reports, the application's p95 is seconds, not milliseconds, until its working set is
back in memory.

```bash
# The benchmark: warm-up, measured run, pause.
/root/run.sh r1-warm none 0 180
/root/run.sh r2-reports-mariadb none 1 300
sleep 120
/root/run.sh r3-warm none 0 180
/root/run.sh r4-reports-dbtrail dbtrail 1 300
/root/run.sh r5-warm none 0 180
/root/run.sh r6-reports-dbtrail-only dbtrail-reports 1 300
/root/run.sh r7-warm none 0 180
/root/run.sh r8-reports-replica replica-reports 1 300   # with the replica applying

# The hop.
/root/hop.sh h1-idle-direct  direct  0
/root/hop.sh h2-idle-dbtrail dbtrail 0
/root/hop.sh h3-load-direct  direct  1
/root/hop.sh h4-load-dbtrail dbtrail 1

# Network round trip from DBTrail to the source (run on the DBTrail machine).
ping -c 200 -i 0.05 -q SRC
```

Check that every report in `r4` and `r6` came from the copy: `SHOW ROUTER STATUS` on port 3309
shows `statements_copy` going up by the number of reports and `statements_mysql` not moving. In
`h2` and `h4` it is the other way around: every lookup goes to `statements_mysql`.

Comparing results with the application stopped:

```bash
for q in R1 R2 R3; do   # R1, R2, R3 as defined in run.sh
  diff <(mysql -hSRC -ubench -pCHANGE_ME sbtest -N -B -e "${!q}") \
       <(mysql -hDBT -P3309 -uapp -papp-pw sbtest -N -B -e "${!q}") > /dev/null && echo "$q same" || echo "$q differs"
done
```

R3 sorts by a column with many ties, so compare it as a set of rows.

## 8. Clean up

Terminate the four instances and delete the security group. Nothing else is created.
