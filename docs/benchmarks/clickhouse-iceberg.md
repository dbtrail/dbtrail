# Benchmark: ClickHouse reading DBTrail's Iceberg export

This is the full procedure behind the blog post
[Querying a live MySQL copy from ClickHouse: it works, with one workaround](https://blog.dbtrail.com/clickhouse-live-mysql-copy/).
It sets up MySQL, DBTrail and ClickHouse on one EC2 machine, exports two tables to Iceberg
every minute under a steady write load, measures how far behind ClickHouse is and how long a
report query takes, compacts with Athena, and compares the result with MySQL row by row.

Versions we ran: MySQL 8.0.46, DBTrail 0.125.3, ClickHouse 26.10.3.7, Altinity Antalya
26.6.8.20002, Athena engine version 3.

Everything below is what we ran, with our bucket and account replaced by placeholders. It takes
about two hours and a few dollars. Commands that start with `aws` run on your own computer with
the AWS CLI; the rest run on the test machine as root.

**Replace `CHANGE_ME` with a password of your own everywhere it appears.** It is the password of
the MySQL user the test creates, and every step uses the same one.

## 1. Set the names

```bash
export AWS_REGION=us-west-2
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export BUCKET=your-bucket          # an existing bucket in the same region
export PREFIX=chbench              # everything the test writes goes under s3://$BUCKET/$PREFIX/
export ROLE=dbtrail-ch-bench       # role and instance profile name
```

## 2. Create the role and the instance profile

The machine needs one role for three things: DBTrail writes the tables to S3 and registers them in Glue, ClickHouse reads them, and Athena compacts them. The role also gets the managed SSM policy, so you can open a shell without SSH or open ports.

```bash
cat > trust.json <<'EOF'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
  "Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF

cat > policy.json <<EOF
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow",
  "Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:AbortMultipartUpload"],
  "Resource":"arn:aws:s3:::$BUCKET/$PREFIX/*"},
 {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],
  "Resource":"arn:aws:s3:::$BUCKET"},
 {"Effect":"Allow",
  "Action":["glue:GetDatabase","glue:GetDatabases","glue:CreateDatabase","glue:DeleteDatabase",
            "glue:GetTable","glue:GetTables","glue:CreateTable","glue:UpdateTable","glue:DeleteTable",
            "glue:GetCatalog","glue:GetCatalogs","glue:GetPartitions"],
  "Resource":["arn:aws:glue:$AWS_REGION:$ACCOUNT:catalog",
              "arn:aws:glue:$AWS_REGION:$ACCOUNT:database/default",
              "arn:aws:glue:$AWS_REGION:$ACCOUNT:database/shop",
              "arn:aws:glue:$AWS_REGION:$ACCOUNT:table/shop/*"]},
 {"Effect":"Allow",
  "Action":["athena:StartQueryExecution","athena:GetQueryExecution","athena:GetQueryResults",
            "athena:StopQueryExecution","athena:GetWorkGroup"],
  "Resource":"arn:aws:athena:$AWS_REGION:$ACCOUNT:workgroup/primary"}]}
EOF

aws iam create-role --role-name $ROLE --assume-role-policy-document file://trust.json
aws iam put-role-policy --role-name $ROLE --policy-name $ROLE --policy-document file://policy.json
aws iam attach-role-policy --role-name $ROLE \
  --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam create-instance-profile --instance-profile-name $ROLE
aws iam add-role-to-instance-profile --instance-profile-name $ROLE --role-name $ROLE
sleep 15   # IAM takes a few seconds before EC2 accepts a new profile
```

The Glue part only covers the `shop` database, because DBTrail names each Glue database after its MySQL schema. Athena writes its own result files under the same S3 prefix.

## 3. Launch the machine

Ubuntu 24.04 on ARM, m7g.xlarge (4 cores, 16 GB), a 40 GB disk. Use any subnet that can reach the internet (public IP or NAT) and a security group with no inbound rules. We used Ubuntu's image from 2026-10-23; the lookup below returns the current one.

```bash
AMI=$(aws ssm get-parameter --query Parameter.Value --output text \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id)

aws ec2 run-instances --image-id $AMI --instance-type m7g.xlarge \
  --iam-instance-profile Name=$ROLE \
  --instance-initiated-shutdown-behavior terminate \
  --subnet-id subnet-XXXX --security-group-ids sg-XXXX --associate-public-ip-address \
  --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=40,VolumeType=gp3,DeleteOnTermination=true}' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=dbtrail-ch-bench}]' \
  --query 'Instances[0].InstanceId' --output text

aws ssm start-session --target i-XXXX    # wait a minute or two for the agent, then: sudo -i
```

For the third run, launch a second machine the same way, with the same profile, for Antalya.

Shutdown behavior `terminate` plus `shutdown -h +240` in the next step means the machine deletes itself after four hours if you forget it.

## 4. Install MySQL, DBTrail, mydumper and ClickHouse

```bash
shutdown -h +240
export DEBIAN_FRONTEND=noninteractive HOME=/root
apt-get update -y && apt-get install -y mysql-server-8.0 curl jq python3-boto3

cat > /etc/mysql/mysql.conf.d/zz-bench.cnf <<'EOF'
[mysqld]
server-id=1
log-bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
binlog_row_metadata=FULL
gtid_mode=ON
enforce_gtid_consistency=ON
bind-address=127.0.0.1
default-time-zone='+00:00'
innodb_buffer_pool_size=2G
EOF
systemctl restart mysql
mysql -e "CREATE USER 'bt'@'127.0.0.1' IDENTIFIED BY 'CHANGE_ME';
          GRANT ALL ON *.* TO 'bt'@'127.0.0.1' WITH GRANT OPTION;"

cd /tmp
curl -fsSLO https://github.com/mydumper/mydumper/releases/download/v1.0.3-1/mydumper_1.0.3-1.noble_arm64.deb
curl -fsSLO https://github.com/dbtrail/dbtrail/releases/download/v0.125.3/bintrail_0.125.3_linux_arm64.deb
apt-get install -y ./mydumper_1.0.3-1.noble_arm64.deb ./bintrail_0.125.3_linux_arm64.deb

mkdir -p /opt/ch && cd /opt/ch && curl -fsSL https://clickhouse.com/ | sh
./clickhouse install --noninteractive
cat > /etc/clickhouse-server/users.d/s3-server-credentials.xml <<'EOF'
<clickhouse><profiles><default>
  <s3_allow_server_credentials_in_user_queries>1</s3_allow_server_credentials_in_user_queries>
</default></profiles></clickhouse>
EOF
cat > /etc/clickhouse-server/config.d/s3-cache.xml <<'EOF'
<clickhouse><filesystem_caches><s3_cache>
  <path>/var/lib/clickhouse/caches/s3_cache/</path><max_size>20Gi</max_size>
</s3_cache></filesystem_caches></clickhouse>
EOF
clickhouse start
clickhouse-client -q "SELECT version()"
```

Ubuntu's package gave us MySQL 8.0.46. The ClickHouse installer fetches the latest release; we ran 26.10.3.7. The credentials file lets ClickHouse use the machine's role for S3 in user queries; without it, every read from S3 fails with `ACCESS_DENIED`. The last file declares the local disk cache for S3 files; ClickHouse only accepts cache paths under `/var/lib/clickhouse/caches/`.

For the Antalya machine, install from Altinity's repository instead of the ClickHouse installer, add the same cache file, and leave the credentials file out: Antalya 26.6.8.20002 doesn't have that setting, won't start with the file in place (`CANNOT_LOAD_CONFIG`), and uses the machine's role without it:

```bash
curl -s https://builds.altinity.cloud/antalya-apt-repo/pubkey.gpg | gpg --dearmor \
  > /usr/share/keyrings/altinity-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/altinity-archive-keyring.gpg] https://builds.altinity.cloud/antalya-apt-repo stable main" \
  > /etc/apt/sources.list.d/altinity.list
apt-get update -y
V=26.6.8.20002.altinityantalya
apt-get install -y clickhouse-common-static=$V clickhouse-server=$V clickhouse-client=$V
systemctl start clickhouse-server
```

That machine only needs ClickHouse; it reads the tables the first machine exports.

## 5. Create the data

```bash
M="mysql -h127.0.0.1 -ubt -pCHANGE_ME -N"
$M <<'SQL'
CREATE DATABASE shop;
CREATE TABLE shop.orders (id INT PRIMARY KEY, customer VARCHAR(50) NULL,
  status ENUM('new','paid','shipped','cancelled') NOT NULL, amount DECIMAL(10,2) NOT NULL,
  note TEXT NULL, created DATETIME NOT NULL);
CREATE TABLE shop.heartbeat (id INT PRIMARY KEY, ts DATETIME(6) NOT NULL);
INSERT INTO shop.heartbeat VALUES (1, UTC_TIMESTAMP(6));
SET SESSION cte_max_recursion_depth=200000;
INSERT INTO shop.orders WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<100000)
 SELECT i, IF(i%7=0,NULL,CONCAT('cust',i%500)), ELT(1+i%4,'new','paid','shipped','cancelled'),
  ROUND((i*37)%10000/100,2), IF(i%3=0,NULL,CONCAT('note ',i)), '2026-01-01' + INTERVAL i MINUTE FROM n;
SQL
```

## 6. Start DBTrail: capture, baseline, export

```bash
SRC='bt:CHANGE_ME@tcp(127.0.0.1:3306)/'
IDX='bt:CHANGE_ME@tcp(127.0.0.1:3306)/bt_index'
mkdir -p /root/log /data
bintrail init --index-dsn "$IDX"
bintrail snapshot --source-dsn "$SRC" --index-dsn "$IDX" --schemas shop
nohup bintrail stream --source-dsn "$SRC" --index-dsn "$IDX" --server-id 999 --schemas shop \
  > /root/log/stream.txt 2>&1 &
sleep 5
bintrail dump --source-dsn "$SRC" --schemas shop --output /data/dump
bintrail baseline --input /data/dump --output /data/baselines
```

Then the export. **S3 and Glue** (the second run):

```bash
WH=s3://$BUCKET/$PREFIX/wh       # set BUCKET and PREFIX on this machine too
AWS_REGION=us-west-2 nohup bintrail export iceberg --index-dsn "$IDX" --baseline-dir /data/baselines \
  --catalog glue --warehouse $WH --every 1m --format json \
  > /root/log/export.jsonl 2> /root/log/export.err &
```

**Local disk** (the first run) writes inside ClickHouse's `user_files` folder instead:

```bash
W=/var/lib/clickhouse/user_files/iceberg; mkdir -p $W
nohup bintrail export iceberg --index-dsn "$IDX" --baseline-dir /data/baselines \
  --warehouse $W --every 1m --format json > /root/log/export.jsonl 2> /root/log/export.err &
# after the first export: chown -R clickhouse:clickhouse $W
```

Each export prints one JSON line per table to `export.jsonl`; that is how we counted exports.

## 7. Point ClickHouse at the tables

S3 and Glue, once the first export is done:

```sql
SET allow_experimental_database_glue_catalog = 1;
CREATE DATABASE glue ENGINE = DataLakeCatalog
  SETTINGS catalog_type = 'glue', region = 'us-west-2';
-- the tables are glue.`shop.orders` and glue.`shop.heartbeat`
```

Local disk:

```sql
CREATE DATABASE live;
CREATE TABLE live.orders    ENGINE = IcebergLocal('/var/lib/clickhouse/user_files/iceberg/shop/orders');
CREATE TABLE live.heartbeat ENGINE = IcebergLocal('/var/lib/clickhouse/user_files/iceberg/shop/heartbeat');
```

## 8. Start the load

About 15 changes per second on `orders`, the heartbeat row rewritten every second, and a primary key change every 10 seconds. Touch `/root/STOP_LOAD` to stop it.

```bash
cat > /root/load.sh <<'EOF'
#!/bin/bash
M="mysql -h127.0.0.1 -ubt -pCHANGE_ME -N"; n=0
while [ ! -f /root/STOP_LOAD ]; do
  n=$((n+1)); pk=""
  [ $((n % 10)) -eq 0 ] && pk="SET @i=FLOOR(1+RAND()*100000); SET @n=(SELECT MAX(id)+1 FROM shop.orders); UPDATE shop.orders SET id=@n WHERE id=@i;"
  $M -e "UPDATE shop.heartbeat SET ts=UTC_TIMESTAMP(6) WHERE id=1;
  SET @i=FLOOR(1+RAND()*100000); UPDATE shop.orders SET amount=amount+1, status='paid' WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); UPDATE shop.orders SET note=IF(note IS NULL, CONCAT('n',@i), NULL) WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); UPDATE shop.orders SET customer=NULL WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); UPDATE shop.orders SET amount=amount-1 WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); UPDATE shop.orders SET status='shipped' WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); UPDATE shop.orders SET note='x' WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); UPDATE shop.orders SET amount=amount+2 WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); UPDATE shop.orders SET status='cancelled' WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); DELETE FROM shop.orders WHERE id=@i;
  SET @i=FLOOR(1+RAND()*100000); DELETE FROM shop.orders WHERE id=@i;
  SET @n=(SELECT MAX(id)+1 FROM shop.orders); INSERT INTO shop.orders VALUES (@n, 'new1', 'new', 9.99, NULL, UTC_TIMESTAMP());
  SET @n=(SELECT MAX(id)+1 FROM shop.orders); INSERT INTO shop.orders VALUES (@n, NULL, 'new', 1.50, 'with note', UTC_TIMESTAMP());
  SET @n=(SELECT MAX(id)+1 FROM shop.orders); INSERT INTO shop.orders VALUES (@n, 'other', 'paid', 3.00, NULL, UTC_TIMESTAMP());
  $pk" 2>> /root/log/load.err
  sleep 1
done
EOF
chmod +x /root/load.sh
nohup /root/load.sh > /dev/null 2>&1 &
```

## 9. Measure every 20 seconds

Two numbers: how far ClickHouse is behind MySQL (read from the heartbeat row) and how long the report query takes. Set `T` to the tables you created in step 7.

```bash
T=glue.\`shop.orders\`; H=glue.\`shop.heartbeat\`     # or T=live.orders; H=live.heartbeat
echo "time,lag_s,report_s,exports" > /root/log/probe.csv
while [ ! -f /root/STOP_LOAD ]; do
  lag=$(clickhouse-client -q "SELECT dateDiff('millisecond', max(ts), now64(6,'UTC'))/1000 FROM $H")
  rep=$(clickhouse-client --time -q "SELECT status, count(), sum(amount) FROM $T GROUP BY status FORMAT Null" 2>&1)
  echo "$(date -u +%T),$lag,$rep,$(grep -c '"orders"' /root/log/export.jsonl)" >> /root/log/probe.csv
  sleep 20
done &
```

The tables in the post are the median `report_s` for each value of `exports`.

For the cache run, add a second timing in the same loop with every cache on, and check that the disk cache is really used (`system.filesystem_cache` must not stay empty, or the number means nothing):

```bash
C="use_iceberg_metadata_files_cache = 1, use_parquet_metadata_cache = 1, enable_filesystem_cache = 1, filesystem_cache_name = 's3_cache'"
clickhouse-client --time -q "SELECT status, count(), sum(amount) FROM $T GROUP BY status FORMAT Null SETTINGS $C"
clickhouse-client -q "SELECT count(), sum(size) FROM system.filesystem_cache"
```

To see a past state of the table, as in the post's screenshots, add `iceberg_snapshot_id = <id>` to the settings; `system.iceberg_history` lists the versions.

## 10. Try the ClickHouse settings (local disk, after 60 exports)

Run each query a few times and look at the time it prints:

```sql
-- prefetch + version hint: a second table over the same files
CREATE TABLE live.orders_pf ENGINE = IcebergLocal('/var/lib/clickhouse/user_files/iceberg/shop/orders')
  SETTINGS iceberg_metadata_async_prefetch_period_ms = 10000, iceberg_use_version_hint = 1;
SELECT status, count(), sum(amount) FROM live.orders_pf GROUP BY status;

-- accept metadata up to 60 s old
SELECT status, count(), sum(amount) FROM live.orders_pf GROUP BY status
  SETTINGS iceberg_metadata_staleness_ms = 60000;

-- merge the index files (the default threshold is 100 manifests, so lower it)
OPTIMIZE TABLE live.orders MANIFEST
  SETTINGS allow_experimental_iceberg_compaction = 1, iceberg_manifest_min_count_to_compact = 1;

-- rewrite the data: refused with "Unsupported snapshot's operation type OVERWRITE"
OPTIMIZE TABLE live.orders SETTINGS allow_experimental_iceberg_compaction = 1;

-- background compaction: refused, not supported by Iceberg storage
ALTER TABLE live.orders MODIFY SETTING allow_experimental_iceberg_compaction = 1;
```

## 11. Compact with Athena (S3 run, after 30 exports)

Leave the load and the export running. From the machine, so it uses the same role (boto3 came with step 4):

```bash
python3 - <<EOF
import boto3, time
a = boto3.client('athena', region_name='us-west-2')
for t in ('orders', 'heartbeat'):
    t0 = time.time()
    qid = a.start_query_execution(
        QueryString=f'OPTIMIZE shop.{t} REWRITE DATA USING BIN_PACK', WorkGroup='primary',
        ResultConfiguration={'OutputLocation': 's3://$BUCKET/$PREFIX/athena/'})['QueryExecutionId']
    while True:
        s = a.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']
        if s['State'] in ('SUCCEEDED', 'FAILED', 'CANCELLED'): break
        time.sleep(2)
    print(t, s['State'], s.get('StateChangeReason', ''), f'{time.time()-t0:.1f}s')
EOF
```

We used Athena engine version 3, the default. Keep the probe from step 9 running for 15 more measurements.

## 12. Compare row by row

Stop the load, wait for one more export (the heartbeat in ClickHouse matches MySQL), and compare:

```bash
touch /root/STOP_LOAD; sleep 90
mysql -h127.0.0.1 -ubt -pCHANGE_ME -N -B -e "SELECT id, COALESCE(customer,'<NULL>'), status, amount,
  COALESCE(note,'<NULL>') FROM shop.orders ORDER BY id" > /root/log/mysql.tsv
clickhouse-client -q "SELECT id, ifNull(customer,'<NULL>'), status, toDecimalString(amount,2),
  ifNull(note,'<NULL>') FROM $T ORDER BY id FORMAT TSVRaw" > /root/log/ch.tsv
cmp /root/log/mysql.tsv /root/log/ch.tsv && echo "identical: $(wc -l < /root/log/mysql.tsv) rows"
grep -o '"verdict":"[a-z-]*"' /root/log/export.jsonl | sort | uniq -c   # no "refused" lines
```

`toDecimalString` makes ClickHouse print `3.70` the way MySQL does instead of `3.7`.

## 13. Clean up

```bash
aws ec2 terminate-instances --instance-ids i-XXXX
aws glue delete-table --database-name shop --name orders
aws glue delete-table --database-name shop --name heartbeat
aws glue delete-database --name shop
aws s3 rm s3://$BUCKET/$PREFIX/ --recursive
aws iam remove-role-from-instance-profile --instance-profile-name $ROLE --role-name $ROLE
aws iam delete-instance-profile --instance-profile-name $ROLE
aws iam detach-role-policy --role-name $ROLE --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam delete-role-policy --role-name $ROLE --policy-name $ROLE
aws iam delete-role --role-name $ROLE
```

## What we did not test

- Tables larger than 100,000 rows, and a schedule that compacts over days rather than once.
- Spark or other engines for the compaction.
- Antalya swarms, which spread one query over several machines.
