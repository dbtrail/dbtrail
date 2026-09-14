# DBTrail — Upload Command

The `bintrail upload` command uploads local Parquet files to S3, independently of the pipeline that generated them. This is useful when files were created by `baseline` or `rotate --archive-dir` without the S3 flags — for example, because the network was down, AWS credentials weren't configured, or S3 wasn't needed at the time.

---

## Quick Start

```bash
bintrail upload \
  --source /var/lib/bintrail/archives/ \
  --destination s3://my-bucket/archives/
```

This recursively walks the source directory and uploads every `*.parquet` file to S3, preserving the directory structure as key prefixes.

---

## Flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--source` | Yes | — | Local directory containing Parquet files |
| `--destination` | Yes | — | S3 destination URL (e.g. `s3://my-bucket/archives/`) |
| `--region` | No | SDK default | AWS region override |
| `--retry` | No | `false` | Skip files that already exist in S3 (checked via `HeadObject`) |
| `--index-dsn` | No | — | MySQL DSN for the index database; updates `archive_state` with S3 metadata |
| `--format` | No | `text` | Output format: `text` or `json` |

---

## AWS Credentials

> Uploading to an S3 **Object Lock** bucket (ransomware-proof archives) works
> with no extra flags — see [object-lock.md](object-lock.md) for the bucket
> recipe and the `bintrail doctor --archive-s3` posture check.


`bintrail upload` uses the standard AWS SDK credential chain. The SDK checks these sources **in order** — the first one that provides valid credentials wins:

### 1. Environment variables (recommended for CI/CD and automation)

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_REGION=us-east-1

bintrail upload --source ./archives/ --destination s3://my-bucket/archives/
```

For temporary credentials (e.g. from `aws sts assume-role`), also set:

```bash
export AWS_SESSION_TOKEN=FwoGZXIvYXdzE...
```

### 2. Shared credentials file (`~/.aws/credentials`)

```ini
# ~/.aws/credentials
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

To use a named profile instead of `[default]`:

```bash
export AWS_PROFILE=bintrail-prod
bintrail upload --source ./archives/ --destination s3://my-bucket/archives/
```

### 3. Shared config file (`~/.aws/config`)

```ini
# ~/.aws/config
[default]
region = us-east-1

[profile bintrail-prod]
region = us-west-2
role_arn = arn:aws:iam::123456789012:role/BintrailUploader
source_profile = default
```

### 4. EC2 instance metadata / ECS task role / EKS IRSA

On AWS infrastructure, credentials are provided automatically:

- **EC2**: Attach an IAM instance profile to the instance
- **ECS**: Set a task IAM role in the task definition
- **EKS**: Use IAM Roles for Service Accounts (IRSA)

No environment variables or config files needed — the SDK discovers credentials from the instance metadata service.

### Region resolution

The AWS region is resolved in this order:

1. `--region` flag (if provided)
2. `AWS_REGION` environment variable
3. `AWS_DEFAULT_REGION` environment variable
4. `~/.aws/config` region setting for the active profile

### S3-compatible stores (MinIO, Wasabi, LocalStack)

Every S3 path in bintrail, the SDK uploads and the DuckDB reads alike, can
point at an S3-compatible store instead of AWS. One variable does it:

```sh
export BINTRAIL_S3_ENDPOINT=http://minio:9000     # scheme://host[:port], no path
export AWS_ACCESS_KEY_ID=...                       # the store's access key
export AWS_SECRET_ACCESS_KEY=...
```

Bucket-in-path addressing (`http://host/bucket/key`, what MinIO and
LocalStack need) is on by default with `BINTRAIL_S3_ENDPOINT`. A store that only
serves virtual-hosted URLs turns it off:

```sh
export BINTRAIL_S3_ENDPOINT=https://s3.wasabisys.com
export BINTRAIL_S3_PATH_STYLE=false
```

Notes:

- The endpoint applies to every S3 path, including `upload`, rotation's S3
  archiving, baseline upload and prune, `restore-index`, `archive reconcile`,
  `doctor --archive-s3`, `init`, the agent's payload uploads, and every DuckDB
  read of `s3://` paths (`query`/`recover` over archives, `reconstruct
  --baseline-s3`, `verify`, `drill`, `baseline refresh`, the shim, the
  console). The `views.sql` the console and `bintrail views` write names the
  endpoint too, so it reads the same store from another machine.
- `AWS_ENDPOINT_URL_S3` and `AWS_ENDPOINT_URL` are honored as fallbacks, so an
  environment already set up for the AWS CLI keeps working: bintrail mirrors
  their value to its DuckDB reads but otherwise leaves them to the SDK (no
  syntax check, no forced addressing, no change to region resolution). Set
  `BINTRAIL_S3_ENDPOINT` in new setups. An endpoint configured **only** as
  `endpoint_url` in `~/.aws/config` routes the SDK half but cannot reach the
  DuckDB half, which reads no AWS configuration at all: uploads land in your
  store while Parquet reads go to AWS. bintrail warns when it sees that shape;
  set `BINTRAIL_S3_ENDPOINT` to the same value.
- With `BINTRAIL_S3_ENDPOINT` set and no region configured anywhere, the
  region defaults to `us-east-1`; S3-compatible stores accept any. An
  endpoint the SDK resolved keeps the SDK's own region resolution.
- An invalid `BINTRAIL_S3_ENDPOINT` (no scheme, a path, credentials in the
  URL) fails any command that reads or writes S3, on the SDK paths and the
  DuckDB ones alike. A command whose data is entirely local is not refused
  over a setting it never reads, so `bintrail views` over local archives
  still renders. It never falls back to AWS: with uploads going to your store and reads going to an
  AWS bucket of the same name, a baseline that exists reads as missing, which
  is worse than an error. A value bintrail cannot parse in one of the AWS SDK
  variables is not fatal (the SDK may well accept it); it is logged, and only
  the DuckDB mirror is lost.
- Object Lock and `s3:GetBucketLocation` behave as the store implements
  them; `doctor --archive-s3` reports what it can query.

#### A store per server, from the console

`BINTRAIL_S3_ENDPOINT` is one setting for the whole process. When different
servers keep their buckets in different places (one in MinIO, one in AWS, one
in Wasabi's `eu-central-1`), the console sets the store **per server**
instead: the server form's `S3 endpoint`, `S3 addressing` and `S3 region`
fields (registry keys `s3_endpoint`, `s3_path_style`, `s3_region`). The
values are locations, never keys: the daemon's credential chain signs for
every store. What the setting does:

- It applies **per bucket**. Every bucket the server's `Archive to S3` and
  Backups S3 locations name is routed to that store, for the SDK uploads
  (rotation's archiving, baseline upload and prune) and for every DuckDB
  read alike, whichever server asked for it. A bucket has one store, and
  "no store" counts as one: two servers naming the same bucket with
  different settings is refused (HTTP 422), including when one of them has
  no store set, since that one reads the bucket from AWS or the process-wide
  endpoint.
- A store needs the server's **own** `Archive to S3` or Backups S3 location.
  A store with neither is refused, and so is one beside a location that is
  not an `s3://bucket/prefix/` URL. A server with no Backups location of its
  own reads the daemon's `--baseline-s3`; that bucket keeps the process-wide
  behaviour, whatever the server's store says, and a store on a server that
  names that bucket itself is refused (HTTP 422): it would take over every
  server that inherits it. A store saved before the daemon was started with
  that bucket as its `--baseline-s3` stops applying, with a warning at
  startup naming the bucket.
- The server form still shows a store that is saved but not applied (a
  hand-edited conflict, or a store on a bucket that later became the
  daemon's `--baseline-s3`). The startup log names the bucket and the reason.
- The store follows the server's **current** locations. Archives written to
  a bucket before the server's `Archive to S3` moved elsewhere, or before its
  store was cleared, are read through whatever routes that bucket now: keep a
  server naming that bucket with the same store for as long as those archives
  are read.
- Without DuckDB's aws extension, a bucket whose endpoint-only secret cannot
  be created fails the whole read session, not only reads under that bucket,
  since sending that bucket's reads to AWS instead would be worse.
- The DuckDB half gets one secret **scoped to the bucket**
  (`SCOPE 's3://<bucket>/'`), which DuckDB picks over the general one for
  paths under it. `views.sql` carries the same scoped secrets, still
  `credential_chain`, still no keys.
- `S3 addressing` defaults to path style when an endpoint is set (MinIO,
  LocalStack); `vhost` is for a store that only serves `bucket.host` URLs.
  A style without an endpoint is refused. `S3 region` alone (no endpoint)
  is allowed: it pins the signing region for a bucket outside the default
  one on AWS. MinIO ignores the region; Wasabi wants the one in its
  endpoint's name. With an endpoint and no region, uploads and DuckDB reads
  both sign as `us-east-1`.
- Buckets without a store keep the process-wide behaviour above.
- Keys per server and a `Test connection` that exercises the store are not
  part of this: the per-server setting covers where the store is, the
  ambient chain covers who signs.

### Minimum IAM permissions

> Want one policy that covers `upload` **and** archiving/baselines/queries
> against S3, with no per-feature tuning? Use **[S3 IAM
> Policy](s3-iam-policy.md)** instead — it's the same shape as below, just
> bucket-wide instead of prefix-scoped. Keep reading here only if you want
> to scope permissions to a single prefix.

The IAM principal (user or role) needs these S3 permissions on the destination bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": "arn:aws:s3:::my-bucket/archives/*"
    }
  ]
}
```

- `s3:PutObject` — required for uploading files
- `s3:GetObject` — required for the `HeadObject` existence check `--retry` issues (AWS authorizes `HeadObject` under the `s3:GetObject` permission — there is no separate `s3:HeadObject` IAM action), and if you later query archives with `bintrail query --archive-s3`
- `s3:AbortMultipartUpload` — uploads stream through the AWS SDK multipart Uploader (files above ~5 MiB are split into parts); when an upload fails or is interrupted, the SDK automatically aborts the multipart upload, which requires this permission. Without it, orphaned parts remain in the bucket and are billed. As a backstop for aborts that never run (crash, `SIGKILL`, network loss), attach the `AbortIncompleteMultipartUpload` lifecycle rule from [Deployment → S3 archive bucket: abort orphaned multipart uploads](deployment.md#s3-archive-bucket-abort-orphaned-multipart-uploads)
- `s3:GetBucketLocation` — **optional**, bucket-level (not scoped to `/archives/*`). `bintrail query --archive-s3` uses it only to cross-check the bucket's region against the one already resolved from the credential chain; without it, that check is skipped (logged at debug level, not a warning) and the resolved region is used as-is. Grant it only if the archive bucket lives in a different region than your EC2/ECS/EKS principal otherwise resolves — see [S3 Prerequisites](query-and-recovery.md#s3-prerequisites).

---

## Examples

### Upload archive files from a previous rotate

```bash
# Archives created by: bintrail rotate --archive-dir /var/lib/bintrail/archives/ ...
bintrail upload \
  --source /var/lib/bintrail/archives/ \
  --destination s3://my-bucket/archives/
```

### Upload with retry (skip already-uploaded files)

Useful when a previous upload was interrupted:

```bash
bintrail upload \
  --source /var/lib/bintrail/archives/ \
  --destination s3://my-bucket/archives/ \
  --retry
```

### Upload and update archive_state

When uploading rotate archives, pass `--index-dsn` to record the S3 location in the `archive_state` table. This allows `bintrail status` to show which partitions have been uploaded to S3:

```bash
bintrail upload \
  --source /var/lib/bintrail/archives/ \
  --destination s3://my-bucket/archives/ \
  --index-dsn 'user:pass@tcp(localhost:3306)/binlog_index'
```

The command extracts `partition_name` and `bintrail_id` from the Hive-partitioned directory structure (`bintrail_id=<uuid>/event_date=YYYY-MM-DD/event_hour=HH/events.parquet`) and updates the matching `archive_state` rows with `s3_bucket`, `s3_key`, and `s3_uploaded_at`. Files that don't match this pattern (e.g. baseline Parquet files) are still uploaded but skip the DB update.

### Upload baseline files

```bash
# Baselines created by: bintrail baseline --input ... --output /var/lib/bintrail/baselines/
bintrail upload \
  --source /var/lib/bintrail/baselines/ \
  --destination s3://my-bucket/baselines/ \
  --retry
```

### JSON output

```bash
bintrail upload \
  --source ./archives/ \
  --destination s3://my-bucket/archives/ \
  --format json
```

```json
{
  "uploaded": 42,
  "skipped": 0,
  "destination": "s3://my-bucket/archives/",
  "duration_ms": 12345
}
```

### Explicit region

```bash
bintrail upload \
  --source ./archives/ \
  --destination s3://my-bucket/archives/ \
  --region eu-west-1
```

---

## How It Works

1. **Walk**: Recursively scans `--source` for `*.parquet` files
2. **Key construction**: For each file, computes the S3 key by taking the path relative to `--source` and prepending the prefix from `--destination`
3. **Retry check** (if `--retry`): Issues a `HeadObject` request — skips the file if it already exists in S3
4. **Upload**: Streams the file through the AWS SDK multipart Uploader — files above ~5 MiB upload as multipart (per-part retry), smaller files as a single `PutObject`
5. **DB update** (if `--index-dsn`): For files matching the Hive archive path pattern, updates `archive_state` with the S3 bucket, key, and upload timestamp

---

## Troubleshooting

### "load AWS config" error

The AWS SDK could not find valid credentials. Check that one of the credential sources above is configured. Common causes:

- Missing `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
- Expired temporary credentials (`AWS_SESSION_TOKEN`)
- Wrong `AWS_PROFILE` pointing to a non-existent profile in `~/.aws/credentials`

### "Access Denied" on PutObject

The IAM principal has insufficient permissions. Verify the policy attached to your IAM user/role includes `s3:PutObject` on the correct bucket and prefix.

### "no such host" or timeout

The S3 endpoint is unreachable. Check:

- Network connectivity to AWS
- The `--region` flag or `AWS_REGION` matches the bucket's region
- VPC endpoints are configured (if running inside a VPC with no internet access)

### archive_state not updated

The `--index-dsn` update only works for files whose paths match the Hive archive layout produced by `rotate --archive-dir`. Baseline files use a different directory structure and won't trigger DB updates.
