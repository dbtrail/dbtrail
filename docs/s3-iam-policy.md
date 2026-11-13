# S3 IAM Policy (copy-paste)

One IAM policy that covers every DBTrail feature that touches S3 — archiving
rotated partitions, baseline snapshots, `bintrail upload`, and querying/
time-traveling against archived data. Attach it to the IAM user or role that
runs DBTrail, swap in your bucket name, and every `--archive-s3` /
`--baseline-s3` / `--s3-bucket` flag in the docs will work without further
tuning.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BintrailS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket",
        "arn:aws:s3:::my-bucket/*"
      ]
    }
  ]
}
```

Replace `my-bucket` with your bucket name (both ARNs — the bucket itself and
everything inside it). If your account isn't in the standard `aws` partition
(GovCloud, China), replace `arn:aws:s3` with `arn:aws-us-gov:s3` or
`arn:aws-cn:s3`.

`bintrail init --s3-bucket <bucket>` prints this exact policy for you (with
the partition already filled in) if it can't create/configure the bucket
itself — so if you've already run that, you don't need to write this by
hand. Policies attached before `s3:AbortMultipartUpload` was added are still
safe; add that action per the table below.

## What each permission is for

| Action | Used by |
|---|---|
| `s3:PutObject` | `bintrail rotate --archive-s3`, `bintrail baseline --upload`, `bintrail upload` — writing Parquet archives/baselines to the bucket. Large files stream as S3 multipart uploads, and the `CreateMultipartUpload`/`UploadPart`/`CompleteMultipartUpload` calls are all authorized by `s3:PutObject` — successful uploads of any size need nothing extra |
| `s3:GetObject` | `bintrail query`/`recover --archive-s3`, `--baseline-s3`/`reconstruct` reads, the `HeadObject` existence check `bintrail upload --retry` issues (S3 authorizes `HeadObject` under `s3:GetObject` — there's no separate `s3:HeadObject` action), and the read of `<baselines root>/_NEWEST` every baseline upload does before moving that pointer. Also how the console's snapshot cleanup confirms a snapshot is in S3 before removing the local copy: with `s3:PutObject` and no `s3:GetObject`, S3 takes uploads and refuses to read them back, so the cleanup keeps every snapshot on the local disk, stops sending them again, and says so on the Snapshots page. S3 authorizes every `HEAD` under `s3:GetObject`, so without it every question is refused, a key that does not exist included, and the refusal looks the same as credentials that stopped working; the page names both causes ("either the credentials lack s3:GetObject on this prefix, or they are no longer valid; check both") |
| `s3:ListBucket` | Enumerating archived partitions/baseline snapshots (`ListObjectsV2`), and the bucket-reachability check (`HeadBucket`) `bintrail init --s3-bucket` and `bintrail rotate` run. Also what lets a baseline upload create `<baselines root>/_NEWEST` the first time: without it S3 answers 403 instead of 404 for a key that does not exist yet, so the upload cannot tell "no pointer yet" from "not allowed" and leaves the pointer alone (the snapshot is still published; the upload warns). A server with **Delete from S3 after** set needs it too: before sending a snapshot again, the console asks whether the S3 cleanup is removing that snapshot's folder, and without `s3:ListBucket` that question is refused, so the send does not happen and is tried again every hour. It is not skipped: a folder an earlier S3 cleanup marked and did not finish is finished by a later one whatever its age, so a copy sent into it could be removed after the local copy went on the strength of it. The question is refused the same way without `s3:GetObject` (a `HEAD` needs it), and S3's answer does not say which one is missing, so the Snapshots page names both and the ways out: add the one that is missing, or clear **Delete from S3 after** |
| `s3:DeleteObject` | Baseline upload's own cleanup of its in-progress `_INCOMPLETE` marker, `agent --validate` removing its connectivity-probe object, and, for a server with **Delete from S3 after** set, the console removing that server's snapshots older than the age (#2596). No bintrail command deletes archive data from S3 (`archive reconcile --prune` deletes registry rows only). Optional but recommended: omit it if you'd rather nothing in the bucket ever be deleted by bintrail, and leave **Delete from S3 after** empty, or each cleanup stops on the refusal and says so in the log |
| `s3:AbortMultipartUpload` | Cleaning up after a **failed or interrupted** large upload: the SDK automatically aborts the in-progress multipart upload, and without this permission that abort is `AccessDenied` — the orphaned parts stay in the bucket, invisible in listings but billed as storage. Never used on the success path. Pair it with an [`AbortIncompleteMultipartUpload` lifecycle rule](deployment.md#s3-archive-bucket-abort-orphaned-multipart-uploads) on the bucket as the backstop for uploads that die before the abort can run (crash, `SIGKILL`) |

## Three things this policy deliberately leaves out

**`s3:GetBucketObjectLockConfiguration`** is only needed by the advisory
`bintrail doctor --archive-s3` posture check ([object-lock.md](object-lock.md));
without it that check reports SKIP and everything else works. Add it as a
bucket-level action (alongside `s3:ListBucket`) if you use the check.

**`s3:GetBucketLifecycleConfiguration`** is only needed to read whether a
rule in the bucket expires old snapshots. DBTrail never sets a bucket rule:
the web interface writes the rule and you apply it. It deletes snapshots from
S3 only for a server with **Delete from S3 after** set (see
[dump-and-baseline.md](dump-and-baseline.md)). This permission lets two places say whether it is there: the
line above the rule in the Snapshots settings, and the advisory
check of `bintrail doctor`:

```bash
bintrail doctor --source-dsn "$SRC" \
  --baseline-s3 s3://my-bucket/backups/ --snapshot-every 6h
```

| The bucket | `doctor` reports | The web interface line says |
|---|---|---|
| has a rule that covers the snapshot prefix | PASS, with the rule and its age (WARN if the age is no longer than `--snapshot-every`) | the rule and its age, in red if it is too short for the schedule |
| has no rule that covers it | WARN | that the bucket grows without limit |
| could not be read | SKIP | that the rules could not be read, and that this permission is the likely reason |

Without the permission you get the third row and everything else works:
the check never fails `doctor`, and no snapshot, restore or query needs it.
Add it as a bucket-level action (alongside `s3:ListBucket`) if you want the
answer. Some S3-compatible stores do not implement the call; they get the
third row too.

**`s3:GetBucketLocation`** is not in the policy above, and a bucket in
another region than the one DBTrail resolves for its credentials does not
need it: DBTrail finds the bucket's region with a HEAD request on the bucket
that carries no credentials, which S3 answers with the region whatever the
policy says (see [A bucket in another
region](upload.md#a-bucket-in-another-region)). DBTrail asks
`s3:GetBucketLocation` only when that HEAD gets no answer (a custom endpoint,
or a VPC endpoint policy that refuses requests without credentials); adding
it to the `Action` list above, as a bucket-level permission alongside
`s3:ListBucket`, is then what lets the region be found.

## Read-only access for people who query the Parquet copy

The policy above is DBTrail's own: it writes and deletes. Someone who only
queries the Parquet copy (for example with the DuckDB `views.sql` file) needs
read access, and should not get the hourly change archives:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListTheBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-bucket"
    },
    {
      "Sid": "ReadSnapshots",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/*"
    },
    {
      "Sid": "NoChangeArchives",
      "Effect": "Deny",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/*bintrail_id=*"
    }
  ]
}
```

What each part does:

- **The Deny keeps the change archives out of reach.** Hourly archives are
  written under a `bintrail_id=<id>/` path segment, next to that source's
  `index-meta.json`. Each archived row carries the MySQL connection id and,
  when the source logs it, the original SQL statement (`query_text`). The
  web interface deliberately does not show either.
- **It only covers keys that carry the segment.** `rotate` and `bintrail-console watch`
  always write it. `bintrail upload --source` pointed at a folder INSIDE
  `bintrail_id=<id>/` uploads keys without it, and this rule does not cover
  those objects. Upload archives from the folder that holds `bintrail_id=<id>/`.
  The same goes for the agent's bring-your-own-storage files
  (`<server_id>/<schema>.<table>/<date>/events_*.parquet`, full before and
  after row images): keep them out of a bucket this reader can open.
- **It matches the segment, not a prefix.** Archive and backup prefixes are
  chosen by the operator and can be the same or nested, so a prefix-based
  Deny can miss the archives or also block the snapshots. In an S3 resource
  ARN, `*` matches across `/`, so `*bintrail_id=*` covers the segment wherever
  it sits in the key. An explicit Deny wins over the Allow above it.
- **Listing still shows names.** `s3:ListBucket` lets the reader see archive
  object keys (dates and hours), not their contents.
- **The change log views stop working.** A `views.sql` downloaded with
  **Include the change log** reads the archives, so its events view fails with
  `AccessDenied`. The table views (`shop.orders`) keep working.

Two things a reader with this policy can still see, stated plainly:

- **Every column of every table.** A snapshot holds full rows. The access
  rules (data profiles, redaction, roles) apply to the web interface, not to the
  files in the bucket.
- **What changed between two snapshots.** Comparing two consecutive snapshots
  of a table shows which rows changed, even without the archives.

If the reader's credentials resolve to a different region than the bucket,
the `views.sql` file the web interface writes names the bucket's region when DBTrail could find
it. Otherwise, set `AWS_REGION` to the bucket's region on the reader's machine.

## Read-only access for the router

The compose `router` service (`bintrail-console router`, the process that
serves the MySQL port) reads S3 when a `_snapshot` or `_flashback` query
reaches back past the local snapshots: it finds snapshots and change archives
by listing, and reads their Parquet files. It never writes or deletes. The
snapshot jobs, the uploads and the S3 cleanup run in the `bintrail` service,
not in the router, and the router's SQL workers get no AWS variables at all.
So it can hold keys with this policy instead of the one at the top of this
page:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RouterListTheBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-bucket"
    },
    {
      "Sid": "RouterReadObjects",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/*"
    }
  ]
}
```

Give the keys to the router alone with `ROUTER_AWS_ACCESS_KEY_ID` and
`ROUTER_AWS_SECRET_ACCESS_KEY` in `.env` (see [Read-only keys for the
router](docker.md#read-only-keys-for-the-router)).

| What the router calls | Permission |
|---|---|
| `ListObjectsV2` on the snapshot and archive prefixes, to find snapshots, their `_SUCCESS` markers and the hourly archive files | `s3:ListBucket` |
| `GetObject` of snapshot and archive Parquet files, and the range reads and `HEAD` requests DuckDB makes on them | `s3:GetObject` (S3 authorizes `HEAD` under it) |
| An unsigned `HEAD` on the bucket to find its region | none |
| `GetBucketLocation`, only when that `HEAD` gets no answer | `s3:GetBucketLocation`, optional, as in the policy at the top |

Unlike the reader policy above, this one has no `Deny` on `bintrail_id=*`:
`_flashback` and the changes laid over a `_snapshot` come from the change
archives, so the router needs them. Scoping `s3:GetObject` to prefixes works
if it names both the snapshot prefix and the archive prefix. Scoping
`s3:ListBucket` with an `s3:prefix` condition has to allow every key under
those prefixes, since the listing walks them without a delimiter.

Two limits, stated plainly:

- A server whose S3 store has keys of its own (an S3-compatible store set up
  in the web interface with an access key) is read with those keys by the
  router as well. This policy does not apply to them.
- The router's AWS variables are read when it starts: `docker compose up -d
  router` after changing them.

## Tighter scope (optional)

DBTrail's own policy at the top of this page grants access to the whole bucket. If you want to scope it
to a prefix instead (e.g. only `archives/*` inside a bucket shared with
other tools), see [upload.md — Minimum IAM
permissions](upload.md#minimum-iam-permissions) for a prefix-scoped example
— just be aware a prefix-only policy needs one grant per prefix you actually
use (`archives/`, `baselines/`, etc.) since `--archive-s3` and
`--baseline-s3`/`--upload` are typically pointed at different prefixes.
