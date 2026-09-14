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
| `s3:GetObject` | `bintrail query`/`recover --archive-s3`, `--baseline-s3`/`reconstruct` reads, and the `HeadObject` existence check `bintrail upload --retry` issues (S3 authorizes `HeadObject` under `s3:GetObject` — there's no separate `s3:HeadObject` action) |
| `s3:ListBucket` | Enumerating archived partitions/baseline snapshots (`ListObjectsV2`), and the bucket-reachability check (`HeadBucket`) `bintrail init --s3-bucket` and `bintrail rotate` run |
| `s3:DeleteObject` | Baseline upload's own cleanup of its in-progress `_INCOMPLETE` marker, and `agent --validate` removing its connectivity-probe object. No bintrail command deletes archive data from S3 (`archive reconcile --prune` deletes registry rows only). Optional but recommended — omit it if you'd rather nothing in the bucket ever be deleted by bintrail |
| `s3:AbortMultipartUpload` | Cleaning up after a **failed or interrupted** large upload: the SDK automatically aborts the in-progress multipart upload, and without this permission that abort is `AccessDenied` — the orphaned parts stay in the bucket, invisible in listings but billed as storage. Never used on the success path. Pair it with an [`AbortIncompleteMultipartUpload` lifecycle rule](deployment.md#s3-archive-bucket-abort-orphaned-multipart-uploads) on the bucket as the backstop for uploads that die before the abort can run (crash, `SIGKILL`) |

## Two things this policy deliberately leaves out

**`s3:GetBucketObjectLockConfiguration`** is only needed by the advisory
`bintrail doctor --archive-s3` posture check ([object-lock.md](object-lock.md));
without it that check reports SKIP and everything else works. Add it as a
bucket-level action (alongside `s3:ListBucket`) if you use the check.

**`s3:GetBucketLocation`** is not in the policy above. It's only needed if
your archive/baseline bucket lives in a **different AWS region** than the
one DBTrail otherwise resolves for its credentials (env vars, `~/.aws`
profile, or EC2/ECS/EKS instance metadata). Without it, `bintrail query
--archive-s3` just uses the region it already resolved — same-region setups
(the common case) work fine. See [S3
Prerequisites](query-and-recovery.md#s3-prerequisites) for the full
explanation, or add `"s3:GetBucketLocation"` to the `Action` list above (as
a bucket-level permission, alongside `s3:ListBucket`) if your bucket is
cross-region.

## Tighter scope (optional)

The policy above grants access to the whole bucket. If you want to scope it
to a prefix instead (e.g. only `archives/*` inside a bucket shared with
other tools), see [upload.md — Minimum IAM
permissions](upload.md#minimum-iam-permissions) for a prefix-scoped example
— just be aware a prefix-only policy needs one grant per prefix you actually
use (`archives/`, `baselines/`, etc.) since `--archive-s3` and
`--baseline-s3`/`--upload` are typically pointed at different prefixes.
