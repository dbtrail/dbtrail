# Running DBTrail on Amazon ECS

One ECS task runs everything the Docker Compose stack runs in its `bintrail`
service: capture, the web interface, and the control plane that adds servers
from it. Servers are added in the web interface; no source is given at
startup. What changes from Compose is where the index lives (your own MySQL),
where the saved state lives (EFS), and how the web interface is reached (a load
balancer).

Everything on this page was run on Fargate (ARM64): a server added from the web
interface, captured under a steady write load, a full read, and three
deployments of new task definition revisions. After them the servers, the login
and the snapshots were still there, and the index held every change once.

## What you need

- **An index MySQL** that the task reaches, MySQL 8.0 or newer: for example
  RDS for MySQL. It is the same index the Compose stack runs in its
  `index-mysql` service, which does not apply here; neither does `index-init`.
  Its account needs to create databases: DBTrail creates `bintrail_index` and
  one `bintrail_idx_<id>` per server, and needs all rights on them. Its sizing
  is in [capacity.md](capacity.md).
- **An EFS file system** with an access point for user and group `999`, the
  user the image runs as. If the file system has a policy, it must let the
  task role mount and write (`elasticfilesystem:ClientMount`, `ClientWrite`).
- **A load balancer** for the web interface (an ALB, HTTPS in production).
- **The `ghcr.io/dbtrail/bintrail-console` image**, pinned to a version, never
  `latest`. To pull it from a private subnet, give the task a NAT gateway or
  copy the image to ECR.

## The task definition

Replace the values in `<...>`. ARM64 or X86_64 both work: the image is
published for both.

```json
{
  "family": "dbtrail",
  "requiresCompatibilities": ["FARGATE"],
  "networkMode": "awsvpc",
  "cpu": "1024",
  "memory": "4096",
  "runtimePlatform": { "cpuArchitecture": "ARM64", "operatingSystemFamily": "LINUX" },
  "ephemeralStorage": { "sizeInGiB": 30 },
  "executionRoleArn": "arn:aws:iam::<account>:role/<execution-role>",
  "taskRoleArn": "arn:aws:iam::<account>:role/<task-role>",
  "volumes": [{
    "name": "state",
    "efsVolumeConfiguration": {
      "fileSystemId": "<fs-id>",
      "transitEncryption": "ENABLED",
      "authorizationConfig": { "accessPointId": "<fsap-id>", "iam": "ENABLED" }
    }
  }],
  "containerDefinitions": [{
    "name": "dbtrail",
    "image": "ghcr.io/dbtrail/bintrail-console:<version>",
    "essential": true,
    "command": ["watch", "--log-format", "json"],
    "portMappings": [{ "containerPort": 8090, "protocol": "tcp" }],
    "environment": [
      { "name": "BINTRAIL_CONSOLE_LISTEN", "value": "0.0.0.0:8090" },
      { "name": "BINTRAIL_CONSOLE_ALLOWED_HOSTS", "value": "<the name people open, e.g. dbtrail.example.com>" },
      { "name": "BINTRAIL_CONSOLE_ALLOW_SETUP", "value": "1" },
      { "name": "BINTRAIL_CONSOLE_SERVERS", "value": "/var/lib/bintrail/console-servers.yaml" },
      { "name": "BINTRAIL_CONSOLE_AUTH", "value": "/var/lib/bintrail/console-auth.yaml" },
      { "name": "BINTRAIL_CONSOLE_MCP_TOKEN_FILE", "value": "/var/lib/bintrail/console-mcp-token.yaml" },
      { "name": "BINTRAIL_CONSOLE_BASELINE_TRIGGER", "value": "1" },
      { "name": "BINTRAIL_CONSOLE_VERIFY_TRIGGER", "value": "1" },
      { "name": "AWS_REGION", "value": "<region>" }
    ],
    "secrets": [{ "name": "BINTRAIL_INDEX_DSN", "valueFrom": "<secret ARN>" }],
    "mountPoints": [{ "sourceVolume": "state", "containerPath": "/var/lib/bintrail" }],
    "healthCheck": {
      "command": ["CMD-SHELL", "wget -q -T 5 -O /dev/null http://127.0.0.1:8090/api/healthz"],
      "interval": 30, "timeout": 10, "retries": 3, "startPeriod": 60
    },
    "stopTimeout": 60,
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": { "awslogs-group": "/ecs/dbtrail", "awslogs-region": "<region>", "awslogs-stream-prefix": "dbtrail" }
    }
  }]
}
```

What each part is for:

- **`BINTRAIL_INDEX_DSN`** comes from Secrets Manager (or SSM Parameter
  Store): `user:password@tcp(host:3306)/bintrail_index`. The execution role
  needs `secretsmanager:GetSecretValue` on that secret.
- **`BINTRAIL_CONSOLE_BASELINE_TRIGGER` and `BINTRAIL_CONSOLE_VERIFY_TRIGGER`**
  turn on **Read database now** and the checks, as the Compose stack does.
  Without the first, the Snapshots page says the full read is turned off.
- **`BINTRAIL_CONSOLE_ALLOW_SETUP`** lets the first visitor create the
  username and password. Keep the load balancer closed to everyone else until
  that is done; after it, the setup page answers no more.
- **`stopTimeout: 60`** gives capture time to save its position on a deploy.

## State that must survive the task

Everything under `/var/lib/bintrail` goes on EFS:

| File or folder | Without it, after a new task |
| --- | --- |
| `console-servers.yaml` | No servers: nothing is captured, and nothing says so |
| `console-auth.yaml` | The setup page asks for a new username and password |
| `console-mcp-token.yaml` | Claude's token stops working |
| `console-mysql-port.yaml` | The MySQL port is off again, and needs a new password |
| `snapshots/` | The snapshots on disk are gone. With an S3 location the bucket still has them |

The folder also keeps the history of full reads and checks, and a server
being added that was not finished.

Create the service only once the EFS mount targets are `available`. A task
started before that fails with `failed to resolve "fs-....efs..."`; the service
starts another one.

## Disk for full reads

A full read (**Read database now**, or a schedule) writes mydumper's output
and, with S3, the Parquet copy to the working folder before it converts and
uploads them. Leave `BINTRAIL_CONSOLE_BASELINE_STAGING` unset: it then uses a
folder under `/tmp`, which on Fargate is the task's ephemeral storage, not EFS.
Fargate gives 20 GiB by default and up to 200 GiB with `ephemeralStorage`.
Size it to the largest dump: about the source's data size, uncompressed. A
folder saved in the web interface for building `.sql` backups takes precedence
over the variable; keep that one off EFS too.

Hours of the change log on their way to S3 are staged the same way
(`BINTRAIL_CONSOLE_ARCHIVE_STAGING`, also under `/tmp` when unset). Losing
that folder with the task loses nothing: an hour not yet uploaded is staged
again from the index.

## The web interface

- **Load balancer.** An ALB listener to a target group of type `ip`, port
  `8090`, health check path `/api/healthz` (it needs no login). Give the
  service a health check grace period of at least 120 seconds.
- **Host names.** Besides IP addresses and `localhost`, the web interface
  answers only to the names it is given: put every name people type (the
  ALB's DNS name, your own record) in `BINTRAIL_CONSOLE_ALLOWED_HOSTS`, comma
  separated with no spaces. The ALB's health check uses the task's IP, which
  is always allowed.
- **HTTPS.** Terminate TLS on the ALB, with a certificate for the name people
  open. The session cookie is always marked `Secure`.
- **Security groups.** The task accepts `8090` from the load balancer only,
  reaches the index on `3306`, every source it captures, and EFS on `2049`.

## The MySQL port

To let a MySQL client in ([time-travel-sql.md](time-travel-sql.md)), add a
port mapping for `3309` and a **Network** Load Balancer in front of it: it is
plain TCP, which an ALB does not carry. Then turn the port on in the web
interface (**Settings → MCP Server → Connect a SQL client**, address
`0.0.0.0:3309`). The setting and its password are kept in
`console-mysql-port.yaml` on EFS. The port has no TLS: keep the NLB internal.

Starting it with `--flashback-listen` instead also needs
`BINTRAIL_CONSOLE_TOKEN` (as a secret): without it the task stops at startup.

## AWS access

Give the task role the S3 policy in [s3-iam-policy.md](s3-iam-policy.md) for
the bucket set as a server's S3 location. No access keys: the role is found on
its own. Set `AWS_REGION` to the bucket's region (or give each S3 location its
region in the web interface): that policy does not let DBTrail ask a bucket
where it lives, so a bucket in another region is read with the wrong one.

## One task, and deployments

Run **exactly one** task: `desiredCount: 1`, and no autoscaling.

A deployment starts the new task before it stops the old one (the ECS default,
`minimumHealthyPercent: 100`, `maximumPercent: 200`). From the release after
0.99.0, capture is safe through it with no change: the new task finds each
server's capture lock held, shows **WAITING FOR OTHER DBTRAIL**, and starts
capturing from the old task's position once the old task stops. Measured on
Fargate: the new task waited about 1 minute 40 seconds, capture paused about
20 seconds around the hand-over, and every change was in the index once.

While both tasks run, the load balancer sends requests to either one:

- **Sign in again after a deployment.** Sessions are kept by each task, so a
  request that reaches the other task asks for the login again. Load balancer
  stickiness keeps one browser on one task during the overlap.
- **Do not add, remove or change servers or settings during a deployment.**
  Each task reads the saved settings when it starts and writes the whole file
  when they change, so a change made on one task during the overlap can be
  overwritten by the other, or not be seen by the new one.

**On 0.99.0 and older**, set `minimumHealthyPercent: 0` and `maximumPercent:
100`, so ECS stops the old task before it starts the new one. With the default,
the new task marks each server `failed` and nobody captures after the old task
stops; or, once the old task has run 8 hours, both tasks capture until ECS
stops the old one, and every change in that window is stored twice. The
changelog entry for the fix has a query that finds such rows. The first
upgrade from 0.99.0 also needs the stop-first setting, once: the old task does
not keep its lock.

Capture pauses during a deployment either way. The source must keep its binary
logs longer than that: on RDS, `binlog retention hours` of at least a few hours.

A task that dies without stopping (its host or its network gone) keeps its
lock on the index until the index server notices, which can take about two
hours; the new task waits that long. How to free it sooner:
[when the host dies](deployment.md#when-the-host-dies).

## Logs and metrics

`--log-format json` sends one JSON object per line to CloudWatch Logs. For
Prometheus metrics, add `--metrics-addr :9090` to the command and a port
mapping, and scrape it from inside the VPC. Alert on
`bintrail_stream_last_flush_timestamp_seconds` going stale, not on the task
stopping: the daemon restarts a failed stream itself instead of exiting
([observability.md](observability.md)).

## Upgrades

Register a new task definition revision with the new image tag and update the
service. Read [upgrade.md](upgrade.md) for the version you go to.
