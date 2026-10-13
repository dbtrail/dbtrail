# Telemetry

bintrail reports **metadata-only** usage statistics: which commands you run,
whether they succeeded, a coarse error class, and your version and platform.
It is **on by default** in official release builds.

Turn it off with any of these — the first one that applies wins:

```sh
bintrail telemetry off      # this account, permanently
DO_NOT_TRACK=1              # every tool on this machine that honours the convention
BINTRAIL_TELEMETRY=off      # this environment, e.g. in a systemd unit
bintrail --telemetry=off …  # this one invocation
```

Or, if you run the web interface (`bintrail-console watch`): open **Status → Usage telemetry** and click the toggle. It stops the running daemon's beacons
immediately (no restart) and records the same machine-wide choice as
`bintrail telemetry off`.

To see exactly what would be sent, byte for byte, without sending anything:

```sh
bintrail telemetry show
bintrail telemetry status   # is it on, and what decided that
```

The web interface shows the same event: on **Status → Usage telemetry**, open
**Show a sample event**. The daemon renders it through the same function the
command uses, so the fields and their form are the same; the values are the
daemon's own (each render draws a fresh `run_id`), and opening it sends
nothing.

## Why this document is longer than you expect

bintrail reads your binary logs. It sees your schemas, your table names, your
row data, and your credentials. A tool in that position asking to phone home
has to be specific about what it does and does not send, and it has to make
those claims checkable rather than merely stated. That is what the rest of this
document is for.

The claim is precisely this, in two parts:

- **The payload is metadata-only, and that is verifiable from the source.** The
  wire format is a closed list of thirteen coarse fields, enforced by tests.
- **Received data is anonymised at ingestion, and that is an operational
  commitment.** It is a promise about how our servers behave, which you cannot
  verify by reading the client. It is stated separately for that reason.

We do not use the unqualified word "anonymous", because the second half is not
something you can check.

## What is sent

This is the complete wire payload. There are no other fields.

| Field | Example | Why |
|---|---|---|
| `schema_version` | `2` | Lets the format evolve additively |
| `event_type` | `command_run` | One of `command_run`, `command_error`, `daemon_beacon` |
| `command` | `archive-reconcile` | Which command ran. Built from cobra command *names*, which are compile-time constants — never arguments or flag values |
| `outcome` | `ok` | `ok` or `error` |
| `error_class` | `db_connection` | A bounded class (list below). **Never** the error message |
| `duration_bucket` | `1s-10s` | `<100ms`, `100ms-1s`, `1s-10s`, `10s-1m`, `1m-10m`, `>10m` |
| `version` | `0.40` | Truncated to major.minor |
| `is_release` | `true` | Official build vs source build |
| `os` | `linux` | `runtime.GOOS` |
| `arch` | `arm64` | `amd64`, `arm64`, or `other` |
| `is_ci` | `false` | Whether a CI environment was detected |
| `is_interactive` | `true` | Whether stderr is a terminal |
| `run_id` | a UUID | Ephemeral, per process. Lets ingestion discard a re-sent batch. **Absent from daemon beacons** |
| `install_age` | `30d-90d` | **Daemon beacons only.** How long ago this install sent its first beacon, as one of four ranges: `<7d`, `7d-30d`, `30d-90d`, `>90d`. Never a date |
| `previous_version` | `0.103` | **Daemon beacons only, and on one beacon:** the first one after an upgrade. The major.minor this install ran before. Absent otherwise |

A real event looks like this:

```json
{
  "schema_version": 2,
  "event_type": "command_error",
  "command": "status",
  "outcome": "error",
  "error_class": "db_connection",
  "duration_bucket": "<100ms",
  "version": "0.40",
  "is_release": true,
  "os": "linux",
  "arch": "amd64",
  "is_ci": false,
  "is_interactive": true,
  "run_id": "470a53a1-20c2-4819-aacc-b132e516fec8"
}
```

A daemon beacon is the only event that carries the two install fields, and it
carries no `run_id`:

```json
{
  "schema_version": 2,
  "event_type": "daemon_beacon",
  "command": "watch",
  "outcome": "ok",
  "version": "0.104",
  "is_release": true,
  "os": "linux",
  "arch": "arm64",
  "is_ci": false,
  "is_interactive": false,
  "install_age": "30d-90d",
  "previous_version": "0.103"
}
```

`error_class` is one of exactly these, and nothing else:

```
db_connection   db_permission   binlog_not_found   schema_mismatch
config_invalid  storage_io      not_found          internal
unknown
```

Every class has at least one code path that produces it. A refused
preflight (`doctor`, `bintrail-pg doctor`, or `up` / `bintrail-console watch`
refusing to boot) reports the class of what
failed: `db_connection` when the source or the index could not be reached,
`db_permission` for missing grants, index write access or schema access,
`config_invalid` for a server setting (ROW format, row image, `log_bin`) —
and the `binlog_format` / `binlog_row_image` refusals that
`stream`, `index --source-dsn` and `agent` run without the doctor in front
report `config_invalid` too. A server
identity conflict is `config_invalid`, `binlog_not_found` covers both the
server's own "binlog purged" error (1236) and the `--no-gap-fill` refusal,
and `schema_mismatch` is the stale-snapshot guard. `unknown` is what a
failure with no bucket reports — an honest "no bucket" rather than a guess.

A capture that was already running reports the class of what stopped it.
The class names the kind of failure, not which server had it:

| What stopped capture | `error_class` |
|---|---|
| A database server out of disk, or a full table | `storage_io` |
| Too many connections, a lock wait timeout, a deadlock, a lost connection | `db_connection` |
| An index write that ran past `--write-timeout` | `db_connection` |
| No partition for the row | `not_found` |
| A statement larger than `max_allowed_packet` | `config_invalid` |
| A binlog file past 4GiB while capturing by position | `config_invalid` |
| A primary key too wide for the index | `schema_mismatch` |
| A row that could not be encoded, or a row event with no position | `internal` |

Only the class name is sent. The server's error number and message stay
on your machine.

### Why the values are coarse

`version` is truncated and `arch` is bucketed on purpose. Joined together,
`{version × os × arch × command mix}` starts to single out individual installs
once the population is small — which is exactly the situation a young project
is in. Coarsening the inputs is what stops the aggregate from becoming an
identifier.

`duration_bucket` collapses everything past ten minutes for the same kind of
reason: a precise runtime on a long `index` or `reconstruct` is a proxy for how
much data you have.

## What is never sent

Not "is not currently sent" — cannot be, by the structure of the code:

- Connection strings, DSNs, or any credential
- Hostnames, IP addresses, or server identifiers (`bintrail_id`, server UUIDs)
- Schema, table, or column names
- Primary key values or any row data
- Query text, or the SQL that produced a change
- File paths, binlog file names, or binlog positions
- GTIDs
- Flag values or positional arguments
- Verbatim error strings or panic messages
- Row counts, table sizes, or any measure of your data volume

## No persistent identifier

Nothing that identifies your installation is written to disk or sent. There is
no install UUID, no machine fingerprint, no account linkage.

A daemon does keep two facts between runs, in `telemetry-install.json` in the
telemetry folder (`~/.config/bintrail/`, or `BINTRAIL_TELEMETRY_DIR` when that
is set, as it is in the bundled Docker Compose file), and neither is an
identifier:

```json
{"first_seen":"2026-07-21","last_versions":{"watch":"0.104"}}
```

- `first_seen` is the calendar day (UTC) this install spooled its first
  beacon. Every install that started that day has the same value. It never
  leaves your machine: a beacon reports only which of four ranges the age
  falls in (`install_age`).
- `last_versions` is the newest major.minor that sent a beacon from here, per
  long-running command. Every install on that release has the same value. It
  is sent once, as `previous_version`, on the first beacon after that command
  is upgraded. A downgrade is not reported and does not lower the record:
  two processes of one command can share this folder at different versions,
  and counting every difference would have them report each other daily.

The file is written only while telemetry is on, only by a daemon's beacon. A
source build neither reports nor records a version.

`bintrail telemetry off` and the web interface toggle delete the file. A
daemon that is already running follows an opt-out recorded by another process
(you typed `telemetry off` next to it) at its next hourly check: it stops,
and deletes the file and anything it had spooled since. The environment
controls (`DO_NOT_TRACK=1`, `BINTRAIL_TELEMETRY=off`, `--telemetry=off`) stop
everything from being written or sent but delete nothing: a file left by an
earlier run with telemetry on stays on disk, unused, until you remove it or
run `bintrail telemetry off`.

A beacon carries the two fields only when the daemon can keep the file in step
with what it sends. If the file needed updating and could not be saved, or is
there and cannot be read, the beacon goes out without them.

What this buys, and what it does not: we can see whether installs stay
(the spread of `install_age`) and whether they upgrade, and from which release
(`previous_version`). We still cannot follow one install from day to day,
because nothing sent says which install a beacon came from. On a small
population two wide ranges and a version do narrow things down, which is why
the ranges are wide, why `previous_version` rides on a single beacon instead
of every one, and why neither is ever attached to an event that carries a
`run_id`.

Things to know when reading these numbers. An install that was already
running before this release has no recorded start, so its age counts from the
first beacon it sends on this release. A patch release (0.104.0 to 0.104.1)
is not a version change here, because the version is truncated to
major.minor. A downgrade is not reported. And a container whose telemetry folder is not on a volume starts
over on every upgrade (see "Where the choice is kept, and containers").

`run_id` is generated fresh in memory each time a command runs, lives in the
spooled event only until that batch is delivered or expires, and never appears
in a daemon beacon — a process that runs for months would otherwise carry one
stable identifier for months, which is exactly the thing this design refuses to
create.

The cost is real and worth stating: **we cannot count users, or follow any
one install over time.** Daily totals, version adoption, command mix, error
rates, platform mix, and the two coarse install facts above are what remain.
That trade was made deliberately.

## How it is sent

**No command ever makes a network call to report itself.** Finishing a command
appends one line to a local file. Delivery happens on a *later* run.

- Events are appended to `~/.config/bintrail/telemetry-spool/<date>.ndjson`
  (mode `0600`).
- Starting a command kicks off a background attempt to deliver whatever earlier
  runs left there, with a 2-second deadline, concurrent with your command.
- On exit, a command waits at most 250 ms for a delivery already in flight.
  When there is nothing pending this costs nothing.
- If delivery fails, the batch is dropped. There is no retry queue, so an
  offline machine never builds a backlog that floods out later.
- The spool is capped at 5 MB per day and anything undelivered is discarded
  after 7 days.
- When several bintrail processes share a home directory, each batch is claimed
  by an atomic rename, so a batch is never sent twice or lost between them.

Delivery goes to `https://telemetry.dbtrail.com` — a host separate from the
authenticated DBTrail API, so telemetry traffic cannot be correlated with an
account at the network layer. The request carries **no `Authorization` header,
no cookie, and no account identifier**.

### Daemons

Long-running commands (`stream`, `agent`, `up`, `watch`,
`bintrail-console serve`, `bintrail shim`, `bintrail-pg stream`,
`bintrail-pg flashback`) send at most **one beacon per UTC day**, and the
first only after they have been running for an hour. A finer cadence would
reconstruct your uptime and maintenance windows; a beacon at startup would
mean a crash-looping daemon emitted one per restart.

Beacons carry no `run_id`. They are the only events that carry `install_age`
and `previous_version` (see "No persistent identifier").

Because daemons have no terminal, they log one line at startup when telemetry
is on. It is logged at `WARN` so that a host running at a raised log level — a
fleet machine, typically — still sees it.

## Controls

The first that applies decides. `bintrail telemetry status` will tell you which
one did.

| | Effect |
|---|---|
| `DO_NOT_TRACK=1` | Off. Checked before any file is read or written |
| `--telemetry=on\|off` | Off (or on) for this invocation |
| `BINTRAIL_TELEMETRY=on\|off` | Off (or on) for this environment |
| `~/.config/bintrail/telemetry.json` | Written by `bintrail telemetry on\|off`, or by the web interface's **Status → Usage telemetry** toggle |
| *(nothing set)* | **On** |

The toggle in the web interface writes that same file *and* flips the running
`watch` daemon's live decision, so its beacons stop the moment you opt out.
Every other bintrail process that reads that folder honours the file from its
next run, and a long-running one that is already up follows an opt-out within
the hour (turning it back **on** takes effect at its next start). When a
higher-precedence control above is in charge, the web interface shows that and defers
to it rather than pretending to override it.

### Where the choice is kept, and containers

The consent file, the spool and a daemon's install record (see "No persistent
identifier") live in `~/.config/bintrail/`. Set
`BINTRAIL_TELEMETRY_DIR` to an absolute path to keep them somewhere else. It
only names a folder: it is not a consent control and cannot turn telemetry on.
A value that is not an absolute path is refused, and telemetry stays off until
it is fixed.

That setting exists because of containers. A container's home directory is part
of the container, so recreating the container (which is what an upgrade does)
deletes the file, and a choice made with `telemetry off` or the web interface
toggle is gone: telemetry is back to the default, on. The bundled
`docker-compose.yml` sets `BINTRAIL_TELEMETRY_DIR` to a folder on its state
volume from file version 5, so the choice survives. **With an older compose
file, or your own container setup, a choice recorded in the file does not
survive an upgrade.** Use `BINTRAIL_TELEMETRY=off` or `DO_NOT_TRACK=1` in the
container's environment, which do, or set `BINTRAIL_TELEMETRY_DIR` to a path on
a volume.

**If you had turned telemetry off before taking file version 5, turn it off
once more after that upgrade.** The earlier choice was inside the container
that the upgrade replaces, so there is nothing to carry over; from then on it
is kept.

Each container has its own home directory, so in the bundled file the three
long-lived services (`bintrail`, `router`, and `shim` from the `flashback`
profile) are all given the same folder. `router` and `shim` mount the volume
read-only: they follow the choice and report nothing themselves. With a file
older than version 5 they read their own empty home directory, so the toggle
in the web interface does not reach them. The one-shot services (`baseline`,
`iceberg-export`) do not read that folder either; `BINTRAIL_TELEMETRY=off` or
`DO_NOT_TRACK=1` in their environment is the way to turn it off there.

`bintrail telemetry status` names the folder in use, and says so when
`BINTRAIL_TELEMETRY_DIR` was refused. A daemon logs one `WARN` line for a
refused value.

The `.deb` and `.rpm` packages are not affected: their service user's home is
`/var/lib/bintrail`, which is kept.

`bintrail telemetry off` also deletes anything already spooled locally, so
opting out does not leave earlier events sitting on your disk waiting for a
delivery that will never come.

Additionally, and only ever to suppress:

- **CI is detected** (`CI`, `GITHUB_ACTIONS`, `TF_BUILD`, `TRAVIS`, `CIRCLECI`,
  `JENKINS_URL`, `BUILDKITE`, `GITLAB_CI`) and reporting is disabled. A build
  robot is not a person making a choice.
- **No home directory** (distroless images, systemd `DynamicUser`, a scrubbed
  environment) disables everything silently.

None of these can turn telemetry *on*.

`BINTRAIL_TELEMETRY` is intentionally absent from `bintrail config init`'s
generated template. That file's variables are applied to command-line flags,
and routing consent through it would make an environment setting
indistinguishable from an explicit `--telemetry` flag in `telemetry status`.

### Diagnosing

Telemetry swallows every error by design, so that it can never change a
command's behaviour, output, or exit code. If you want to see what it is doing:

```sh
BINTRAIL_TELEMETRY_DEBUG=1 bintrail status …
```

## Builds that cannot report at all

The ingestion address is injected at build time. It is **empty by default**,
which makes any binary you build yourself physically incapable of reporting —
no address, no network path:

```console
$ go build ./cmd/bintrail && ./bintrail telemetry status
Telemetry:    OFF
Consent:      on (decided by: default)
Endpoint:     not compiled in; this build cannot send anything
```

This is the assertion a distribution packager needs (Debian and Fedora both
forbid phone-home *capability*, not merely phone-home behaviour), and it is why
the test suite — including the end-to-end test that builds and runs the real
binary — cannot emit telemetry.

## Which surfaces report

| Surface | Reports? | |
|---|---|---|
| `bintrail`, `bintrail-pg` commands | Yes, by default | |
| Daemons (`stream`, `agent`, `up`, `watch`, `bintrail-console serve`, `bintrail shim`, `bintrail-pg stream`, `bintrail-pg flashback`) | Yes, one beacon per day | `watch` also records the web interface actions below |
| **Demo image** (`ghcr.io/dbtrail/bintrail-demo`) | **Never** | Hard-disabled in the image and in its entrypoint, and asserted by its smoke test. An evaluation image must not phone home from a laptop |
| **MCP server** (`bintrail-mcp`) | **Never** | Invoked by an AI agent inside an editor or chat session — no human is present to consent and no terminal exists for a notice. It cannot link the telemetry package at all |
| **Web interface** (under `watch`) | A deliberate-action event, server-side | See below. No JavaScript beacon, ever: the frontend has no third-party dependencies and makes no telemetry request; the event is recorded by the server it already talks to |

### Web interface actions

When the web interface runs inside a reporting `watch` daemon, a **deliberate** UI
action records one usage event so we can see which features of the web interface are used,
the same signal a CLI `recover` gives. Only these intent surfaces are counted:
`console-recover`, `console-recover-cascade`, `console-reconstruct` (time
travel), `console-verify`, `console-baseline`. Passive browsing (listing events,
paging, status polling, capability checks) records nothing.

The action name is a **compile-time constant** on the route — never derived from
the request path, query, or body — so no schema, table, primary key, or row
value can reach the wire; the closed allowlist still applies. Crucially, because
the web interface lives in a months-long daemon that holds a single `run_id`, these
events are recorded **without any `run_id`** (like beacons), so they cannot be
stitched into a per-install activity timeline. They are day-granularity usage
counts, nothing more. The read-only `bintrail-console serve` wires no
client for web interface actions and records none of these events; like every other
long-running daemon it still emits the daily liveness beacon, which carries
nothing a command event doesn't.

## Cloned machines — the honest caveat

Telemetry state lives in a home directory. If you enable it, then bake that
machine into an AMI, a container layer, or a golden image, **the setting travels
with the image**. Every host cloned from it will report, and nobody on those
hosts made that choice.

There is no way for us to detect this, so: set `BINTRAIL_TELEMETRY=off` (or
`DO_NOT_TRACK=1`) in your base image if that is your situation. Daemons log
their startup disclosure partly so a cloned host is not silent about it.

## What happens to received data

These are operational commitments about our infrastructure, not properties you
can verify by reading this repository. They are listed separately for that
reason.

- Source IP addresses are truncated or dropped at the edge **before anything is
  persisted**, and are never joined to a payload. This includes load-balancer
  and CDN logs, which retain client IPs independently of application settings.
- `run_id` is used to discard duplicate batches at ingestion and is then dropped
  from the retained store, so per-install command sequences cannot be
  reconstructed.
- Raw events are retained for at most 90 days, after which only aggregates
  remain.

## Not a sales channel

Telemetry is **never** used for sales, lead generation, or targeting
free-to-paid conversion — and is architecturally incapable of it. The requests
are unauthenticated and carry no account identifier, so a received event cannot
be attributed to a customer, a company, or a person even in principle.

## Legal basis and your rights

- **Controller**: DBTrail. Contact via the repository issue tracker or the
  address published at <https://dbtrail.com>.
- **Purposes**: deciding what to build next (which commands are actually used)
  and finding reliability problems (which error classes are rising).
- **Lawful basis**: legitimate interest in maintaining and improving the
  software, with the objection right exercisable at any time by the one-line
  opt-out above.
- **Recipients**: nobody. Data is not sold, shared, or passed to advertising or
  analytics third parties. There is no third-party analytics SDK in this
  codebase.
- **Retention**: raw events ≤ 90 days; aggregates indefinitely.
- **Your rights**: access, erasure, restriction and objection. Because no
  identifier is stored, we cannot locate "your" events to export or delete them
  on request — which is a direct consequence of collecting no identifier, not an
  evasion. Turning telemetry off stops all future collection immediately.

Under CCPA/CPRA the payload is deidentified: it contains no identifier and no
information reasonably linkable to a household or individual, and we make no
attempt to reidentify it.

## Counting downloads

Release download counts and container image pulls are observed **server-side**,
from ordinary web request logs with truncated IPs — the same way any download
page works. That is a separate mechanism from everything above: it involves no
code in the binary, nothing stored on your machine, and it happens whether or
not you have telemetry enabled. It is what tells us how many installs exist;
telemetry is only what tells us which commands they run.

The binary sends no first-run or install ping.

## Verifying the claims

These run in CI on every change. They are the reason the guarantees above are
statements of fact rather than intent.

| Test | What it proves |
|---|---|
| `TestTelemetryImportsNothingFromThisModule` | The telemetry package links **no** other package in this repository. It cannot reach the code that knows about DSNs, rows, schemas, or server identity — whatever anyone later writes inside it |
| `TestTelemetryPackagesAreSelfContained` | The same, for direct imports, so the boundary cannot be routed around via a helper package |
| `TestAllowlistMatchesStruct` | The field list above is the whole field list |
| `TestMarshalledEventHasOnlyAllowedKeys` | The serialized bytes contain nothing else either |
| `TestRequestCarriesNoCredential` | No `Authorization`, cookie, API key, or server UUID on the request |
| `TestClassifyErrorNeverLeaksMessage` | An error carrying a DSN and a table name still yields only a bounded class |
| `TestRootHookIsNotShadowed` (×3 binaries) | Instrumentation cannot be silently lost from part of the command tree |
| `TestMCPServerIsTelemetryFree` | The MCP server cannot link the telemetry package |
| `TestRunDaemonDoesNotBeaconBeforeFirstTick` | A crash-looping daemon emits nothing |
| Per-daemon wiring guards (`*DaemonWiringEmitsBeacon`, one per call site across `stream`, `agent`, `up`, both `watch` paths, `serve`, `shim`, `bintrail-pg stream`, `bintrail-pg flashback`) | Every daemon this document says beacons actually starts the beacon loop — each test drives the real run function and fails if its launch line is deleted |
| `TestBeaconCarriesNoRunID` | Daemon beacons carry no identifier |
| `TestInstallFieldsOnlyOnBeacons` | `install_age` and `previous_version` never ride on an event that carries a `run_id` |
| `TestBeaconReportsPreviousVersionOnce` | `previous_version` is on one beacon per upgrade; a patch release, a downgrade or a source build is not one |
| `TestTamperedInstallRecord` | A hand-edited install file can put nothing on the wire except a range and a major.minor of at most three and four digits |
| `TestUnsavableRecordSendsNoInstallFields` | A daemon that cannot save the install file sends neither field, so `previous_version` can never repeat daily |
| `TestTwoDaemonsAtDifferentVersionsDoNotReportEachOther`, `TestTwoProcessesWithOneNameDoNotReportEachOther` | Daemons sharing a folder at different versions do not report each other, same command name or not |
| `TestOptOutDeletesTheInstallRecord` | Opting out deletes the install file |
| `TestRunDaemonFollowsARecordedOptOut`, `TestRunningDaemonFollowsARecordedOptOut` | A running daemon's loop follows an opt-out recorded next to it, and deletes what it wrote in between |
| `TestOptOutWaitsForABeaconInFlight`, `TestBeaconChecksConsentAgainUnderTheLock` | An opt-out waits for a beacon that is writing, and a beacon that was waiting writes nothing after it |
| `TestBeaconNeverRewritesTheConsentFile` | The daily beacon never writes the consent file, so it cannot overwrite an opt-out |
| `TestRecordDaemonCommandOmitsRunID` | A web interface action inside a daemon records no `run_id`, so there is no per-install timeline |
| `TestRecordActionUsesFixedName` | The web interface action name is a fixed constant, never derived from the request |
| Demo image smoke test | The demo image's telemetry guard is present and applies to every process in the container |

The most load-bearing of these is the first. A field allowlist alone cannot
establish the metadata-only claim, because an allowlist checks field *names* and
is blind to where a value came from — nothing in it would stop someone
assigning a server UUID to `run_id`. A package that cannot import the code
holding that UUID has no such option.
