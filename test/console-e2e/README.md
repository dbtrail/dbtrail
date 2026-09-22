# console-e2e — headless-Chrome regression guard

`go test` never renders the embedded console SPA (`internal/console/assets/*`),
so a whole class of bugs is invisible to it: CSS cascade (an invisible button),
frontend DOM/state logic (a form that auto-expands), and how the UI *presents*
a backend state (a raw `Unknown database` error wall; the control plane
vanishing when the selected server's index is missing). Every scenario in
`console_e2e.mjs` pins a bug that shipped in 0.13.3 and reached a user.

## Run it

Requires Docker (the shared `bintrail-test-mysql` container, same as the Go
integration suite) and Node.

```sh
# start the test MySQL if it isn't already (see CONTRIBUTING.md)
docker run -d --name bintrail-test-mysql -e MYSQL_ROOT_PASSWORD=testroot \
  -p 13306:3306 mysql:8.4 --binlog-format=ROW --binlog-row-image=FULL \
  --log-bin=binlog --server-id=1

make console-e2e
# or, to use your system Chrome instead of the playwright-managed chromium:
PW_CHANNEL=chrome make console-e2e
```

`run.sh` builds `bintrail-console` and `bintrail`, creates a throwaway index
database, provisions it with the production schema (`bintrail init`) plus a
seeded read fixture — row events, a cascade fixture, and a baseline snapshot
produced by the real `bintrail baseline` converter over a hand-written
mydumper-format dump — launches a source-less `watch` daemon (with
`--baseline-dir` and the baseline-trigger opt-in), seeds a monitored source
whose per-source index is intentionally **not** provisioned (the lifecycle
state that exercises the guards), drives the scenarios, and tears everything
down.

## What each scenario guards

| Scenario | Bug it pins |
|---|---|
| boot: monitor capability reported | caps fetch for a broken/unprovisioned selected server must not degrade to `{}` |
| control plane: Start button / monitor copy | `/api/capabilities` 502 cascade hiding the whole control plane |
| button: primary keeps gradient on hover + text stays white | `.btn:hover` background winning → white text on light gray |
| form: advanced collapsed for a source entry | the optional "BYO index" section auto-expanding for a source entry |
| form: advanced expanded for a BYO-index entry | the other `byoIndex` arm — a no-source entry must show its index fields |
| form: source fields visible | the monitor form rendering for a source entry |
| error: real 1049 reaches the frontend + empty state (×4) | the actual backend 1049 (from the unprovisioned default server) must surface as an actionable empty state, not a raw wall — and `scrubDSNError` must preserve the index db name |
| events: render + redaction (×6) | the primary read workflow over REAL indexed rows (#970): rows render, the diff expands, and the seeded `query_text`/`query_hash` canary never reaches the DOM while `connection_id` passes through (#701 D1) |
| export: JSON/CSV blobs (×6) | the real download buttons, blobs captured at `URL.createObjectURL`: query_text-free, connection_id kept, CSV header in lockstep with `EVENT_CSV_COLUMNS` |
| recover: submit renders SQL (×3) | Recover actually submits and paints the reversal SQL panel — scenario 6 only checks the form DOM exists |
| cascade: banner + counts (×3) | the `cascade_detected` positive-half rendering (#619) — the banner block whose missing `)` once broke the whole SPA |
| schema changes (×9) | the DDL history view (#1443): seeded rows render, same-second DDLs list in binlog order, UTC column, the type filter narrows through the API, the empty state |
| timetravel (×6) | the reconstruct gate + baseline+deltas over a real fixture baseline (#970): event fold, baseline-only row, deleted row |
| overview: window honesty (×2) | `buildOverview` window line uses the fetched window's own bounds, never `status.coverage` (#679/#686) |
| storage: Create-baseline gates (×5) | the button's double-gate (#686): live enabled arm, destination-missing arm, capability-off arm; the fixture snapshot listed |
| telemetry: sample event (×3) | the Storage card's "Show a sample event" fold (#1447): closed by default, opens to a read-only `<pre>` carrying the daemon's `sample_event` string verbatim (never re-serialized in the frontend) |
| access profiles (×6) | the Settings > Access profiles page (#1445): reachable from the sidebar on a fresh index, the real forms author a flag, a profile and a deny rule onto the index, a refusal shows the shared message, and the Remove buttons take everything back off |
| no uncaught JS errors | any thrown error over the whole drive |

Adding a scenario: append an `ok(...)`/`bad(...)` block in `console_e2e.mjs`.
A non-zero exit fails CI and writes `console-e2e-failure.png` to the artifact dir.

---

# first-run walk — a scoreboard of a person's first hour (#1800)

`console_e2e.mjs` above asks "does this still work?". The walk asks a
different question: **how much work is it, the first time?** It drives a fresh
`watch` daemon (no servers, no login yet) and a **stock** `mysql:8.4` it starts
itself — no server flags at all, because "zero yellow or red on a stock MySQL"
is only a real claim if nothing tuned it — and measures the walk from creating
the login to the first snapshot.

```sh
# start the test MySQL if it isn't already (see above), then:
make console-first-run-walk
```

It needs Docker, Node and `mydumper` on PATH (the first snapshot runs it).
Anything missing and it **skips loudly**: exit 77 with a banner saying nothing
was measured. `FIRST_RUN_WALK_ALLOW_SKIP=1` turns that into exit 0 for a local
run; CI never sets it.

## What it measures

Five runs — a clean database; a reload in the middle of Connect; a second
server; a server with a table that has no primary key; and a refused snapshot
update — each reporting its own columns:

| Column | What it counts |
|---|---|
| Clicks to the first snapshot | every click, dropdown choice and reload, walked and the minimum a snapshot needs |
| Fields typed | fields the walk had to fill in (a value already there is not typed) |
| Forced choices | places where nothing can proceed until the person picks |
| Trips out of the browser | things that can only be done elsewhere (the permissions block, `mkdir`) |
| Visible words per step | words **rendered above the fold** on the step's own surface, at 1280×720 |
| Banned words and em dashes | the closed word list, counted once per distinct sentence on screen |
| Yellow or red elements | the interface's **own** warning and error classes, never a pixel colour |
| Changes visible without reloading | of three changes made on the database, how many appear on the Overview |
| Primary button below the fold | how far the step's main button sits below the visible area, in px |
| Final-screen window vs real retention | the sentence on the last screen against `/api/rotation` |
| Copied text different from what is shown | the Copy button's clipboard, and whether the block's password is the one in the form |

Rules worth knowing before changing a number:

- **The word count reads the rendered page**, never a source file: a range per
  word, so a paragraph cut by the fold counts only the lines above it, and
  text scrolled out of a box does not count. Copyable SQL in a `<pre>` is not
  prose and is left out of both counts.
- **A folded `Details` placed after the step's primary button is skipped
  whole**, summary included. That fold is allowed to name the mechanism; the
  rule is written into the test so it is visible rather than assumed.
- **A column whose feature does not exist yet reports "not measurable"**, with
  the reason, and never "pass". Two do today: the final screen states no
  retention, and nothing on screen reports a refused snapshot update.
- The walk may WAIT for the product, but it never acts for the person without
  recording the click.

## The ratchet

`first_run_baseline.json` holds what main measures today. The default mode
fails when any number gets **worse** than that; a number that gets better
passes, with a line asking the PR that improved it to lower the baseline in
the same change. That is how a test that is red against the goal can still be
green in CI.

Three of the columns — the two word counts and the distance below the fold —
depend on where text WRAPS, and that depends on the fonts of the machine that
measured it. They are compared only against a baseline recorded on the same
platform; anywhere else they are printed and marked "not compared", with the
command to record them for that platform. Everything else counts things —
clicks, fields, sentences, elements — and is compared everywhere.

Two things the numbers mean precisely, because the labels are shorter than
the rules:

- **Yellow or red** counts SIGHTINGS, not distinct elements: a warning shown
  on three measured screens counts three times, and a warning box inside a
  warning notice counts both. That is deliberate — the question is how much
  alarm a person walks through — and it is why the count rises when the walk
  adds a second server.
- **Clicks to the first snapshot** counts what this walk clicked, including
  one reload it only needs while later changes do not appear on their own.
  The minimum beside it is the same walk without the clicks a snapshot does
  not need.

```sh
FIRST_RUN_WALK_MODE=target make console-first-run-walk        # the goals: red today
FIRST_RUN_WALK_MODE=write-baseline make console-first-run-walk # record what it measured
FIRST_RUN_WALK_SHOTS=/tmp/walk make console-first-run-walk     # a screenshot of every step
```

Every run writes `first-run-scoreboard.json` to the artifact directory with
the evidence behind each number: the clicks in order, the fields typed, the
words per step, the sentences that carried a banned word, and which element
was yellow.

The arithmetic, the ratchet and the counting rules have their own unit tests,
which run first and need no Docker:

```sh
node --test test/console-e2e/first_run_walk.test.mjs
```
