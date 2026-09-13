package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// continuityPollInterval is how often the continuity watcher re-reads each
// index's stream_state. gap_lost is a permanent, stamped condition — a few
// minutes of notification latency is fine; hammering every index every few
// seconds is not.
const continuityPollInterval = 5 * time.Minute

// eventSender is the one seam watchNotifier needs from the delivery layer —
// an interface so tests can capture events without HTTP.
type eventSender interface {
	Notify(ev notify.Event)
}

// watchNotifier maps the watch daemon's health signals onto webhook events
// with edge-triggering: one notification on the transition into a bad state,
// a daily reminder while it persists, one on recovery. Wired only when
// --notify-webhook is set.
type watchNotifier struct {
	send eventSender
	edge *notify.Edge
}

func newWatchNotifier(ctx context.Context, url string) *watchNotifier {
	return &watchNotifier{send: notify.NewWebhook(ctx, url), edge: notify.NewEdge(0)}
}

// newWatchNotifierFromFlags builds the notifier when --notify-webhook is set;
// nil otherwise — every hook then stays disconnected. The URL is validated
// here so a typo refuses startup instead of surfacing as a buried delivery
// warning at the moment of the first real incident.
func newWatchNotifierFromFlags(ctx context.Context) (*watchNotifier, error) {
	if upNotifyWebhook == "" {
		return nil, nil
	}
	u, err := url.Parse(upNotifyWebhook)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("--notify-webhook: %q is not a valid http(s) URL", upNotifyWebhook)
	}
	slog.Info("webhook notifications enabled")
	return newWatchNotifier(ctx, upNotifyWebhook), nil
}

// rotationCycleHooks builds rotation.StartLoop's onCycle callbacks: the
// health gauge always (#1203 — it publishes only once a cycle actually runs),
// plus the webhook notifier when configured.
func rotationCycleHooks(n *watchNotifier) []func(failed bool, deferred int) {
	hooks := []func(bool, int){observe.SetRotationHealth}
	if n != nil {
		hooks = append(hooks, n.RotationCycle)
	}
	return hooks
}

// VerifyFinished is the verifySupervisor.onFinish hook: it fires on runs that
// found mismatches (critical — the data recover would produce is wrong) or
// could not verify cleanly (warning), and resolves once a clean run lands.
// Skip records never notify — the skip is bookkeeping, not a health signal.
// resolveVerify clears every severity tier of one server's verify condition,
// reporting whether any was active. The tier enumeration lives HERE, once — a
// tier forgotten on resolve would leave a stale active entry that suppresses
// future alerts and never sends its recovery event.
func (n *watchNotifier) resolveVerify(serverID string) bool {
	resolved := n.edge.Resolve("verify:" + notify.SeverityCritical + ":" + serverID)
	if n.edge.Resolve("verify:" + notify.SeverityWarning + ":" + serverID) {
		resolved = true
	}
	return resolved
}

func (n *watchNotifier) VerifyFinished(rec console.VerifyRunRecord) {
	// Defensive only: finish() never produces skip records (they are appended
	// straight to history by the scheduler), but a future caller must not
	// turn one into an alert.
	if rec.State == console.VerifyStateSkipped {
		return
	}
	s := rec.Summary
	// A run that verified nothing must neither alarm as a mismatch nor
	// reassure. Two shapes land here with zero verified tables: Total == 0
	// ("only one baseline yet", an empty tables filter) and all-inconclusive
	// (baseline/archive unreachable). The CLI's Report.ExitError treats
	// all-inconclusive as a failure, so it fires here too — but never as the
	// "clean" that would auto-close a real mismatch alert.
	allInconclusive := s.Total > 0 && s.Inconclusive == s.Total
	problem := rec.State == "failed" || s.Mismatch > 0 || s.Error > 0 || allInconclusive
	clean := rec.State == "succeeded" && !problem && s.Match > 0
	sev := notify.SeverityWarning
	if s.Mismatch > 0 {
		sev = notify.SeverityCritical
	}
	// The edge key carries the severity class so an escalation (a
	// warning-grade run followed by a critical mismatch) is a new transition,
	// never suppressed by the lower tier's repeat window.
	key := "verify:" + sev + ":" + rec.ServerID
	if clean {
		if n.resolveVerify(rec.ServerID) {
			n.send.Notify(notify.Event{
				Event: notify.EventVerifyProblem, Severity: notify.SeverityInfo, Server: rec.ServerName, Resolved: true,
				Summary: fmt.Sprintf("verification is clean again: %d/%d tables match", s.Match, s.Total),
			})
		}
		return
	}
	if !problem || !n.edge.Fire(key, "") {
		return
	}
	summary := fmt.Sprintf("verification found problems: %d mismatch, %d error, %d inconclusive (%d match)",
		s.Mismatch, s.Error, s.Inconclusive, s.Match)
	switch {
	case rec.State == "failed":
		summary = "verification run failed: " + rec.LastError
	case allInconclusive:
		summary = fmt.Sprintf("verification could not verify any table: all %d inconclusive", s.Total)
	}
	n.send.Notify(notify.Event{
		Event: notify.EventVerifyProblem, Severity: sev, Server: rec.ServerName, Summary: summary,
		Details: map[string]string{
			"mode": string(rec.Mode), "trigger": rec.Trigger, "state": rec.State,
			"mismatch": strconv.Itoa(s.Mismatch), "error": strconv.Itoa(s.Error), "match": strconv.Itoa(s.Match),
		},
	})
}

// RotationCycle is the rotation.StartLoop onCycle hook. Unhealthy mirrors the
// loop's own escalation condition — failed OR deferring unarchived partitions
// — because either way the index is not shrinking when it should.
func (n *watchNotifier) RotationCycle(failed bool, deferred int) {
	const key = "rotation"
	if !failed && deferred == 0 {
		if n.edge.Resolve(key) {
			n.send.Notify(notify.Event{
				Event: notify.EventRotationUnhealthy, Severity: notify.SeverityInfo, Resolved: true,
				Summary: "built-in rotation is healthy again",
			})
		}
		return
	}
	if !n.edge.Fire(key, "") {
		return
	}
	n.send.Notify(notify.Event{
		Event: notify.EventRotationUnhealthy, Severity: notify.SeverityWarning,
		Summary: "built-in rotation made no progress; the index keeps growing (see the daemon log)",
		Details: map[string]string{"failed": strconv.FormatBool(failed), "deferred_partitions": strconv.Itoa(deferred)},
	})
}

// Continuity reports one index's stream continuity. gap_lost is critical:
// events in the gap are permanently unrecoverable. edgeKey identifies the
// index (the target DSN — unique after dedup, unlike display names, which a
// registry entry could share with the reserved boot label).
func (n *watchNotifier) Continuity(server, edgeKey string, gapLost bool, detail string) {
	key := "continuity:" + edgeKey
	if !gapLost {
		if n.edge.Resolve(key) {
			n.send.Notify(notify.Event{
				Event: notify.EventContinuityGapLost, Severity: notify.SeverityInfo, Server: server, Resolved: true,
				Summary: "capture continuity restored (the stream was re-baselined)",
			})
		}
		return
	}
	// The detail rides the edge: a DIFFERENT gap while the first is still
	// active (re-baseline plus a second loss inside one repeat window) is a
	// new data-loss event — Edge.Fire bypasses the suppression window on a
	// changed detail.
	if !n.edge.Fire(key, detail) {
		return
	}
	ev := notify.Event{
		Event: notify.EventContinuityGapLost, Severity: notify.SeverityCritical, Server: server,
		Summary: "capture continuity lost: events in the gap are PERMANENTLY unrecoverable; re-baseline to resume trustworthy coverage",
	}
	if detail != "" {
		ev.Details = map[string]string{"detail": detail}
	}
	n.send.Notify(ev)
}

// bootContinuityName labels the command-line boot index in continuity/verify
// gauges and webhook events.
const bootContinuityName = "cli index"

// BaselineStale reports one server's staleness verdict (#1193). Only broken
// notifies — aging stays visible in status/console (pre-saturation aging is a
// bootstrap artifact; alerting on it would cry wolf — see
// status.baselineAgingFraction's comment); the channel carries the transition
// that means "full-table restore is impossible NOW". edgeKey identifies the
// (index DSN, baseline source) PAIR — never the display name (it can collide
// with the reserved boot label), and never the DSN alone: staleness is a
// property of the pair, so unlike the continuity poller (which dedups by DSN
// because gap_lost is per-index) two sources graded against one index are
// distinct conditions and must not Fire/Resolve against each other.
// brokenTables must be a STABLE identity (sorted
// table list) — never a clock- or floor-derived string: the coverage floor
// advances with every rotation cycle, and a volatile edge detail would bypass
// the repeat window and page every hour.
func (n *watchNotifier) BaselineStale(server, edgeKey string, broken bool, brokenTables, coverageFloor string) {
	key := "baseline:" + edgeKey
	if !broken {
		if n.edge.Resolve(key) {
			n.send.Notify(notify.Event{
				Event: notify.EventBaselineStale, Severity: notify.SeverityInfo, Server: server, Resolved: true,
				Summary: "every table's newest baseline is inside delta coverage again; full-table restore is possible",
			})
		}
		return
	}
	if !n.edge.Fire(key, brokenTables) {
		return
	}
	n.send.Notify(notify.Event{
		Event: notify.EventBaselineStale, Severity: notify.SeverityCritical, Server: server,
		Summary: "the newest baseline predates available delta coverage: full-table restore through the missing window is impossible; take a fresh baseline (bintrail dump + bintrail baseline)",
		Details: map[string]string{"tables": brokenTables, "coverage_floor": coverageFloor},
	})
}

// stalenessPollInterval: staleness moves with rotation cycles (hourly by
// default), and each check lists the baseline source — an S3 LIST every few
// minutes would buy nothing.
const stalenessPollInterval = time.Hour

// stalenessWatcher evaluates each server's baseline staleness (#1193) and
// feeds the webhook channel on the broken transition. Webhook-gated only:
// status and the console carry the full ok/aging verdicts.
type stalenessWatcher struct {
	n         *watchNotifier
	registry  *console.Registry
	bootDSN   string
	globalDir string
	globalS3  string

	// unknownEdge rate-limits the cannot-evaluate warnings to one per day per
	// index — a watcher that cannot watch is itself a coverage hole and must
	// never go silent (the continuity poller's rule).
	unknownEdge *notify.Edge

	// Injectable for tests — no ticker, no real DB, no real S3.
	listBaselines func(ctx context.Context, source string) (files []reconstruct.BaselineFile, skipped int, err error)
	oldestDelta   func(ctx context.Context, dsn string) (status.DeltaFloor, error)
}

func startStalenessWatch(ctx context.Context, n *watchNotifier, registry *console.Registry, bootDSN, globalDir, globalS3 string) {
	w := &stalenessWatcher{
		n: n, registry: registry, bootDSN: bootDSN, globalDir: globalDir, globalS3: globalS3,
		unknownEdge:   notify.NewEdge(0),
		listBaselines: reconstruct.ListBaselinesReport,
		oldestDelta:   oldestDeltaByDSN,
	}
	go func() {
		if ctx.Err() == nil {
			w.runCycle(ctx)
		}
		tick := time.NewTicker(stalenessPollInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				w.runCycle(ctx)
			}
		}
	}()
}

// stalenessTarget is one server with a baseline source to grade.
type stalenessTarget struct{ name, dsn, source string }

// targets applies the same all-or-nothing baseline fallback as
// withBaselineDefaults (#1010): an entry with its OWN dir or S3 chose its
// location; only a fully unset entry inherits the process-wide one. Servers
// with no baseline anywhere have nothing to grade and are skipped.
func (w *stalenessWatcher) targets() []stalenessTarget {
	globalSrc := w.globalDir
	if globalSrc == "" {
		globalSrc = w.globalS3
	}
	var out []stalenessTarget
	if w.bootDSN != "" && globalSrc != "" {
		out = append(out, stalenessTarget{name: bootContinuityName, dsn: w.bootDSN, source: globalSrc})
	}
	if w.registry != nil {
		for _, e := range w.registry.List() {
			src := e.BaselineDir
			if src == "" {
				src = e.BaselineS3
			}
			if src == "" {
				src = globalSrc
			}
			if src == "" {
				continue
			}
			out = append(out, stalenessTarget{name: e.Name, dsn: e.DSN, source: src})
		}
	}
	return out
}

func (w *stalenessWatcher) runCycle(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("baseline staleness cycle panicked; checking continues next tick", "panic", r)
		}
	}()
	for _, t := range w.targets() {
		if ctx.Err() != nil {
			return
		}
		// The edge identity is the (dsn, source) pair — see BaselineStale's
		// doc. \x1f as separator: a control char no DSN or path contains.
		edgeID := t.dsn + "\x1f" + t.source
		files, skipped, err := w.listBaselines(ctx, t.source)
		if err == nil && skipped > 0 {
			// A location read in PART is not evaluable either (#1601): the
			// newest snapshot may sit in the directory that would not open,
			// and grading the readable subset fires a baseline_stale alert
			// on a healthy backup, the cry-wolf shape the coverage card
			// refuses. Same edge as the unreadable source, so it is logged
			// once and never resolves an active alert.
			err = fmt.Errorf("%d snapshot director(y/ies) could not be read", skipped)
		}
		if err != nil {
			// A configured-but-unreadable source (broken mount, revoked S3
			// credentials, deleted bucket) disarms this whole check — that
			// must never be silent: a broken window would go undetected, and
			// an active alert freezes with a dead evaluator.
			if w.unknownEdge.Fire("staleness-source:"+edgeID, "") {
				msg := "baseline staleness cannot be evaluated: the baseline source is unreadable; a broken restore window would go UNDETECTED for this server"
				if skipped > 0 {
					msg = "baseline staleness cannot be evaluated: the baseline source could only be read in part; a broken restore window would go UNDETECTED for this server"
				}
				slog.Warn(msg, "server", t.name, "source", t.source, "error", err)
			}
			continue
		}
		w.unknownEdge.Resolve("staleness-source:" + edgeID)
		if len(files) == 0 {
			// Routine on fresh installs — but while a broken alert is ACTIVE
			// it means the stale snapshots vanished without replacement
			// (prune loop, lifecycle rule, emptied mount): restore is still
			// impossible and the alert would silently freeze, so warn.
			// Never a Resolve — nothing was fixed.
			if w.n.edge.Active("baseline:"+edgeID) && w.unknownEdge.Fire("staleness-empty:"+edgeID, "") {
				slog.Warn("baseline source lists NO snapshots while a baseline_stale alert is active — restore is still impossible and the alert is frozen; take a fresh baseline",
					"server", t.name, "source", t.source)
			}
			continue
		}
		w.unknownEdge.Resolve("staleness-empty:" + edgeID)
		floor, err := w.oldestDelta(ctx, t.dsn)
		if err != nil || floor.Hour.IsZero() {
			// Unknown floor is never a verdict — and must never RESOLVE an
			// active broken alert either, so the target is skipped whole.
			// Skipped, not silent: same rule as the unreadable source above.
			if w.unknownEdge.Fire("staleness-floor:"+edgeID, "") {
				slog.Warn("baseline staleness cannot be evaluated — the delta-coverage floor is unknown (index unreachable or unpartitioned)",
					"server", t.name, "error", err)
			}
			continue
		}
		w.unknownEdge.Resolve("staleness-floor:" + edgeID)
		now := time.Now().UTC()
		newest := make(map[string]time.Time, len(files))
		for _, f := range files {
			k := f.Schema + "." + f.Table
			if f.SnapshotTime.After(newest[k]) {
				newest[k] = f.SnapshotTime
			}
		}
		// ALL broken tables, sorted — the edge detail must be a stable
		// identity, and a map-iteration-ordered single pick would flip
		// between cycles and re-fire through the repeat window.
		var brokenTables []string
		var ungradable bool
		for k, ts := range newest {
			switch floor.Grade(ts, now) {
			case status.BaselineBroken:
				brokenTables = append(brokenTables, k)
			case status.BaselineUnknown:
				// Keyed on the VERDICT, not on the floor flag: whatever makes
				// a table ungradable, grading the rest and reporting
				// broken=false below would resolve on partial evidence.
				ungradable = true
			}
		}
		if ungradable {
			// #1219: on a multi-source index the archives cannot be attributed
			// to the source that owns these baselines, so a snapshot older than
			// the live window is unknowable. It must NOT fall through to the
			// call below: with no broken table left, that call reports
			// broken=false and RESOLVES an active alert — adding a second
			// source to an index would silently clear a real broken-baseline
			// alert. Skip the target whole, exactly like an unknown floor.
			reason := "a baseline snapshot carries no usable timestamp"
			if floor.BelowIsUnknown {
				reason = "this index serves more than one source, so archived coverage below the live index window cannot be attributed to the source that owns these baselines"
			}
			if w.unknownEdge.Fire("staleness-attribution:"+edgeID, "") {
				slog.Warn("baseline staleness cannot be evaluated for at least one table; a broken restore window would go UNDETECTED for this server",
					"server", t.name, "source", t.source, "reason", reason,
					"live_floor", floor.Hour.UTC().Format(time.RFC3339))
			}
			continue
		}
		w.unknownEdge.Resolve("staleness-attribution:" + edgeID)
		sort.Strings(brokenTables)
		w.n.BaselineStale(t.name, edgeID, len(brokenTables) > 0,
			strings.Join(brokenTables, ", "), floor.Hour.UTC().Format(time.RFC3339))
	}
}

func oldestDeltaByDSN(ctx context.Context, dsn string) (status.DeltaFloor, error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return status.DeltaFloor{}, err
	}
	defer db.Close()
	return status.OldestDeltaFromDB(ctx, db, indexDBName(dsn))
}

// continuityTarget is one index DB the watcher polls.
type continuityTarget struct {
	name string
	dsn  string
}

// startContinuityWatch polls each index's stream_state for a stamped
// gap_lost_at, publishes the verdict as a Prometheus gauge (#1203), and —
// when a notifier is configured — edge-notifies transitions. Its own loop,
// deliberately NOT piggybacked on rotation (rotation can be off) or the
// metrics scraper (it lives in the capture plane). n may be nil: the watch
// then serves the pull path only (started under --metrics-addr without
// --notify-webhook).
func startContinuityWatch(ctx context.Context, n *watchNotifier, registry *console.Registry, bootDSN string) {
	w := &continuityWatcher{
		n: n, registry: registry, bootDSN: bootDSN,
		unknownEdge: notify.NewEdge(0),
		prevNames:   make(map[string]bool),
		readGap:     readGapLost,
	}
	go func() {
		if ctx.Err() == nil {
			w.runCycle(ctx)
		}
		tick := time.NewTicker(continuityPollInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				w.runCycle(ctx)
			}
		}
	}()
}

// continuityWatcher is the poller's state, split from the goroutine so a unit
// test can drive cycles with an injected readGap — no ticker, no real DB.
// All fields are touched only by the poller goroutine (or the test driving
// runCycle directly).
type continuityWatcher struct {
	n        *watchNotifier // nil = gauge-only (started under --metrics-addr alone)
	registry *console.Registry
	bootDSN  string

	// unknownEdge rate-limits the cannot-evaluate warning (one per day per
	// index) — its own edge so it works with a nil notifier too.
	unknownEdge *notify.Edge
	// prevNames: every gauge label this watcher owned after the last cycle —
	// the boot label plus ALL registry entry names (not the DSN-deduped poll
	// targets: a second entry sharing a DSN still publishes verify gauges
	// under its own name and must be cleaned up when it goes). A renamed or
	// deleted server's gauges are UNPUBLISHED, not frozen at their last value
	// (a stale gap_lost=1 would alert forever; a stale gap_lost=0 would read
	// "evaluated, healthy" for an index nobody evaluates).
	prevNames map[string]bool
	readGap   func(ctx context.Context, dsn string) (gapLost bool, detail string, err error)
}

func (w *continuityWatcher) runCycle(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("continuity watch cycle panicked; watching continues next tick", "panic", r)
		}
	}()
	curNames := w.ownedNames()
	for name := range w.prevNames {
		if !curNames[name] {
			observe.ClearContinuity(name)
			observe.DeleteVerifyOutcome(name)
		}
	}
	w.prevNames = curNames
	for _, t := range continuityTargets(w.registry, w.bootDSN) {
		if ctx.Err() != nil {
			return
		}
		gapLost, detail, err := w.readGap(ctx, t.dsn)
		if err != nil {
			// Unknown is never "no gap" — and never silent either: a watcher
			// that cannot watch is itself a coverage hole. The gauge is
			// UNPUBLISHED (never a healthy 0) and the edge keeps the warning
			// to one per day per index.
			observe.ClearContinuity(t.name)
			if w.unknownEdge.Fire("continuity-unknown:"+t.dsn, "") {
				slog.Warn("continuity watch cannot evaluate this index (unreachable, or a legacy schema without the gap columns) — gap_lost will NOT be detected for it",
					"server", t.name, "error", err)
			}
			continue
		}
		w.unknownEdge.Resolve("continuity-unknown:" + t.dsn)
		observe.SetContinuityGapLost(t.name, gapLost)
		if w.n != nil {
			w.n.Continuity(t.name, t.dsn, gapLost, detail)
		}
	}
}

// ownedNames is the full set of gauge labels this watcher is responsible
// for cleaning up — see prevNames.
func (w *continuityWatcher) ownedNames() map[string]bool {
	out := make(map[string]bool)
	if w.bootDSN != "" {
		out[bootContinuityName] = true
	}
	if w.registry != nil {
		for _, e := range w.registry.List() {
			out[e.Name] = true
		}
	}
	return out
}

// continuityTargets enumerates the boot index (when watch was given one) plus
// every registry server, deduplicated by DSN — unlike scheduled verify, the
// continuity check is cheap enough to cover the command-line boot stream too.
func continuityTargets(registry *console.Registry, bootDSN string) []continuityTarget {
	var out []continuityTarget
	seen := make(map[string]bool)
	if bootDSN != "" {
		out = append(out, continuityTarget{name: bootContinuityName, dsn: bootDSN})
		seen[bootDSN] = true
	}
	if registry != nil {
		for _, e := range registry.List() {
			if seen[e.DSN] {
				continue
			}
			seen[e.DSN] = true
			out = append(out, continuityTarget{name: e.Name, dsn: e.DSN})
		}
	}
	return out
}

// readGapLost reads the gap_lost stamp from one index's stream_state. A
// non-nil error means the state is unknowable right now (index unreachable,
// or a legacy schema without the gap columns) — the caller must treat it as
// unknown, never as "no gap", and must say so.
func readGapLost(ctx context.Context, dsn string) (gapLost bool, detail string, err error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return false, "", err
	}
	defer db.Close()
	var lost bool
	var d sql.NullString
	err = db.QueryRowContext(ctx,
		"SELECT gap_lost_at IS NOT NULL, gap_lost_detail FROM stream_state WHERE id = 1").Scan(&lost, &d)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Empty stream_state = no capture ran = genuinely no continuity to
		// break (same rule as verify's gap evaluation).
		return false, "", nil
	case err != nil:
		return false, "", err
	}
	return lost, d.String, nil
}
