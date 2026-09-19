package rotation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
)

// Settings is the parsed configuration of `up`'s built-in rotation —
// the loop that keeps an unattended index from growing until the volume fills
// and takes the forensic record with it (#420).
type Settings struct {
	Enabled   bool
	Retain    time.Duration
	RetainRaw string
	Interval  time.Duration
	AddFuture int
	// Explicit records whether the operator configured --rotate-retain
	// themselves (flag or env — bindCommandEnv marks env-set flags Changed).
	// When false (running on the built-in default), the upgrade guard
	// refuses to drop pre-existing deep history: an operator who never chose
	// a retention must not lose months of forensic record to a binary
	// upgrade.
	Explicit bool
}

// ParseSettings validates the --rotate-* flag values and is the sole
// author of every Settings field. retain accepts the rotate
// command's Nd/Nh forms, plus "off", "0", or "" to disable the built-in
// rotation entirely. explicit is whether the operator set --rotate-retain
// themselves (cmd.Flags().Changed — true for flag and env alike).
func ParseSettings(retain, interval string, addFuture int, explicit bool) (Settings, error) {
	switch retain {
	case "off", "0", "":
		return Settings{}, nil
	}
	dur, err := cliutil.ParseRetain(retain)
	if err != nil {
		return Settings{}, fmt.Errorf("--rotate-retain: %w (or \"off\" to disable)", err)
	}
	iv, err := time.ParseDuration(interval)
	if err != nil {
		return Settings{}, fmt.Errorf("--rotate-interval: %w", err)
	}
	if iv <= 0 {
		return Settings{}, fmt.Errorf("--rotate-interval must be positive, got %q", interval)
	}
	if addFuture < 0 {
		return Settings{}, fmt.Errorf("--rotate-add-future cannot be negative, got %d", addFuture)
	}
	return Settings{
		Enabled:   true,
		Retain:    dur,
		RetainRaw: retain,
		Interval:  iv,
		AddFuture: addFuture,
		Explicit:  explicit,
	}, nil
}

// RotateTarget is one index database the loop rotates this cycle, plus its
// optional per-source archive config. ArchiveS3 == "" means drop-only (the
// historical behavior). When ArchiveS3 is set, the cycle archives each expired
// partition to ArchiveDir (a local staging dir) under bintrail_id=BintrailID,
// uploads it to ArchiveS3, then drops it — and prunes the local copy. The
// provider (cmd/bintrail-console) is responsible for only setting the archive
// fields once BintrailID is known (resolved from the source's stream_state).
type RotateTarget struct {
	DSN string
	// NoWriter: as far as this daemon knows, nothing streams into this
	// index (the boot index of a source-less `watch`, #1715). The daemon
	// cannot verify that — another process may point at the same DSN — which
	// is why the mark alone decides nothing: a per-cycle probe of the events
	// table does. While it holds no events its rotation is
	// housekeeping over empty partitions — still done, so the layout stays
	// current for the day a stream starts writing it and never grows toward
	// the partition cap — but logged as such (Options.QuietEmpty) instead of
	// as freed space. A per-source index always has a writer and never sets
	// this. Once such an index holds events (a source-ful era left them) it
	// is rotated and logged like any other.
	NoWriter           bool
	ArchiveDir         string
	ArchiveS3          string
	ArchiveS3Region    string
	BintrailID         string
	ArchiveCompression string
}

// StartLoop announces and launches the built-in rotation loop: one
// cycle immediately, then one every interval, each cycle rotating every target
// the provider returns (the boot index plus the control plane's per-source
// databases). Rotation is the secondary job — failures are logged, never fatal
// to the stream. Returns immediately; the loop stops when ctx is cancelled, and
// the returned channel closes when it has fully exited (used by tests for
// deterministic shutdown; production callers may ignore it).
//
// settings is a PROVIDER, read fresh each cycle (mirroring the targets
// provider): the console can edit the global rotation policy at runtime, and an
// edit applies on the next tick — retain/add-future immediately, and a changed
// interval re-tunes the ticker. The enabled/disabled decision and the startup
// banner are taken once from the initial read: a daemon started with rotation
// off runs no loop (re-enabling needs a restart).
// onCycle callbacks (optional) observe each cycle's health — failed reports a
// rotation error, deferred counts unarchived partitions the cycle declined to
// drop. They run inside the cycle's recover guard, so a panicking callback
// cannot take down the loop.
func StartLoop(ctx context.Context, settings func() Settings, targets func() []RotateTarget, onCycle ...func(failed bool, deferred int)) <-chan struct{} {
	done := make(chan struct{})
	s0 := settings()
	if !s0.Enabled {
		fmt.Fprintln(os.Stderr, "Built-in rotation: off — the index grows until you rotate it yourself (see `bintrail rotate`)")
		slog.Info("built-in rotation disabled")
		close(done)
		return done
	}
	fmt.Fprintf(os.Stderr,
		"Built-in rotation: dropping index partitions older than %s every %s, keeping %d future partitions ready.\n"+
			"  Tune with --rotate-retain / --rotate-interval (or BINTRAIL_ROTATE_RETAIN), or live from the console; disable with --rotate-retain off.\n",
		s0.RetainRaw, s0.Interval, s0.AddFuture)
	slog.Info("built-in rotation enabled",
		"retain", s0.RetainRaw, "interval", s0.Interval.String(), "add_future", s0.AddFuture)

	go func() {
		defer close(done)
		// Consecutive-unhealthy-cycle streak for escalation: an hourly Warn
		// blends into noise, but a rotation that makes no progress it should
		// have — failing, deferring to a stalled archiving flow, or any
		// alternation of the two — for hours means the index is growing
		// unbounded, the exact condition this loop exists to detect. ONE
		// streak over (failed || deferred>0), not per-reason counters: split
		// counters each reset while the other condition fires, so an index
		// alternating defer/fail would never escalate.
		var unhealthyStreak int
		// runOne reads the current settings (the console may have edited them
		// since the last tick), rotates every target, and returns the interval
		// it saw so the caller can re-tune the ticker. The settings read is
		// outside the recover so a panicking cycle still reports the intended
		// interval; the rotation work itself is recover-guarded because a panic
		// here must never take down the stream (the primary forensic capture).
		runOne := func() time.Duration {
			s := settings()
			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("built-in rotation cycle panicked; rotation continues next tick", "panic", r)
					}
				}()
				deferred, failed := runCycle(ctx, s, targets)
				if failed || deferred > 0 {
					unhealthyStreak++
				} else {
					unhealthyStreak = 0
				}
				if unhealthyStreak >= escalateAfter {
					slog.Error("built-in rotation made no progress for consecutive cycles — the index is growing unbounded (rotation is failing and/or deferring unarchived partitions to a stalled archiving flow; archive the partitions, fix the failure, or set --rotate-retain off and rotate manually)",
						"consecutive_cycles", unhealthyStreak,
						"deferred_last_cycle", deferred,
						"failed_last_cycle", failed)
				}
				// Callbacks run LAST so a panicking callback (caught by the
				// guard above) can never skip the streak accounting or the
				// escalation log it exists to amplify.
				for _, cb := range onCycle {
					cb(failed, deferred)
				}
			}()
			return s.Interval
		}
		runOne()
		iv := s0.Interval
		ticker := time.NewTicker(iv)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if newIV := runOne(); newIV > 0 && newIV != iv {
					ticker.Reset(newIV)
					iv = newIV
					slog.Info("built-in rotation interval changed", "interval", iv.String())
				}
			}
		}
	}()
	return done
}

// escalateAfter is how many consecutive unhealthy cycles (failed
// or deferring) the loop tolerates before escalating from Warn to Error. A
// var, not a const, so tests can shrink it.
var escalateAfter = 3

// runCycle rotates each index database once. Errors are logged and
// the cycle moves to the next target — a transient failure self-heals on the
// next tick. Returns the total number of partitions the protect-unarchived
// guard deferred this cycle, and whether any database's rotation failed.
func runCycle(ctx context.Context, s Settings, targets func() []RotateTarget) (deferred int, failed bool) {
	for _, t := range dedupeTargets(targets()) {
		d, err := rotateOneIndex(ctx, t, s)
		deferred += d
		if err != nil {
			failed = true
		}
	}
	return deferred, failed
}

// dedupeTargets drops empty-DSN and duplicate-DSN targets, preserving order.
func dedupeTargets(in []RotateTarget) []RotateTarget {
	seen := make(map[string]bool, len(in))
	out := make([]RotateTarget, 0, len(in))
	for _, t := range in {
		if t.DSN == "" || seen[t.DSN] {
			continue
		}
		seen[t.DSN] = true
		out = append(out, t)
	}
	return out
}

// indexHoldsNoEvents reports whether binlog_events has no rows at all. One
// row is enough to answer, so this is an index probe, not a count.
func indexHoldsNoEvents(ctx context.Context, db *sql.DB, dbName string) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM `"+dbName+"`.binlog_events LIMIT 1").Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("probe binlog_events: %w", err)
	}
	return false, nil
}

// loopOptions builds the Perform Options for one built-in-rotation cycle. It
// ALWAYS arms ProtectUnarchived so the loop can never be the first to destroy
// data an external archiving flow would preserve; Format "json" suppresses
// per-partition stdout (slog carries the per-cycle signal). When the target
// carries an ArchiveS3 bucket, the cycle archives-then-drops (and prunes the
// local staging copy after upload); otherwise ArchiveDir is empty and it
// drops-and-tops-up only — the historical behavior.
func loopOptions(retain time.Duration, retainRaw string, s Settings, t RotateTarget, quietEmpty bool) Options {
	o := Options{
		RetainDur:         retain,
		RetainRaw:         retainRaw,
		AddFuture:         s.AddFuture,
		NoReplace:         false,
		Format:            "json",
		ProtectUnarchived: true,
		QuietEmpty:        quietEmpty,
	}
	if t.ArchiveS3 != "" {
		o.ArchiveDir = t.ArchiveDir
		o.ArchiveS3 = t.ArchiveS3
		o.ArchiveS3Region = t.ArchiveS3Region
		o.BintrailID = t.BintrailID
		o.ArchiveCompression = t.ArchiveCompression
		o.Retry = true                 // skip re-archiving/re-uploading what a prior cycle already did
		o.PruneLocalAfterUpload = true // unattended daemon: don't grow the staging dir
	}
	return o
}

// rotateOneIndex runs one Perform cycle against a single target's index DSN,
// returning the guard-deferred partition count and any failure. Log messages
// are scrubbed: DSNs (and their passwords) never reach the log.
func rotateOneIndex(ctx context.Context, t RotateTarget, s Settings) (int, error) {
	dsn := t.DSN
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		slog.Warn("built-in rotation: skipping unparseable index DSN",
			"error", config.ScrubDSNText(err.Error(), dsn))
		return 0, fmt.Errorf("parse index DSN: %w", err)
	}
	if cfg.DBName == "" {
		slog.Warn("built-in rotation: skipping index DSN without a database name")
		return 0, fmt.Errorf("index DSN without a database name")
	}
	db, err := config.Connect(dsn)
	if err != nil {
		slog.Warn("built-in rotation: cannot connect to index database",
			"db", cfg.DBName, "error", config.ScrubDSNText(err.Error(), dsn))
		return 0, err
	}
	defer db.Close()

	// The probe runs BEFORE Perform on purpose: an index being drained by
	// retention logs its last real drops at Info only because it still held
	// events when it was asked. Probed after the drops, they would go to
	// Debug.
	quietEmpty := false
	if t.NoWriter {
		empty, err := indexHoldsNoEvents(ctx, db, cfg.DBName)
		if err != nil {
			if ctx.Err() != nil {
				// Shutdown mid-cycle: not a failure of this target, as the
				// Perform path below reads it.
				return 0, nil
			}
			slog.Warn("built-in rotation: could not tell whether the index holds events; counting this cycle as failed",
				"db", cfg.DBName, "error", config.ScrubDSNText(err.Error(), dsn))
			return 0, err
		}
		if empty {
			slog.Debug("built-in rotation: index holds no events and has no writer; rotating its empty partitions as housekeeping", "db", cfg.DBName)
			quietEmpty = true
		}
	}

	retain, retainRaw := s.Retain, s.RetainRaw
	if !s.Explicit {
		// No explicit choice: the index itself says which window it runs on
		// (#1709). The record is written when the index is CREATED, so an
		// index that predates the record — an older build's — is recognised
		// by its absence and keeps LegacyRetain, the window it has been
		// running on all along. Changing the default must never shorten an
		// existing index's window behind the operator's back.
		value, found, readErr := indexer.ReadInitialRetain(ctx, db, cfg.DBName)
		imp, err := implicitRetainFrom(value, found, readErr)
		if err != nil {
			if ctx.Err() != nil {
				return 0, nil // shutdown mid-cycle, not a failure
			}
			slog.Warn("built-in rotation: could not read which retention this index was created under; keeping "+LegacyRetain+", the window every index kept before that was recorded",
				"db", cfg.DBName, "error", config.ScrubDSNText(err.Error(), dsn))
		} else if imp.retain != s.Retain {
			noticeKeptRetain(dsn, cfg.DBName, imp.raw, s.RetainRaw)
		}
		retain, retainRaw = imp.retain, imp.raw

		if !imp.recorded {
			// Upgrade guard, for an index that records nothing: its operator
			// never chose a retention AND no build of ours wrote down which
			// window it started on. If the oldest partition extends far
			// beyond that window — the signature of a pre-existing deployment
			// that predates built-in rotation — refuse to drop it and demand
			// an explicit choice.
			//
			// An index that DOES carry a record is exempt: its whole history
			// accumulated under the window it records, so depth beyond it is
			// the daemon having been stopped, not an upgrade changing the
			// rules. Guarding it would turn any outage longer than the window
			// into a refusal to drop anything — while the catch-up that
			// follows writes the fastest the index ever grows.
			guarded, oldest, err := upgradeGuardTrips(ctx, db, cfg.DBName, retain)
			if err != nil {
				slog.Warn("built-in rotation: could not evaluate the upgrade guard; skipping drops this cycle",
					"db", cfg.DBName, "error", config.ScrubDSNText(err.Error(), dsn))
				return 0, err
			}
			if guarded {
				slog.Error("built-in rotation: existing history extends far beyond the default retention — refusing to drop it without an explicit choice",
					"db", cfg.DBName,
					"oldest_partition", oldest.UTC().Format("2006-01-02 15:04"),
					"default_retain", retainRaw,
					"action", "set --rotate-retain explicitly (e.g. 30d to confirm, 90d to keep more, off to disable) or BINTRAIL_ROTATE_RETAIN")
				retain = 0 // still top up future partitions; no drops
			}
		}
	}

	res, err := Perform(ctx, db, cfg.DBName, loopOptions(retain, retainRaw, s, t, quietEmpty))
	if err != nil && ctx.Err() == nil {
		slog.Warn("built-in rotation cycle failed",
			"db", cfg.DBName, "error", config.ScrubDSNText(err.Error(), dsn))
		return res.Deferred, err
	}
	return res.Deferred, nil
}

// upgradeGuardTrips reports whether the implicit-default upgrade guard should
// block drops: true when the oldest named hourly partition is more than twice
// the retain window old. Returns the oldest partition hour for the log line.
func upgradeGuardTrips(ctx context.Context, db *sql.DB, dbName string, retain time.Duration) (bool, time.Time, error) {
	partitions, err := listPartitions(ctx, db, dbName)
	if err != nil {
		return false, time.Time{}, err
	}
	trips, oldest := guardTrips(partitions, retain, time.Now())
	return trips, oldest, nil
}

// guardTrips is the pure decision behind the upgrade guard: strict-> on twice
// the retain window, measured against the oldest named hourly partition.
// p_future and malformed names are ignored; no named partitions (a fresh
// install) never trips.
func guardTrips(partitions []partitionInfo, retain time.Duration, now time.Time) (bool, time.Time) {
	var oldest time.Time
	for _, p := range partitions {
		d, ok := indexer.PartitionDate(p.Name)
		if !ok {
			continue
		}
		if oldest.IsZero() || d.Before(oldest) {
			oldest = d
		}
	}
	if oldest.IsZero() {
		return false, time.Time{}
	}
	return now.Sub(oldest) > 2*retain, oldest
}
