package console

import (
	"context"
	"database/sql"
	"errors"
	"net/http"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// First-run step states (#1606). Waiting is a step that has not started, which
// must never read as failed to someone who has not seen a working install.
const (
	firstRunWaiting = "waiting"
	firstRunRunning = "running"
	firstRunDone    = "done"
	firstRunFailed  = "failed"
)

// FirstRunStep is one row of the first-run list.
type FirstRunStep struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
	Fix    string `json:"fix,omitempty"`
}

// FirstRunReport is GET /api/servers/{id}/first-run. Complete: the server has
// indexed a change, and the list is no longer shown.
type FirstRunReport struct {
	Complete   bool           `json:"complete"`
	Steps      []FirstRunStep `json:"steps"`
	CheckError string         `json:"check_error,omitempty"`
}

// firstRunInput is what the step list is computed from: the supervisor's view
// and what the server's own index database holds.
type firstRunInput struct {
	Monitor MonitorStatus
	// IndexExists is nil when the index database could not be checked.
	IndexExists   *bool
	SnapshotTaken bool
	StreamStarted bool
	EventsIndexed int64
	// HasEvents: binlog_events holds a change though the counter says none,
	// as after a reset, which zeroes the counter and keeps the rows.
	HasEvents  bool
	CheckError string
	// Postgres: the source is PostgreSQL, whose stream saves the table
	// structure only when changes arrive.
	Postgres bool
	// Backup is the first-backup job, nil when the console cannot create one
	// for this server.
	Backup *BaselineStatus
	// BackupOff: this process cannot create full backups at all (the creation
	// opt-in is off). BackupNoLocation: the server has no backup location of
	// its own. Either one lists the backup step with its reason instead of
	// leaving it out (#1677): a list with no backup step reads as an install
	// that needs none.
	BackupOff        bool
	BackupNoLocation bool
}

// firstRunSteps computes the list. Each capture step is done from evidence: the
// index database, the supervisor's report that this run reached the source,
// and what the index holds. A later step's evidence implies the earlier ones.
// The first step not done takes the supervisor's state: running while it
// works (with what was lost when events were skipped for good), failed with
// its error and a fix when it failed or stalled, waiting for Start when
// capture is stopped. The steps after it wait. "Running" is not "stuck": a
// started stream with no change yet is healthy, and the supervisor reports
// "stalled" when it is not.
func firstRunSteps(in firstRunInput) FirstRunReport {
	if in.CheckError != "" {
		// Nothing is claimed from an index that could not be read: a server
		// with months of changes must not read "Creating it now" because its
		// index server refused one connection.
		return FirstRunReport{CheckError: in.CheckError}
	}
	type stepDef struct {
		name, working string
		done          bool
	}
	// Attached counts as started only once this run reached the source: a
	// PostgreSQL stream's ticker reports progress before the capturer connects.
	attached := in.Monitor.SourceConnected &&
		(in.Monitor.State == "running" || in.Monitor.State == "stalled" || in.Monitor.State == "lost_position")
	defs := []stepDef{
		{"Create the index database", "Creating it now.", in.IndexExists != nil && *in.IndexExists},
		{"Connect to the source", "Connecting to the source database.", in.Monitor.SourceConnected},
	}
	if !in.Postgres {
		// A PostgreSQL stream saves the table structure when the first
		// changes arrive, so the step would sit unfinished on a quiet source.
		defs = append(defs, stepDef{"Read the table structure", "Reading which tables and columns to capture.", in.SnapshotTaken})
	}
	defs = append(defs,
		stepDef{"Start capturing changes", "Finding where to start reading changes.", in.StreamStarted || attached},
		stepDef{"Capture the first change", "Waiting for the first change on the source. A quiet database is normal.", in.EventsIndexed > 0 || in.HasEvents},
	)
	for i := len(defs) - 2; i >= 0; i-- {
		defs[i].done = defs[i].done || defs[i+1].done
	}
	rep := FirstRunReport{Complete: defs[len(defs)-1].done}
	current := -1
	for i, d := range defs {
		step := FirstRunStep{Name: d.name, State: firstRunWaiting}
		switch {
		case d.done:
			step.State = firstRunDone
		case current < 0:
			current = i
			switch in.Monitor.State {
			case "failed":
				step.State, step.Detail = firstRunFailed, in.Monitor.LastError
				// Only a failure the supervisor will retry says so; one it gave
				// up on, or a Start that failed while setting up, waits for Start.
				step.Fix = "Fix the cause above, then press Start on this server in Servers."
				if in.Monitor.Retrying {
					step.Fix = "Capture retries on its own. Fix the cause above, or press Start on this server in Servers to run the startup checks."
				}
			case "stalled":
				step.State, step.Detail = firstRunFailed, in.Monitor.LastError
				step.Fix = "Stop and start capture on this server in Servers."
			case "pending", "running":
				step.State, step.Detail = firstRunRunning, d.working
			case "lost_position":
				// Still capturing, but events were skipped for good: say which.
				step.State, step.Detail = firstRunRunning, in.Monitor.LastError
			default:
				step.Fix = "Press Start on this server in Servers."
			}
		}
		rep.Steps = append(rep.Steps, step)
	}
	if b := in.Backup; b != nil {
		step := FirstRunStep{Name: "Take the first backup", State: firstRunWaiting, Fix: "Create one on the " + PageSnapshots + " page."}
		switch {
		case b.Published || b.State == "succeeded":
			step.State, step.Fix = firstRunDone, ""
		case b.State == "running":
			step.State, step.Fix = firstRunRunning, ""
		case b.State == "failed":
			step.State, step.Detail, step.Fix = firstRunFailed, b.LastError, "Try again on the "+PageSnapshots+" page."
		}
		rep.Steps = append(rep.Steps, step)
	} else if step, ok := blockedBackupStep(in); ok {
		rep.Steps = append(rep.Steps, step)
	}
	return rep
}

// blockedBackupStep is the backup step for a server whose first backup the
// console cannot create, with the reason and what to do (#1677). The daemon
// setting is named as the Snapshots page labels it, never as a
// variable. mydumper is named for every source but PostgreSQL (MySQL and
// MariaDB): a PostgreSQL full backup runs inside DBTrail. The step can never
// be done while backups are off, which is why Complete reads only the
// capture steps.
func blockedBackupStep(in firstRunInput) (FirstRunStep, bool) {
	step := FirstRunStep{Name: "Take the first backup", State: firstRunWaiting}
	switch {
	case in.BackupOff:
		step.Detail = "Creating full backups from the console is turned off. Restoring a whole table to a past moment needs a full backup."
		step.Fix = "On the " + PageSnapshots + " page, under Set when DBTrail starts, the Create-backup button row names the setting to change. Restart DBTrail after changing it. A full backup reads every table this server captures"
		if in.Postgres {
			step.Fix += "."
		} else {
			step.Fix += ", and mydumper must be installed where DBTrail runs."
		}
		if in.BackupNoLocation {
			step.Fix += " This server also needs its own backup location, set on that page under Where and how often."
		}
	case in.BackupNoLocation:
		step.Detail = "This server has no backup location of its own, so no backup can be written for it."
		step.Fix = "Set a Backup dir or Backup S3 for this server on the " + PageSnapshots + " page, under Where and how often, then take the backup with the Create backup button at the top of that page."
	default:
		return FirstRunStep{}, false
	}
	return step, true
}

// loadFirstRunIndex reads the evidence from the server's own index database,
// which does not exist until Start creates it. Each request opens one
// connection with a short connect timeout, like the Test connection probe,
// runs its queries on it and closes it: the bundle cache pings on selection
// and cannot open a database that is not there yet.
func loadFirstRunIndex(ctx context.Context, dsn string, in *firstRunInput) {
	no, yes := false, true
	if dsn == "" {
		in.IndexExists = &no
		return
	}
	short, _, err := shortTimeoutDSN(dsn)
	if err != nil {
		in.CheckError = scrubDSNError(err, dsn)
		return
	}
	scrub := func(err error) string { return config.ScrubDSNError(err, short, dsn) }
	db, err := config.Connect(short)
	if err != nil {
		if isUnknownDatabase(err) {
			in.IndexExists = &no
			return
		}
		in.CheckError = scrub(err)
		return
	}
	defer db.Close()
	in.IndexExists = &yes

	var me *mysql.MySQLError
	missingTable := func(err error) bool { return errors.As(err, &me) && me.Number == 1146 }
	var taken sql.NullTime
	switch err := db.QueryRowContext(ctx, "SELECT MAX(snapshot_time) FROM schema_snapshots").Scan(&taken); {
	case err == nil:
		in.SnapshotTaken = taken.Valid
	case !missingTable(err):
		in.CheckError = scrub(err)
		return
	}
	// Started means a position was saved. The row alone is not enough: a
	// PostgreSQL daemon's health poll creates it with no position before any
	// commit (the rule loadStreamStatePG applies to resume).
	var file, gtid string
	var pos uint64
	switch err := db.QueryRowContext(ctx,
		"SELECT binlog_file, binlog_position, COALESCE(gtid_set, ''), events_indexed FROM stream_state WHERE id = 1",
	).Scan(&file, &pos, &gtid, &in.EventsIndexed); {
	case err == nil:
		in.StreamStarted = file != "" || pos > 0 || gtid != ""
	case !errors.Is(err, sql.ErrNoRows) && !missingTable(err):
		in.CheckError = scrub(err)
		return
	}
	if in.EventsIndexed == 0 {
		var any bool
		switch err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM binlog_events)").Scan(&any); {
		case err == nil:
			in.HasEvents = any
		case !missingTable(err):
			in.CheckError = scrub(err)
		}
	}
}

// handleFirstRun serves GET /api/servers/{id}/first-run (#1606): the steps
// from adding a server to its first indexed change, for the page to show while
// it has none.
func (s *Server) handleFirstRun(w http.ResponseWriter, r *http.Request) {
	e, ok := s.requireMonitorEntry(w, r.PathValue("id"))
	if !ok {
		return
	}
	if e.SourceDSN == "" {
		writeJSONError(w, http.StatusConflict, "this server has no source connection, so nothing is captured from it")
		return
	}
	in := firstRunInput{Monitor: s.monitorCtrl.Status(e.ID), Postgres: e.IsPostgres()}
	loadFirstRunIndex(r.Context(), e.DSN, &in)
	// A PostgreSQL server with no slot or publication lists no backup step,
	// whether or not backups are on: capture cannot run for it either, so the
	// capture steps are what is stuck, and a backup reason would point at the
	// wrong fix. Checked first, because the precheck reports a missing
	// location before the slot. A refusal this list has no words for lists no
	// step rather than blame the location.
	switch {
	case pgSourceIncomplete(e):
	case s.baselineCtrl == nil:
		in.BackupOff, in.BackupNoLocation = true, !hasOwnBackupLocation(e)
	case baselineTriggerPrecheck(e) == nil:
		b := s.baselineCtrl.Status(e.ID)
		in.Backup = &b
	case !hasOwnBackupLocation(e):
		in.BackupNoLocation = true
	}
	writeJSON(w, http.StatusOK, firstRunSteps(in))
}
