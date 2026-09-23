package console

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
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

// FirstRunReport is GET /api/servers/{id}/first-run. Complete: the list is no
// longer shown, which is when a SNAPSHOT exists for the server and no capture
// step has failed (#1801). The list used to end at the first captured change
// and take its backup step with it, so from then on nothing on the Overview
// mentioned backups.
//
// A captured change is deliberately NOT required: seeing your first change is
// a step someone may skip, because nobody can be made to write to production
// to get on, so a person who takes the snapshot instead has finished. The
// backup step's own state is what says a snapshot exists, and a failure on
// either side keeps the list up.
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
	// SnapshotExists: a complete snapshot sits in one of the server's own
	// backup locations, whoever made it (this process, one before a restart,
	// the command line). The backup job's own state forgets a snapshot the
	// moment the daemon restarts, so the list reads the locations too.
	// SnapshotCheckError is why they could not be read; it is never taken to
	// mean "no snapshot".
	SnapshotExists     bool
	SnapshotCheckError string
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
				step.Fix = "Go to Servers and click the Start button."
			}
		}
		rep.Steps = append(rep.Steps, step)
	}
	step, ok := backupStep(in)
	if !ok {
		// No backup step to wait for (a PostgreSQL server with no slot or
		// publication, which cannot capture either): the older rule stands,
		// the first indexed change.
		return rep
	}
	rep.Steps = append(rep.Steps, step)
	// A backup ends the list, whatever the capture steps are still doing: the
	// step that can be left over, seeing the first change, is not one anybody
	// can make happen. The one thing that keeps it up is a backup step that
	// FAILED, which is not a done one, so an older snapshot cannot hide last
	// night's failure.
	//
	// A capture step that failed does NOT keep it up, deliberately. This is a
	// strip of setup steps, not a health indicator: the Overview says on its
	// own when it has stopped updating, which is where a dead stream belongs,
	// and a rule that held the strip open for one would bring a finished
	// person's setup checklist back days after they watched it go. Capture
	// failing BEFORE any backup exists still keeps the strip, by this same
	// line, with no exception needed.
	rep.Complete = step.State == firstRunDone
	return rep
}

// backupStep is the list's last step, or false when the list has none.
func backupStep(in firstRunInput) (FirstRunStep, bool) {
	b := in.Backup
	if b == nil {
		step, ok := blockedBackupStep(in)
		if ok && in.SnapshotExists {
			// Backups are off here, or the server has no job of its own, and
			// a snapshot exists anyway (the command line, an earlier setup):
			// nothing is owed.
			return FirstRunStep{Name: step.Name, State: firstRunDone}, true
		}
		if ok {
			step.Detail = withCheckError(step.Detail, in.SnapshotCheckError)
		}
		return step, ok
	}
	step := FirstRunStep{Name: backupStepName, State: firstRunWaiting, Fix: "Create one on the " + PageSnapshots + " page."}
	// This run's own outcome is read BEFORE an older snapshot. A backup that
	// failed last night on a server backed up last week would otherwise read
	// as done, and the list would take the error and its fix away with it.
	// A run that published and only failed to send its copy on (#1725) did
	// produce a backup, so it stays ahead of both.
	switch {
	case b.Published || b.State == "succeeded":
		step.State, step.Fix = firstRunDone, ""
	case b.State == "failed":
		step.State, step.Detail, step.Fix = firstRunFailed, withCheckError(b.LastError, in.SnapshotCheckError), "Try again on the "+PageSnapshots+" page."
	case b.State == "running":
		step.State, step.Detail, step.Fix = firstRunRunning, withCheckError("", in.SnapshotCheckError), ""
	case in.SnapshotExists:
		step.State, step.Fix = firstRunDone, ""
	default:
		step.Detail = withCheckError("", in.SnapshotCheckError)
	}
	return step, true
}

// backupStepName is the last step's name, in one place: firstRunBackupIsNext
// finds the step by it.
const backupStepName = "Take the first full DB snapshot"

// withCheckError adds why the backup locations could not be read to a step's
// detail. The step stays up and says so: an unreadable location is not a
// location with no snapshot, and one that cannot be read is worth knowing
// about before a restore needs it.
func withCheckError(detail, checkErr string) string {
	if checkErr == "" {
		return detail
	}
	note := "Could not check for an existing backup: " + checkErr
	if detail == "" {
		return note
	}
	return detail + " " + note
}

// blockedBackupStep is the backup step for a server whose first backup the
// console cannot create, with the reason and what to do (#1677). The daemon
// setting is named as the Snapshots page labels it, never as a
// variable. mydumper is named for every source but PostgreSQL (MySQL and
// MariaDB): a PostgreSQL full backup runs inside DBTrail. The step can never
// be done from here while backups are off; it is done when a snapshot exists
// anyway (backupStep), and until then the list stays up with it (#1801).
func blockedBackupStep(in firstRunInput) (FirstRunStep, bool) {
	step := FirstRunStep{Name: backupStepName, State: firstRunWaiting}
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
		step.Fix = "On the " + PageSnapshots + " page, set this server's Backup dir or Backup S3 under Where and how often, then press Create backup."
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
	s.checkOwnSnapshot(r.Context(), e, &in)
	writeJSON(w, http.StatusOK, firstRunSteps(in))
}

// snapshotCheckTTL is how long one answer about a server's own backup
// locations is reused.
//
// A snapshot ends this list whatever the capture steps are doing, so the
// locations have to be read while the list shows — and it is polled every few
// seconds, while each answer costs a read of every location the server names,
// which for an S3 one is a listing over the network. Reusing the answer for a
// minute keeps that to one listing a minute per server instead of one every
// three seconds. What it costs: a backup that appears from somewhere this
// process cannot see (the schedule, the command line, another tab) is noticed
// up to a minute late. A backup this process took is noticed at once, because
// the job's own state answers before any location is read.
const snapshotCheckTTL = time.Minute

// snapshotCheck is one memoized answer: whether a location held a complete
// snapshot, or why that could not be told. It carries the locations it was
// read from, so an edited server is read again rather than answered from the
// place it no longer uses. Keyed by the server alone, so an edit REPLACES the
// answer instead of leaving the old one behind for the life of the process.
type snapshotCheck struct {
	dir   string
	s3    string
	found bool
	err   string
	at    time.Time
}

// checkOwnSnapshot fills in whether one of the server's own backup locations
// holds a complete snapshot (#1801). Only its own: a backup made from here
// writes nowhere else (hasOwnBackupLocation), and a daemon-wide location can
// hold another server's snapshots. It answers without reading anything where
// reading cannot help: an index that could not be read claims no step, a
// PostgreSQL server with no slot lists no backup step, and a backup this
// process published already says one exists. Everything else is read at most
// once per snapshotCheckTTL, and each location is asked for its newest
// snapshot only.
func (s *Server) checkOwnSnapshot(ctx context.Context, e ServerEntry, in *firstRunInput) {
	if in.CheckError != "" || pgSourceIncomplete(e) || (in.Backup != nil && (in.Backup.Published || in.Backup.State == "succeeded")) {
		return
	}
	if c, ok := s.cachedSnapshotCheck(e); ok {
		in.SnapshotExists, in.SnapshotCheckError = c.found, c.err
		return
	}
	list := s.snapshotLister
	if list == nil {
		list = hasCompleteSnapshot
	}
	var errs []string
	for _, src := range []string{e.BaselineDir, e.BaselineS3} {
		if src == "" {
			continue
		}
		ok, err := list(ctx, src)
		if ok {
			in.SnapshotExists, in.SnapshotCheckError = true, ""
			s.storeSnapshotCheck(e, snapshotCheck{found: true})
			return
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	in.SnapshotCheckError = strings.Join(errs, "; ")
	s.storeSnapshotCheck(e, snapshotCheck{err: in.SnapshotCheckError})
}

// snapshotNowOr is the cache's clock (a test fixes it).
func (s *Server) snapshotNowOr() time.Time {
	if s.snapshotNow != nil {
		return s.snapshotNow()
	}
	return time.Now()
}

func (s *Server) cachedSnapshotCheck(e ServerEntry) (snapshotCheck, bool) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	c, ok := s.snapshotChecks[e.ID]
	if !ok || c.dir != e.BaselineDir || c.s3 != e.BaselineS3 {
		return snapshotCheck{}, false
	}
	if s.snapshotNowOr().Sub(c.at) >= snapshotCheckTTL {
		return snapshotCheck{}, false
	}
	return c, true
}

func (s *Server) storeSnapshotCheck(e ServerEntry, c snapshotCheck) {
	c.dir, c.s3, c.at = e.BaselineDir, e.BaselineS3, s.snapshotNowOr()
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if s.snapshotChecks == nil {
		s.snapshotChecks = map[string]snapshotCheck{}
	}
	s.snapshotChecks[e.ID] = c
}

// hasCompleteSnapshot reports whether a backup location holds a complete
// snapshot. A local folder that does not exist yet holds none, which is not
// an error: the first backup creates it. A listing that found nothing but had
// to skip folders it could not read has no answer, and says so.
func hasCompleteSnapshot(ctx context.Context, source string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, baselineListTimeout)
	defer cancel()
	files, skipped, _, err := reconstruct.ListBaselinesNewestReport(ctx, source, 1)
	switch {
	case err != nil && baselineKindOf(source) == "dir" && errors.Is(err, fs.ErrNotExist):
		return false, nil
	case err != nil:
		return false, err
	case len(files) > 0:
		return true, nil
	case skipped > 0:
		return false, fmt.Errorf("%s: %d snapshot folder(s) could not be read", source, skipped)
	}
	return false, nil
}
