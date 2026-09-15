package console

import (
	"context"
	"database/sql"
	"errors"
	"net/http"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/status"
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
	CheckError    string
	// Backup is the first-backup job, nil when the console cannot create one
	// for this server.
	Backup *BaselineStatus
}

// firstRunSteps computes the list. Each capture step is done from evidence in
// the index, and a later step's evidence implies the earlier ones. The first
// step not done takes the supervisor's state: running while it works, failed
// with its error and a fix, waiting for Start when capture is stopped. The
// steps after it wait. "Running" is not "stuck": a started stream with no
// change yet is healthy, and the supervisor reports "stalled" when it is not.
func firstRunSteps(in firstRunInput) FirstRunReport {
	names := []string{"Create the index database", "Read the table structure", "Start capturing changes", "Capture the first change"}
	working := []string{
		"Creating it now.",
		"Reading which tables and columns to capture.",
		"Connecting to the source and saving the first position.",
		"Capture is running and waiting for the first change on the source. A quiet database is normal.",
	}
	done := []bool{in.IndexExists != nil && *in.IndexExists, in.SnapshotTaken, in.StreamStarted, in.EventsIndexed > 0}
	for i := len(done) - 2; i >= 0; i-- {
		done[i] = done[i] || done[i+1]
	}
	rep := FirstRunReport{Complete: done[len(done)-1], CheckError: in.CheckError}
	current := -1
	for i, name := range names {
		step := FirstRunStep{Name: name, State: firstRunWaiting}
		switch {
		case done[i]:
			step.State = firstRunDone
		case current < 0:
			current = i
			switch in.Monitor.State {
			case "failed":
				step.State, step.Detail = firstRunFailed, in.Monitor.LastError
				step.Fix = "Capture retries on its own. Fix the cause above, or press Start on this server in Servers to run the startup checks."
			case "stalled":
				step.State, step.Detail = firstRunFailed, in.Monitor.LastError
				step.Fix = "Stop and start capture on this server in Servers."
			case "pending", "running", "lost_position":
				step.State, step.Detail = firstRunRunning, working[i]
			default:
				step.Fix = "Press Start on this server in Servers."
			}
		}
		rep.Steps = append(rep.Steps, step)
	}
	if b := in.Backup; b != nil {
		step := FirstRunStep{Name: "Take the first backup", State: firstRunWaiting, Fix: "Create one on the Backups page."}
		switch {
		case b.Published || b.State == "succeeded":
			step.State, step.Fix = firstRunDone, ""
		case b.State == "running":
			step.State, step.Fix = firstRunRunning, ""
		case b.State == "failed":
			step.State, step.Detail, step.Fix = firstRunFailed, b.LastError, "Try again on the Backups page."
		}
		rep.Steps = append(rep.Steps, step)
	}
	return rep
}

// loadFirstRunIndex reads the evidence from the server's own index database,
// which does not exist until Start creates it. Each probe opens and closes its
// own short-timeout connection, like the Test connection probe: the bundle
// cache pings on selection and cannot open a database that is not there yet.
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
	db, err := config.Connect(short)
	if err != nil {
		if isUnknownDatabase(err) {
			in.IndexExists = &no
			return
		}
		in.CheckError = scrubDSNError(err, dsn)
		return
	}
	defer db.Close()
	in.IndexExists = &yes

	var taken sql.NullTime
	err = db.QueryRowContext(ctx, "SELECT MAX(snapshot_time) FROM schema_snapshots").Scan(&taken)
	var me *mysql.MySQLError
	switch {
	case err == nil:
		in.SnapshotTaken = taken.Valid
	case errors.As(err, &me) && me.Number == 1146:
	default:
		in.CheckError = scrubDSNError(err, dsn)
		return
	}
	stream, err := status.LoadStreamState(ctx, db)
	switch {
	case err != nil && !(errors.As(err, &me) && me.Number == 1146):
		in.CheckError = scrubDSNError(err, dsn)
	case stream != nil:
		in.StreamStarted = !stream.LastCheckpoint.IsZero()
		in.EventsIndexed = stream.EventsIndexed
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
	in := firstRunInput{Monitor: s.monitorCtrl.Status(e.ID)}
	loadFirstRunIndex(r.Context(), e.DSN, &in)
	if s.baselineCtrl != nil && baselineTriggerPrecheck(e) == nil {
		b := s.baselineCtrl.Status(e.ID)
		in.Backup = &b
	}
	writeJSON(w, http.StatusOK, firstRunSteps(in))
}
