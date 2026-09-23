package console

import "context"

// This file defines the seam between the console (which renders and gates the
// monitoring surface) and the control-plane supervisor (which actually runs
// streams). The supervisor lives in cmd/bintrail and is wired in ONLY by
// `bintrail-console watch` — the write-capable daemon. The standalone
// read-only console never constructs one, so every monitor verb refuses
// there at the endpoint, mirroring how reconstruct gates on
// baselineConfigured.

// DoctorCheck is one preflight check result, JSON-shaped for the UI's
// remediation cards. Status is pass|fail|warn|skip.
type DoctorCheck struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Detail      string `json:"detail,omitempty"`
	Remediation string `json:"remediation,omitempty"`
	// Kind, Subjects and Statements are the doctor's typed finding (#1803):
	// a fixed word a screen switches on, the things it names (privileges,
	// tables, a setting), and for tables without a key one statement each.
	// See internal/doctor/kind.go for the set. Empty on a pass.
	Kind       string   `json:"kind,omitempty"`
	Subjects   []string `json:"subjects,omitempty"`
	Statements []string `json:"statements,omitempty"`
}

// DoctorReport aggregates the preflight checks for one source.
type DoctorReport struct {
	Checks   []DoctorCheck `json:"checks"`
	Passed   int           `json:"passed"`
	Failed   int           `json:"failed"`
	Warnings int           `json:"warnings"`
	Skipped  int           `json:"skipped"`
}

// MonitorStatus is the supervisor's view of one entry's stream.
type MonitorStatus struct {
	// State: "stopped" | "pending" | "running" | "stalled" | "lost_position"
	// | "failed".
	//   - "pending" covers launch through the stream's first checkpoint —
	//     connecting, snapshotting, discovering the start position. Only a
	//     saved checkpoint (or indexed batch) flips it to "running" (#407).
	//   - "stalled" and "lost_position" are running variants derived at read
	//     time: the stream goroutine is alive, but it has made no progress
	//     for several minutes (stalled) or an unfillable binlog gap forced
	//     it to skip permanently lost events (lost_position — durable,
	//     persisted in stream_state, re-hydrated on Start, cleared only by
	//     an explicit Stop).
	//   - "failed" carries LastError; the supervisor keeps retrying with
	//     backoff — "unhealthy, recovering", not terminal — unless the
	//     circuit breaker gave up after hours of continuous crash-looping
	//     (the message says so; Start re-arms it).
	State     string `json:"state"`
	LastError string `json:"last_error,omitempty"`
	// Since is when the underlying STORED state was entered (RFC3339), empty
	// for stopped. For the derived stalled/lost_position presentations it
	// still reflects the running transition, not when the stream stalled or
	// lost its position.
	Since string `json:"since,omitempty"`
	// SourceConnected: the latest run's stream opened the source connection
	// (#1606). It resets when a run starts, and keeps that run's value while
	// the run waits to retry or after it stops.
	SourceConnected bool `json:"source_connected,omitempty"`
	// Retrying: a failed state the supervisor will retry on its own after a
	// backoff. False for a failure it gave up on and for a Start that failed
	// while setting up, which both wait for Start (#1606).
	Retrying bool `json:"retrying,omitempty"`
	// Phase names a long startup step the stream is inside right now, so
	// "pending" can say WHICH part of starting up it is stuck on (#1690).
	// Currently only "resume_cleanup": the pre-capture delete of events a
	// replayed window would re-index, minutes of work on a large index.
	// Empty whenever no such step is running — including between retries and
	// for a stream that never reached one.
	Phase string `json:"phase,omitempty"`
}

// MonitorController is the control-plane supervisor as the console sees it.
// All methods must be safe for concurrent use. Errors returned to handlers
// are written into HTTP responses — implementations must pre-scrub DSN
// secrets out of them.
type MonitorController interface {
	// DeriveIndexDSN returns the index DSN a monitored entry should use — a
	// dedicated per-source database on the daemon's index MySQL server. It
	// does not create anything; Start does.
	DeriveIndexDSN(entryID string) (string, error)
	// Doctor runs the preflight checks against the entry's source (and its
	// index DSN, which may not exist yet — that is a pass, init creates it).
	Doctor(ctx context.Context, e ServerEntry) (*DoctorReport, error)
	// DoctorUnsaved runs the source half of those checks for a server that is
	// not saved yet (#1767), the web interface's Test connection on a new
	// server. It writes nothing anywhere: the index checks are left out (the
	// write-access one creates a probe database), and the first schema
	// snapshot counts as pending, since a new server's index does not exist.
	DoctorUnsaved(ctx context.Context, e ServerEntry) (*DoctorReport, error)
	// Start provisions the entry's index database (CREATE DATABASE + tables +
	// schema migration — the supervisor is a WRITER, the same role the cmd
	// layer plays for the boot DSN, so the console's never-migrates invariant
	// holds) and launches the supervised stream. Idempotent for an already
	// running entry. ctx bounds only the synchronous provisioning; the stream
	// itself lives on the daemon's lifecycle.
	Start(ctx context.Context, e ServerEntry) error
	// Stop cancels the entry's stream and releases its advisory lock.
	// Idempotent for an already stopped entry.
	Stop(ctx context.Context, entryID string) error
	// Status reports the entry's current monitor state.
	Status(entryID string) MonitorStatus
}

// NewEntryDiscarder is implemented by a supervisor that can take back what a
// failed first Start provisioned for an entry created in the same request:
// its job slot and the per-server index database Start may already have
// created. Optional, and checked with a type assertion, so MonitorController
// does not grow a method every implementation must carry; only the Connect
// check's rollback calls it (#1803).
type NewEntryDiscarder interface {
	DiscardNew(ctx context.Context, e ServerEntry) error
}
