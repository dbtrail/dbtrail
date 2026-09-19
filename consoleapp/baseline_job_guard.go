package consoleapp

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/dbtrail/dbtrail/internal/console"
)

// baselineJobKind names one of the four jobs baselineSupervisor runs in a
// goroutine, and with it the status slot that job publishes to. The string
// value is the log prefix each of those jobs already uses.
type baselineJobKind string

const (
	baselineJobDump    baselineJobKind = "baseline"
	baselineJobRefresh baselineJobKind = "baseline refresh"
	baselineJobRestore baselineJobKind = "baseline restore"
	baselineJobExport  baselineJobKind = "sql export"
	baselineJobCompact baselineJobKind = "baseline compact"
)

// statusSlotLocked returns the status map a job kind publishes to. Callers
// must hold s.mu.
//
// The default arm is not defensive filler. A fifth job kind added without a
// case here would compile (the kind is a string), and its panicking runs would
// then keep their slot at "running" forever, which wedges the single-flight
// ALL FOUR existing kinds share for that server. Nothing else would notice, so
// the arm says it out loud. Never panic here: this runs inside a deferred
// recover, where a second panic is uncatchable and would kill the daemon that
// the guard exists to keep alive.
func (s *baselineSupervisor) statusSlotLocked(kind baselineJobKind) map[string]*console.BaselineStatus {
	switch kind {
	case baselineJobDump:
		return s.jobs
	case baselineJobRefresh:
		return s.refreshes
	case baselineJobRestore:
		return s.restores
	case baselineJobExport:
		return s.exports
	case baselineJobCompact:
		return s.compacts
	default:
		slog.Error("baseline supervisor: job kind has no status slot, so its failure cannot be recorded "+
			"and this server's backup jobs will stay blocked until the daemon restarts. This is a bug: "+
			"add the kind to statusSlotLocked.", "kind", string(kind))
		return nil
	}
}

// recoverBaselineJob is the panic guard every baselineSupervisor job goroutine
// defers as its first statement. Deferred as a method value
// (`defer s.recoverBaselineJob(...)`), so the recover() below is called
// directly by the deferred function and does catch the panic.
//
// Under `bintrail-console watch` this process is ALSO the capture plane, so an
// unrecovered panic in a background baseline job stops replication capture. A
// baseline that stopped refreshing is a degradation, a daemon that stopped
// capturing is an outage, and the first must never cause the second
// (docs/dump-and-baseline.md states that as a guarantee). Mirrors
// verifySupervisor's guard, which is the same hazard on the neighbouring
// supervisor.
//
// SCOPE, because recover() is per-goroutine and this one is easy to over-read:
// it covers the JOB goroutine only. The fold itself fans out one goroutine per
// table, and a panic in one of those is invisible here however well this is
// written. That half is guarded at its own fan-out by
// reconstruct.recoverTableFold, and it is the bigger surface: it decodes a
// customer's Parquet through DuckDB. What THIS guard covers is the job's own
// frames around that fan-out: the snapshot lookup, the staging and build-
// directory setup, mydumper's privilege preflight, the history append, and the
// status tail.
//
// Swallowing the panic quietly would trade a loud outage for a silent
// degradation, which is worse. So two things happen here, and BOTH are
// load-bearing:
//
//   - The panic is logged at error level WITH the stack trace, which is the
//     only place the panic site is ever recorded now that the process no
//     longer dies printing it.
//   - The job's own status slot is moved to "failed", the same terminal state
//     its ordinary error path writes. The four slots share ONE per-server
//     single-flight (busyLocked reads State == "running" across all of them),
//     so a guard that logged and left the slot "running" would permanently
//     refuse this server's refresh, dump, restore AND sql export. That cure
//     would be worse than the disease.
//
// It rewrites the slot ONLY while it still reads "running". Every one of the
// four jobs publishes its success inside the same locked region it then logs
// from, so a panic in that tail (a nil deref in a log argument, say) would
// otherwise have this overwrite a genuinely succeeded run with "failed" and
// zero its table counts while the snapshot it published sits on disk. That is
// a false report about durable data. Wedge-safety is untouched by the
// condition: a terminal state already frees the single-flight.
//
// It deliberately does NOT append a run to the history (the Backups page's
// durations ledger). recordRun runs BEFORE the status write in the dump,
// refresh and restore jobs, so the guard cannot tell whether the panicking run
// already has a row, and recording unconditionally would double-count it;
// beyond that, history.Append is itself inside the guarded region, so
// re-entering it from here could panic a second time, which the guard could
// not catch. The consequence to know: a panicked run leaves no row on the
// Backups page's run list. The server's status card shows failed with the
// panic value, and the daemon log carries the stack.
func (s *baselineSupervisor) recoverBaselineJob(kind baselineJobKind, serverID, serverName string) {
	s.failPanickedJob(kind, serverID, serverName, recover())
}

// failPanickedJob is recoverBaselineJob after the recover() call, for a guard
// that has to be a closure (the dump's, which reads a value set during the
// run): recover() returns nil unless the DEFERRED function itself calls it,
// so a closure passes recover()'s result in. r == nil is "no panic".
func (s *baselineSupervisor) failPanickedJob(kind baselineJobKind, serverID, serverName string, r any) {
	if r == nil {
		return
	}
	// debug.Stack() here still walks the panicking frames: the deferred
	// function runs on top of them, so the stack names where the panic came
	// from and not just this guard.
	// Logged BEFORE the status write, and unconditionally, so that no path
	// below can recover a panic and leave no trace of it. It claims only what
	// is already true at this point: the process is alive. Whether the job's
	// slot could be freed is decided below, so the line does not promise it.
	slog.Error(string(kind)+": the job hit an internal error and stopped. Capture and the console keep "+
		"running. Please report this with the stack recorded here.",
		"server", serverName, "id", serverID, "panic", r, "stack", string(debug.Stack()))

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.statusSlotLocked(kind)[serverID]
	if st == nil || st.State != "running" {
		return
	}
	st.State = "failed"
	st.LastError = fmt.Sprintf("internal error: %v", r)
	st.FinishedAt = nowStamp()
	// Nothing was published, so report nothing: a partial count reads as
	// progress to the status API ({state:"failed", rows:12000} looks
	// half-done). Same reasoning the sql export's ordinary failure path
	// spells out. Since and At are kept — they identify the run.
	st.Tables, st.Refused, st.Carried, st.CarriedCopied, st.Uploaded, st.Swept = 0, 0, 0, 0, 0, 0
	st.Rows, st.Bytes = 0, 0
	st.Uploading = false
}
