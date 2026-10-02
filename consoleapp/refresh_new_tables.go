package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/event"
)

// Tables created after the previous snapshot (#1993).
//
// An update folds the recorded changes onto the tables of the snapshot it
// starts from. A table created on the source after that snapshot has no file
// to fold onto, so the update publishes without it, and before #1993 nothing
// said so: with full reads turned off by the schedule, the table never joined
// the copy at all. Each update therefore asks the source which tables its
// scope holds now and names the ones the snapshot lacks. What happens next is
// the scheduler's (newTablesPlanner decides and the run records it,
// includeNewTables starts the full read): a full read when one may start here,
// otherwise the gap stays reported on the run with the reason.

// newTablesCheck is what one update learned about tables it left out.
// unchecked non-empty means the source was not asked or did not answer, and
// then tables is empty because it is unknown, never because there were none.
type newTablesCheck struct {
	all       []string // every table left out, sorted: what the schedule compares
	tables    []string // all, capped at console.RefusedTablesCap for display
	omitted   int
	unchecked string
	// action and reason: what the daemon decided about them
	// (console.NewTablesAction*), set once the snapshot is published.
	action, reason string
}

// sourceTablesTimeout bounds the one information_schema read an update makes on
// the source (and the schema_changes read the quiet-server gate makes on the
// index). The update runs inside the capture process; a database that does not
// answer must not hold it.
const sourceTablesTimeout = 15 * time.Second

// listSourceTables answers "which tables does this server's snapshot scope
// hold on the source now", as schema.table, with the selection a full read
// asks mydumper for (dumpableTablesWhere: base tables only, so views are not
// tables here either, and the system schemas are left out when no schema list
// is set). foldCase reports lower_case_table_names != 0, under which the
// server compares table names without case. Indirected so unit tests do not
// need a source.
var listSourceTables = func(ctx context.Context, sourceDSN string, ssl config.SSL, schemas []string) (tables []string, foldCase bool, err error) {
	db, err := connectSource(sourceDSN, ssl)
	if err != nil {
		return nil, false, err
	}
	defer db.Close()
	var lctn sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&lctn); err != nil {
		return nil, false, err
	}
	where, args := dumpableTablesWhere(schemas)
	rows, err := db.QueryContext(ctx, "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES WHERE "+where, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, false, err
		}
		tables = append(tables, schema+"."+table)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return tables, lctn.Valid && lctn.Int64 != 0, nil
}

// tablesLeftOut returns the entries of source that snapshot does not hold,
// sorted and without repeats. foldCase compares without case (the server's
// lower_case_table_names is not 0); otherwise "shop.Orders" and "shop.orders"
// are two tables, as they are on such a server.
func tablesLeftOut(snapshot, source []string, foldCase bool) []string {
	key := func(s string) string {
		if foldCase {
			return strings.ToLower(s)
		}
		return s
	}
	have := make(map[string]bool, len(snapshot))
	for _, t := range snapshot {
		have[key(t)] = true
	}
	seen := map[string]bool{}
	var out []string
	for _, t := range source {
		k := key(t)
		if have[k] || seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// checkNewTables compares the source's tables with the snapshot an update is
// about to fold. It never fails the update: a source that cannot be asked is
// reported as unchecked, and the update goes on with the tables it has.
func (s *baselineSupervisor) checkNewTables(req refreshRequest, snapshot []string) newTablesCheck {
	if req.SourceDSN == "" {
		// No source is known for this request (the command-line server of
		// the daemon-wide loop, a registry server with no source set). Said,
		// not skipped: an empty list here would read as "nothing missing".
		return newTablesCheck{unchecked: "no source connection is set for this server, so tables created on it after the previous snapshot were not looked for"}
	}
	if req.SourcePostgres {
		return newTablesCheck{unchecked: "tables created after the previous snapshot are not looked for on PostgreSQL sources yet"}
	}
	ctx, cancel := context.WithTimeout(s.ctx, sourceTablesTimeout)
	defer cancel()
	source, foldCase, err := listSourceTables(ctx, req.SourceDSN, req.SourceSSL, req.Schemas)
	if err != nil {
		return newTablesCheck{unchecked: console.ScrubReason(
			fmt.Sprintf("could not ask the source which tables it has: %v", err), req.SourceDSN)}
	}
	all := withoutKnownLeftOut(tablesLeftOut(snapshot, source, foldCase), snapshot, s.knownLeftOut(req.ServerID), foldCase)
	kept, omitted := console.NewTablesOf(all)
	return newTablesCheck{all: all, tables: kept, omitted: omitted}
}

// planNewTables asks the request's planner what happens to the tables the
// update left out. Called only for an update whose snapshot reached its
// destination; nothing to decide when nothing was left out.
func planNewTables(req refreshRequest, all []string) (action, reason string) {
	if len(all) == 0 {
		return "", ""
	}
	if req.PlanNewTables == nil {
		return console.NewTablesActionNoSchedule, ""
	}
	return req.PlanNewTables(all)
}

// applyNewTables writes one update's check onto its live status.
//
// A published update replaces the list, every field, an empty list included:
// its snapshot is the newest one, and the list describes exactly it. An update
// that published nothing leaves the list as it was: the newest copy is still
// the one the list describes, and clearing it would let a run of failing
// updates hide a table the copy lacks.
func applyNewTables(st *console.BaselineStatus, c newTablesCheck, published bool, at time.Time) {
	if !published {
		return
	}
	st.NewTables, st.NewTablesOmitted, st.NewTablesUnchecked = c.tables, c.omitted, c.unchecked
	st.NewTablesAction, st.NewTablesActionReason = c.action, c.reason
	st.NewTablesSnapshot = ""
	if len(c.tables) > 0 || c.omitted > 0 || c.unchecked != "" {
		st.NewTablesSnapshot = at.UTC().Format(time.RFC3339)
	}
}

// carryNewTables copies the list a previous update left on its status onto the
// status of the run that is starting: while it runs, and if it publishes
// nothing, the newest copy still lacks those tables.
func carryNewTables(next, prev *console.BaselineStatus) {
	if prev == nil {
		return
	}
	next.NewTables, next.NewTablesOmitted, next.NewTablesUnchecked = prev.NewTables, prev.NewTablesOmitted, prev.NewTablesUnchecked
	next.NewTablesAction, next.NewTablesActionReason = prev.NewTablesAction, prev.NewTablesActionReason
	next.NewTablesSnapshot = prev.NewTablesSnapshot
}

// tablesCreatedSince asks the index whether capture recorded a CREATE TABLE
// after since (schema_changes, #700). Indirected for tests.
var tablesCreatedSince = func(ctx context.Context, indexDSN string, since time.Time) (bool, error) {
	db, err := config.Connect(indexDSN)
	if err != nil {
		return false, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, sourceTablesTimeout)
	defer cancel()
	var one int
	switch err := db.QueryRowContext(ctx,
		"SELECT 1 FROM schema_changes WHERE ddl_type IN (?, ?) AND detected_at >= ? LIMIT 1",
		event.DDLCreateTable, event.DDLReplaceTable, since.UTC().Add(-time.Minute)).Scan(&one); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	}
	return true, nil
}

// withSource stamps e's source and snapshot scope onto an update request, so
// the update can ask the source for the tables it would leave out. Scope as a
// full read of e resolves it (console.BaselineRequestFor), so the two cannot
// disagree about which schemas count.
func withSource(req *refreshRequest, e console.ServerEntry) {
	br := console.BaselineRequestFor(e)
	req.SourceDSN, req.SourceSSL, req.Schemas, req.SourcePostgres = br.SourceDSN, br.SourceSSL, br.Schemas, e.IsPostgres()
}

// reportNewTables puts the check in the daemon log, where the published line
// alone used to read as the whole database.
func (s *baselineSupervisor) reportNewTables(req refreshRequest, c newTablesCheck) {
	switch {
	case c.unchecked != "" && req.SourceDSN == "":
		// The same at every cycle of a server with no source to ask: said
		// once at Info, then at Debug. The run's status says it each time.
		args := []any{"server", req.ServerName, "id", req.ServerID}
		if s.gateEdge != nil && s.gateEdge.Fire("nosource:"+req.ServerID, "1") {
			slog.Info("snapshot refresh: this server has no source connection, so tables created after the previous snapshot are not looked for", args...)
		} else {
			slog.Debug("snapshot refresh: no source to check for tables created after the previous snapshot", args...)
		}
	case c.unchecked != "":
		slog.Warn("snapshot refresh: could not check for tables created after the previous snapshot; this snapshot may be missing some",
			"server", req.ServerName, "id", req.ServerID, "reason", c.unchecked)
	case len(c.all) > 0:
		slog.Warn("snapshot refresh: tables created after the previous snapshot are not in this snapshot",
			"server", req.ServerName, "id", req.ServerID, "count", len(c.all),
			"tables", strings.Join(c.tables, ","), "not_listed", c.omitted, "next", c.action, "reason", c.reason)
	}
}

// newTablesTry is a full read the schedule started (or promised) for tables an
// update left out: which tables, the loop's stamp for that start, and how many
// were started for them so far.
type newTablesTry struct {
	names map[string]bool
	at    string
	// attempts counts the full reads started for these tables. A full read
	// that keeps failing (privileges, a refused lock) is not retried at
	// every update against production: newTablesMaxAttempts bounds it.
	attempts int
}

// newTablesMaxAttempts is how many full reads the schedule starts for the
// same new tables before it stops and leaves the gap reported.
const newTablesMaxAttempts = 3

// newTablesPlanner is the schedule's PlanNewTables for e. It decides, and
// records for the watcher, without starting anything: the update records the
// decision with its run, so the page says what is coming, and the watcher
// starts the full read once the update has finished.
//
// There is no per-table read (#1649 was not built), so the way to include new
// tables is a full read, under the same gate as every other full read the
// schedule takes (FullBackupPossible). No loop: a full read that went through
// and still lacks a table (a name the dump spells differently, a scope the two
// reads disagree on) would otherwise start another at every slot.
func (b *backupScheduler) newTablesPlanner(e console.ServerEntry) func(all []string) (string, string) {
	return func(all []string) (string, string) {
		attempts, verdict := b.newTablesOwed(e.ID, all)
		if verdict != "" {
			return verdict, ""
		}
		cur, ok := b.reg.Get(e.ID)
		if !ok {
			cur = e
		}
		if err := console.FullBackupPossible(cur, b.gates()); err != nil {
			return console.NewTablesActionNotPossible, err.Error()
		}
		// The same daily cap as every other full read the schedule takes on
		// its own (#2006), on top of newTablesMaxAttempts: whichever holds
		// first.
		if next, held := b.emergencyHeld(e.ID, time.Now().UTC()); held {
			return console.NewTablesActionNotPossible, emergencyHeldWords(next)
		}
		names := make(map[string]bool, len(all))
		for _, n := range all {
			names[n] = true
		}
		b.mu.Lock()
		b.newTablesPending[e.ID] = newTablesTry{names: names, attempts: attempts}
		b.mu.Unlock()
		return console.NewTablesActionFullRead, ""
	}
}

// includeNewTables starts the full read the planner promised for an update
// that finished (#1993). Started as its own job right after the update, not
// instead of it: the update has already published every table it holds, so a
// full read that then fails leaves the copy as current as the schedule could
// make it. Every other decision was made and recorded by the planner.
func (b *backupScheduler) includeNewTables(e console.ServerEntry, done console.BaselineStatus) {
	b.mu.Lock()
	pending, promised := b.newTablesPending[e.ID]
	delete(b.newTablesPending, e.ID)
	if len(done.NewTables) == 0 && done.NewTablesOmitted == 0 && done.NewTablesUnchecked == "" {
		// The newest snapshot holds every table in scope: nothing is owed.
		delete(b.newTablesTried, e.ID)
	}
	b.mu.Unlock()
	if done.NewTablesAction != console.NewTablesActionFullRead || !promised {
		return
	}
	cur, ok := b.reg.Get(e.ID)
	if !ok || cur.BackupSchedule == nil || e.BackupSchedule == nil || cur.BackupSchedule.Identity() != e.BackupSchedule.Identity() {
		slog.Info("snapshot schedule: the update left out tables created after the previous snapshot, but the schedule was removed or changed meanwhile; no full read taken",
			"server", e.Name, "id", e.ID, "tables", strings.Join(done.NewTables, ","))
		b.sup.withdrawNewTablesPromise(e.ID)
		return
	}
	e = cur
	b.mu.Lock()
	_, observed := b.seen[e.ID]
	b.mu.Unlock()
	if !observed {
		slog.Info("snapshot schedule: the update left out tables created after the previous snapshot, but the schedule was removed meanwhile; no full read taken",
			"server", e.Name, "id", e.ID)
		b.sup.withdrawNewTablesPromise(e.ID)
		return
	}
	now := time.Now().UTC()
	stamp := now.Format(time.RFC3339)
	n := len(pending.names)
	slog.Info("snapshot schedule: the update left out tables created after the previous snapshot; taking a full read to include them",
		"server", e.Name, "id", e.ID, "count", n, "tables", strings.Join(done.NewTables, ","), "attempt", pending.attempts+1)
	if b.startFull(e, stamp, now, "", console.NewTablesWhy(n)) {
		b.noteEmergency(e.ID, now)
		b.mu.Lock()
		b.newTablesTried[e.ID] = newTablesTry{names: pending.names, at: stamp, attempts: pending.attempts + 1}
		b.mu.Unlock()
		b.watch(e, stamp, console.BackupMethodFull)
		return
	}
	// startFull recorded why it did not start (another job held the server,
	// or a refusal). The update's status must not keep saying one started.
	b.sup.withdrawNewTablesPromise(e.ID)
}

// withdrawNewTablesPromise takes back a "full read started" decision from the
// live status when the full read did not start: the page then says the tables
// join at the next full snapshot, without claiming one is running. The next
// update decides again.
func (s *baselineSupervisor) withdrawNewTablesPromise(serverID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st := s.refreshes[serverID]; st != nil && st.NewTablesAction == console.NewTablesActionFullRead {
		st.NewTablesAction = ""
	}
}

// newTablesOwed answers, for every table an update left out, whether a full
// read is still owed for them, and how many were already started. verdict ""
// means owed: one of them was never tried, or the full reads started for them
// failed fewer than newTablesMaxAttempts times. NewTablesActionStillMissing:
// a full read recorded at or after the last one started for them went through
// (it read the source and still lacks them: another would too).
// NewTablesActionGaveUp: newTablesMaxAttempts were started and none went
// through, or there is no run history to tell (a loop of full reads against
// production is the worse error; the gap stays reported either way).
//
// A skipped slot is a dump-kind record with no error (AppendSkip): it is not
// a full read that went through, the same rule every history reader applies.
func (b *backupScheduler) newTablesOwed(serverID string, all []string) (attempts int, verdict string) {
	b.mu.Lock()
	tried, ok := b.newTablesTried[serverID]
	b.mu.Unlock()
	if !ok {
		return 0, ""
	}
	for _, n := range all {
		if !tried.names[n] {
			return 0, ""
		}
	}
	if b.sup.history == nil {
		return tried.attempts, console.NewTablesActionGaveUp
	}
	for _, r := range b.sup.history.List(serverID) {
		if r.Kind == console.BaselineRunDump && r.Error == "" && r.SkipReason == "" && r.StartedAt >= tried.at {
			return tried.attempts, console.NewTablesActionStillMissing
		}
	}
	if tried.attempts >= newTablesMaxAttempts {
		return tried.attempts, console.NewTablesActionGaveUp
	}
	return tried.attempts, ""
}

// knownLeftOut is every table the newest full read that went through left
// out on purpose, because its real name cannot be read back or stored (#2006),
// as the source names it: "schema.table", or ".table" when the dump did not
// say the schema's real name. Read from the run's uncapped key list.
func (s *baselineSupervisor) knownLeftOut(serverID string) []string {
	if s.history == nil {
		return nil
	}
	if rec := s.history.LastFullRead(serverID); rec != nil {
		return rec.LeftOutKeys
	}
	return nil
}

// withoutKnownLeftOut drops from the new tables the ones the last full read
// left out on purpose: counting them as new would start a full read for them
// at every update, and each would leave them out again. A ".table" key (the
// schema's real name unknown: mydumper renamed the schema and wrote no CREATE
// DATABASE) matches that table in any schema the snapshot holds no table of,
// which is what such a schema looks like from here.
func withoutKnownLeftOut(newTables, snapshot, known []string, foldCase bool) []string {
	if len(known) == 0 {
		return newTables
	}
	key := func(s string) string {
		if foldCase {
			return strings.ToLower(s)
		}
		return s
	}
	exact, anySchema := map[string]bool{}, map[string]bool{}
	for _, k := range known {
		if t, ok := strings.CutPrefix(k, "."); ok {
			anySchema[key(t)] = true
		} else {
			exact[key(k)] = true
		}
	}
	held := map[string]bool{}
	for _, t := range snapshot {
		if sch, _, ok := strings.Cut(t, "."); ok {
			held[key(sch)] = true
		}
	}
	out := newTables[:0:0]
	for _, t := range newTables {
		sch, tbl, _ := strings.Cut(t, ".")
		if exact[key(t)] || (anySchema[key(tbl)] && !held[key(sch)]) {
			continue
		}
		out = append(out, t)
	}
	return out
}
