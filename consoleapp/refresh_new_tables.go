package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
)

// Tables created after the previous snapshot (#1993).
//
// An update folds the recorded changes onto the tables of the snapshot it
// starts from. A table created on the source after that snapshot has no file
// to fold onto, so the update publishes without it, and before #1993 nothing
// said so: with full reads turned off by the schedule, the table never joined
// the copy at all. Each update therefore asks the source which tables its
// scope holds now and names the ones the snapshot lacks. What happens next is
// the scheduler's (includeNewTables): a full read when one may start here,
// otherwise the gap stays reported on the run.

// newTablesCheck is what one update learned about tables it left out.
// unchecked non-empty means the source was not asked or did not answer, and
// then tables is empty because it is unknown, never because there were none.
type newTablesCheck struct {
	tables    []string // capped at console.RefusedTablesCap, sorted
	omitted   int
	unchecked string
}

// sourceTablesTimeout bounds the one information_schema read an update makes on
// the source. The update runs inside the capture process; a source that does
// not answer must not hold the fold.
const sourceTablesTimeout = 15 * time.Second

// listSourceTables answers "which tables does this server's snapshot scope
// hold on the source now", as schema.table, with the selection a full read
// asks mydumper for (dumpableTablesWhere: base tables only, so views are not
// tables here either, and the system schemas are left out when no schema list
// is set). foldCase reports lower_case_table_names != 0, under which the
// server compares table names without case. Indirected so unit tests do not
// need a source.
var listSourceTables = func(ctx context.Context, sourceDSN string, schemas []string) (tables []string, foldCase bool, err error) {
	db, err := config.Connect(sourceDSN)
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
	source, foldCase, err := listSourceTables(ctx, req.SourceDSN, req.Schemas)
	if err != nil {
		return newTablesCheck{unchecked: console.ScrubReason(
			fmt.Sprintf("could not ask the source which tables it has: %v", err), req.SourceDSN)}
	}
	kept, omitted := console.NewTablesOf(tablesLeftOut(snapshot, source, foldCase))
	return newTablesCheck{tables: kept, omitted: omitted}
}

// applyNewTables writes one update's check onto its status. Set on every run,
// published or not, like applyFoldStatus's fields: the slot is reused, and a
// list left from the run before would name tables this run never left out. A
// run that published nothing carries no list: the full read the schedule takes
// in its place reads every table.
func applyNewTables(st *console.BaselineStatus, c newTablesCheck, published bool) {
	if !published {
		c = newTablesCheck{}
	}
	st.NewTables, st.NewTablesOmitted, st.NewTablesUnchecked = c.tables, c.omitted, c.unchecked
}

// withSource stamps e's source and snapshot scope onto an update request, so
// the update can ask the source for the tables it would leave out. Scope as a
// full read of e resolves it (console.BaselineRequestFor), so the two cannot
// disagree about which schemas count.
func withSource(req *refreshRequest, e console.ServerEntry) {
	br := console.BaselineRequestFor(e)
	req.SourceDSN, req.Schemas, req.SourcePostgres = br.SourceDSN, br.Schemas, e.IsPostgres()
}

// reportNewTables puts the check in the daemon log, where the published line
// alone used to read as the whole database.
func reportNewTables(req refreshRequest, c newTablesCheck) {
	switch {
	case c.unchecked != "" && req.SourceDSN == "":
		// The same at every cycle of a server that has no source to ask;
		// the run's status says it, the log does not repeat it as a warning.
		slog.Debug("snapshot refresh: no source to check for tables created after the previous snapshot",
			"server", req.ServerName, "id", req.ServerID)
	case c.unchecked != "":
		slog.Warn("snapshot refresh: could not check for tables created after the previous snapshot; this snapshot may be missing some",
			"server", req.ServerName, "id", req.ServerID, "reason", c.unchecked)
	case len(c.tables) > 0 || c.omitted > 0:
		slog.Warn("snapshot refresh: tables created after the previous snapshot are not in this snapshot",
			"server", req.ServerName, "id", req.ServerID, "count", len(c.tables)+c.omitted,
			"tables", strings.Join(c.tables, ","), "not_listed", c.omitted)
	}
}

// newTablesTry is one full read the schedule started for tables an update left
// out: which tables, and the loop's stamp for that start.
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

// includeNewTables answers an update that PUBLISHED without tables created on
// the source after its starting snapshot (#1993). There is no per-table read
// (#1649 was not built), so the way to include them is a full read, under the
// same gate as every other full read the schedule takes (FullBackupPossible).
// Where that gate is closed the update stays published, and the gap stays
// reported on the run until a full snapshot holds the tables.
//
// Started as its own job right after the update, not instead of it: the update
// has already published every table it holds, so a full read that then fails
// or is refused leaves the copy as current as the schedule could make it.
//
// No loop: a full read that went through and still lacks a table (a name the
// dump spells differently, a scope the two reads disagree on) would otherwise
// start another at every slot. Only a table no such full read has been tried
// for starts one; a full read that FAILED does not count as tried.
func (b *backupScheduler) includeNewTables(e console.ServerEntry, done console.BaselineStatus) {
	if len(done.NewTables) == 0 && done.NewTablesOmitted == 0 {
		if done.NewTablesUnchecked == "" {
			b.mu.Lock()
			delete(b.newTablesTried, e.ID)
			b.mu.Unlock()
		}
		return
	}
	cur, ok := b.reg.Get(e.ID)
	if !ok || cur.BackupSchedule == nil || e.BackupSchedule == nil || cur.BackupSchedule.Identity() != e.BackupSchedule.Identity() {
		slog.Info("snapshot schedule: the update left out tables created after the previous snapshot, but the schedule was removed or changed meanwhile; no full read taken",
			"server", e.Name, "id", e.ID, "tables", strings.Join(done.NewTables, ","))
		return
	}
	e = cur
	attempts, owed := b.newTablesOwed(e.ID, done.NewTables)
	if !owed {
		slog.Warn("snapshot schedule: tables created after the previous snapshot are still not in it after a full read was taken for them "+
			"(it went through, or failed too many times); not starting another, the gap stays reported",
			"server", e.Name, "id", e.ID, "tables", strings.Join(done.NewTables, ","), "not_listed", done.NewTablesOmitted, "full_reads_started", attempts)
		return
	}
	if err := console.FullBackupPossible(e, b.gates()); err != nil {
		slog.Warn("snapshot schedule: the update left out tables created after the previous snapshot, and a full read cannot start here; they join the copy at the next full snapshot",
			"server", e.Name, "id", e.ID, "tables", strings.Join(done.NewTables, ","), "not_listed", done.NewTablesOmitted, "reason", err)
		return
	}
	b.mu.Lock()
	_, observed := b.seen[e.ID]
	b.mu.Unlock()
	if !observed {
		slog.Info("snapshot schedule: the update left out tables created after the previous snapshot, but the schedule was removed meanwhile; no full read taken",
			"server", e.Name, "id", e.ID)
		return
	}
	now := time.Now().UTC()
	stamp := now.Format(time.RFC3339)
	why := console.NewTablesWhy(len(done.NewTables) + done.NewTablesOmitted)
	slog.Info("snapshot schedule: the update left out tables created after the previous snapshot; taking a full read to include them",
		"server", e.Name, "id", e.ID, "tables", strings.Join(done.NewTables, ","), "not_listed", done.NewTablesOmitted)
	if b.startFull(e, stamp, now, "", why) {
		names := make(map[string]bool, len(done.NewTables))
		for _, n := range done.NewTables {
			names[n] = true
		}
		b.mu.Lock()
		b.newTablesTried[e.ID] = newTablesTry{names: names, at: stamp, attempts: attempts + 1}
		b.mu.Unlock()
		b.watch(e, stamp, console.BackupMethodFull)
	}
}

// newTablesOwed reports whether a full read should still be started for
// names, and how many were already started for them. Yes when one of them was
// never tried. Otherwise no when a full read recorded at or after the last
// one started for them went through (it read the source and still lacks them:
// another would too), or when newTablesMaxAttempts were started and failed;
// yes while fewer failed. Without a run history a failed full read cannot be
// told from one that missed the tables, and the answer is no: a loop of full
// reads against production is the worse error, and the gap stays reported
// either way.
func (b *backupScheduler) newTablesOwed(serverID string, names []string) (attempts int, owed bool) {
	b.mu.Lock()
	tried, ok := b.newTablesTried[serverID]
	b.mu.Unlock()
	if !ok {
		return 0, true
	}
	fresh := false
	for _, n := range names {
		if !tried.names[n] {
			fresh = true
		}
	}
	if fresh {
		return 0, true
	}
	if b.sup.history == nil || tried.attempts >= newTablesMaxAttempts {
		return tried.attempts, false
	}
	for _, r := range b.sup.history.List(serverID) {
		if r.Kind == console.BaselineRunDump && r.Error == "" && r.StartedAt >= tried.at {
			return tried.attempts, false
		}
	}
	return tried.attempts, true
}
