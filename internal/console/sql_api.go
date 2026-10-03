package console

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// POST /api/sql (#1952, slice 2): run one read-only SQL statement on the
// selected server's Parquet copy, in a separate child process with caps
// (internal/sqlsandbox), over the same views the console generates for
// /api/views.sql. The route, its permission, its audit emission and its CSV
// form live here; the panel that calls it is a later slice.
//
// Three gates, in order, before anything runs:
//
//   - The route needs ext.PermSQLExecute (authz.go).
//   - A data-restricted session is refused even with the permission: the
//     profile withholds tables and redacts columns on the console's own
//     reads, and free SQL reads the raw files, which nothing here can filter
//     (condition 3 of #1952). Same rule as the views.sql download, audited
//     as profile.denied like it.
//   - The copy must be LOCAL: the worker admits only local directories and
//     never opens the network, so a copy that lives only on S3 is refused
//     with a message that says so.
//
// The views the worker installs are generated WITHOUT the live leg
// (buildViewsInput never sets LiveIndex unless asked): that leg ATTACHes the
// MySQL index, which the locked session refuses.
//
// Open-core note: the raw archive files carry connection_id, query_text and
// query_hash, which eventDTO omits on purpose. v1 exposes the views over the
// archive as-is: the archive tier is the operator's own files, the omission
// is about the free query_explorer surface, and the permission this route
// needs is its own. Projecting those columns away in the generated views
// would be a change to the views generator, not to this route.

// sqlRunner is what the handler needs from sqlsandbox.Runner; an interface
// so the handler's tests can run without a child process. The slot is taken
// FIRST (#2026): the views are built after it, so a statement refused as
// busy has read nothing from the index and nothing from the copy.
type sqlRunner interface {
	Reserve(user string) (sqlSlot, error)
}

// sqlSlot is one reserved worker slot: Run serves one job and releases it,
// Release gives an unused one back (a no-op after Run).
type sqlSlot interface {
	Run(ctx context.Context, job sqlsandbox.Job) (sqlsandbox.Result, error)
	Release()
}

// sandboxRunner adapts *sqlsandbox.Runner to sqlRunner (its Reserve returns
// the concrete *sqlsandbox.Slot).
type sandboxRunner struct{ r *sqlsandbox.Runner }

func (a sandboxRunner) Reserve(user string) (sqlSlot, error) {
	slot, err := a.r.Reserve(user)
	if err != nil {
		return nil, err
	}
	return slot, nil
}

// sqlRequest is the POST body.
type sqlRequest struct {
	// SQL is the statement: exactly one SELECT-shaped statement.
	SQL string `json:"sql"`
	// MaxRows lowers the row cap for this query; 0 means the server's cap.
	// It cannot raise it.
	MaxRows int `json:"max_rows"`
}

// sqlResponse is the JSON form of the result.
type sqlResponse struct {
	Columns   []sqlsandbox.Column `json:"columns"`
	Rows      [][]any             `json:"rows"`
	Truncated bool                `json:"truncated"`
	// TruncatedCells counts cells cut at sqlsandbox.MaxCellBytes.
	TruncatedCells int   `json:"truncated_cells"`
	ElapsedMS      int64 `json:"elapsed_ms"`
	// CopyUpdatedAt is the newest snapshot the state views were pinned to
	// ("runs on the copy updated N min ago"); null when the copy has no
	// snapshot and only the change log is queryable.
	CopyUpdatedAt *time.Time `json:"copy_updated_at"`
	// PhasesMS is where the time went (#2026): view_build (the daemon's
	// discovery and script generation), then the sandbox's phases.
	// Milliseconds with microsecond precision, so a sub-millisecond phase
	// reads as the small number it is, not as "not measured".
	PhasesMS map[string]float64 `json:"phases_ms"`
}

// sqlPhases names a statement's phases once, for the response, the log
// and the histogram, so the three cannot disagree on a key.
func sqlPhases(viewBuild time.Duration, p sqlsandbox.Phases) map[string]time.Duration {
	return map[string]time.Duration{
		"view_build": viewBuild, "spawn": p.Spawn, "open": p.Open, "lockdown": p.Lockdown,
		"views": p.Views, "query": p.Query, "decode": p.Decode, "total": p.Total,
	}
}

// sqlPhasesMS renders the phases for the response and the log.
func sqlPhasesMS(viewBuild time.Duration, p sqlsandbox.Phases) map[string]float64 {
	out := map[string]float64{}
	for k, d := range sqlPhases(viewBuild, p) {
		out[k] = float64(d.Microseconds()) / 1000
	}
	return out
}

// sqlRequestMaxBytes bounds the POST body: a statement is text a person
// typed, not a file.
const sqlRequestMaxBytes = 1 << 20

// resolveSQLLimits fills every zero field of cfg from the package defaults,
// so the handler can read a cap without knowing which side set it.
func resolveSQLLimits(cfg sqlsandbox.Limits) sqlsandbox.Limits {
	d := sqlsandbox.DefaultLimits()
	if cfg.Threads == 0 {
		cfg.Threads = d.Threads
	}
	if cfg.MemoryLimit == "" {
		cfg.MemoryLimit = d.MemoryLimit
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = d.Timeout
	}
	if cfg.MaxRows == 0 {
		cfg.MaxRows = d.MaxRows
	}
	if cfg.MaxResultBytes == 0 {
		cfg.MaxResultBytes = d.MaxResultBytes
	}
	return cfg
}

// wantsCSV reports whether the client asked for the CSV form, by query
// parameter or by Accept header.
func wantsCSV(r *http.Request) bool {
	if f := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format"))); f != "" {
		return f == "csv"
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/csv")
}

// sqlGate is the part of the route's gating both forms share (the statement
// POST and the metadata GET): the data-profile refusal, audited, and the
// archive switch. It writes the refusal and returns false.
func (s *Server) sqlGate(w http.ResponseWriter, r *http.Request, b *bundle) bool {
	if s.profileActiveFor(r) {
		recordProfileGateDeny(r, "sql")
		writeJSONError(w, http.StatusForbidden,
			"SQL on the copy is unavailable while a data profile is active: the profile withholds "+
				"tables and redacts columns on the console's own reads, and free SQL reads the raw "+
				"Parquet files, which it cannot filter")
		return false
	}
	if b.noArchive {
		writeJSONError(w, http.StatusConflict,
			"archive access is disabled for this server, so its copy cannot be read")
		return false
	}
	return true
}

// sqlAvailable is the `sql` capability: whether POST /api/sql can work for
// this session on this server. The session holds sql:execute, no data
// profile is active, archive access is on, and there is something LOCAL to
// query: a complete snapshot in a local snapshot directory, or failing that
// a change log that lives on local disk only.
//
// It runs on every page load, so it is cheap on purpose: a directory probe
// that stops at the first table file (no footer read, no S3 listing), and
// the archive lookup that reads archive_state without walking the archive
// tree. That makes it CONSERVATIVE in two corners, where the card stays
// hidden although the route would answer a statement over events:
//
//   - the snapshot root is s3:// and the change log is local (the route has
//     to list S3 before it can say anything, which a capability must not);
//   - there is no local snapshot, and the change log is registered in S3
//     as well as still present on local disk (telling "still present"
//     takes the tree walk this probe avoids).
//
// It is never optimistic: where it says true, GET /api/sql answers 200
// (TestCapabilities_sqlAgreesWithTheRoute).
func (s *Server) sqlAvailable(r *http.Request, b *bundle) bool {
	if b == nil || b.noArchive || s.sqlRunner == nil || s.profileActiveFor(r) ||
		!policyFrom(r.Context()).Allows(ext.PermSQLExecute) {
		return false
	}
	if strings.HasPrefix(b.baselineSrc, "s3://") {
		return false
	}
	if b.baselineSrc != "" && reconstruct.HasLocalSnapshot(b.baselineSrc) {
		return true
	}
	// The portable lookup: no tree walk, no warning per call. A source it
	// names in S3 is not (known to be) local.
	sources, err := consoleArchiveSources(r.Context(), b.db, true)
	if err != nil || len(sources) == 0 {
		return false
	}
	return sqlArchivesLocal(sources)
}

// sqlArchivesLocal reports whether the change log can be read by the
// sandbox: there is at least one archive source and none of them is in S3.
func sqlArchivesLocal(sources []string) bool {
	if len(sources) == 0 {
		return false
	}
	for _, src := range sources {
		if strings.HasPrefix(src, "s3://") {
			return false
		}
	}
	return true
}

// sqlMentionsEvents reports whether the statement names the events view.
// The events view is the expensive half of the views to install: defining
// it opens Parquet footers across the archive (one per archived file where
// the column sets cannot be grouped), which on a real archive can cost
// more than the whole time budget, and it is why the first SQL page was
// removed. So the worker installs it only for a statement that names it.
// A word match, deliberately loose, because a false negative would be
// "events does not exist" for a statement that really reads it. A false
// positive (the word as an alias, inside a string, a column of that name)
// installs the view for nothing: on a local change log that reintroduces
// the footer cost and can spend the whole time budget; on a change log that
// lives in S3 it changes nothing, because the view is not installed there
// at all (sqlViewsFor).
//
// One exception to the loose match (#2013): since the table views are named
// after the source, a source table or schema called events is a name a person
// types every day (shop.events, events.orders), and installing the change log
// for each of those statements is the footer cost for nothing. So a match is
// skipped when it is clearly another name's part: followed by a dot (a schema
// called events), or preceded by one whose schema is not main (a table called
// events). main.events, memory.events and memory.main.events are still the
// view.
var sqlEventsWord = regexp.MustCompile(`(?i)\bevents\b`)

// sqlQualifierBefore matches a dotted name ending right before a match, and
// captures the part just before the last dot: `shop.`, `"shop" . `.
var sqlQualifierBefore = regexp.MustCompile(`(?i)("(?:[^"]|"")*"|[\w$]+)\s*\.\s*"?$`)

// sqlLexed blanks string literals and comments in ONE left-to-right pass, so
// whichever starts first wins: a quote inside a comment ("-- what's") does
// not open a string, and "--" inside an identifier ("a--b") or a string does
// not open a comment. Double-quoted identifiers are matched only so they are
// skipped whole, and are kept as they are: the check below reads "events" and
// "main"."events" through them. Comments go, so one ending in a dot cannot
// pass for a qualifier ("-- the log.\n events").
var sqlLexed = regexp.MustCompile(`'(?:[^']|'')*'|"(?:[^"]|"")*"|--[^\n]*|(?s:/\*.*?\*/)`)

func sqlBlankLiteralsAndComments(statement string) string {
	return sqlLexed.ReplaceAllStringFunc(statement, func(tok string) string {
		switch {
		case strings.HasPrefix(tok, `"`):
			return tok
		case strings.HasPrefix(tok, "'"):
			return "''"
		default:
			return " "
		}
	})
}

func sqlMentionsEvents(statement string) bool {
	statement = sqlBlankLiteralsAndComments(statement)
	for _, m := range sqlEventsWord.FindAllStringIndex(statement, -1) {
		after := strings.TrimLeft(statement[m[1]:], `"`)
		if strings.HasPrefix(strings.TrimLeft(after, " \t\r\n"), ".") {
			continue // events.<table>: a schema called events
		}
		if q := sqlQualifierBefore.FindStringSubmatch(statement[:m[0]]); q != nil {
			// main.events, and memory.events: the SQL card's DuckDB opens
			// with no file, so its database is called memory.
			if part := strings.Trim(q[1], `"`); !strings.EqualFold(part, "main") && !strings.EqualFold(part, "memory") {
				continue // <schema>.events: a table called events
			}
		}
		return true
	}
	return false
}

// viewsLeftOutMessage answers a copy that has tables and defines no view
// because every one of them is in a schema DuckDB keeps for itself (#2013).
// Without it that copy was refused for a reason that is false: "only on
// S3", or "no snapshot was found". Empty when that is not what happened.
func viewsLeftOutMessage(in views.Input) string {
	if len(in.Baselines) == 0 || in.RendersAnyView() {
		return ""
	}
	notes := in.NamingNotes()
	if len(notes) == 0 {
		return ""
	}
	return "every table of the snapshot is in a schema DuckDB keeps for itself, so there is no view to query: " +
		strings.Join(notes, "; ")
}

// sqlInfoResponse is GET /api/sql: what the panel needs before the first
// query, read from the same resolver as /api/views.sql with no DuckDB and
// no worker. Views are the names a statement can use.
type sqlInfoResponse struct {
	Views []string `json:"views"`
	// Notes name the tables listed under another name or not at all, and
	// why (#2013): a table in a schema DuckDB keeps for itself has no view,
	// and without this the list would just be one table short.
	Notes         []string     `json:"notes,omitempty"`
	CopyUpdatedAt *time.Time   `json:"copy_updated_at"`
	Limits        sqlLimitsDTO `json:"limits"`
}

// sqlLimitsDTO are the caps a query runs under, so the panel states the
// real ones instead of hardcoding them.
type sqlLimitsDTO struct {
	TimeoutSeconds int `json:"timeout_seconds"`
	MaxRows        int `json:"max_rows"`
	MaxCellBytes   int `json:"max_cell_bytes"`
}

// handleSQLInfo is GET /api/sql. Same permission and the same gates as the
// POST; it lists the views the copy defines (events, when a LOCAL change
// log exists, and one state view per table of the newest snapshot), the
// snapshot they are pinned to, and the limits. Metadata only: names, a
// time, three numbers. Not audited as a data read.
func (s *Server) handleSQLInfo(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	if !s.sqlGate(w, r, b) {
		return
	}
	in, err := s.buildViewsInput(r.Context(), b, viewsRequest{PinSnapshot: true, OmitEvents: true})
	switch {
	case errors.Is(err, errNoViewSources):
		writeJSONError(w, http.StatusConflict, errNoViewSources.Error()+"; there is no copy to run SQL on yet")
		return
	case err != nil:
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}
	if in.NeedsS3() {
		writeJSONError(w, http.StatusConflict, sqlCopyNotLocalMessage)
		return
	}
	// The events view is offered only over a local change log: a statement
	// reading it where the change log is in S3 cannot be answered here.
	eventsLocal := sqlArchivesLocal(in.ArchiveSources)
	names := []string{}
	for _, n := range in.DefinedViews() {
		if n == "events" && !eventsLocal {
			continue
		}
		names = append(names, n)
	}
	if len(names) == 0 {
		if why := viewsLeftOutMessage(in); why != "" {
			writeJSONError(w, http.StatusConflict, why)
			return
		}
		writeJSONError(w, http.StatusConflict, sqlCopyNotLocalMessage)
		return
	}
	resp := sqlInfoResponse{Views: names, Notes: in.NamingNotes(), Limits: sqlLimitsDTO{
		TimeoutSeconds: int(s.sqlLimits.Timeout / time.Second),
		MaxRows:        s.sqlLimits.MaxRows,
		MaxCellBytes:   sqlsandbox.MaxCellBytes,
	}}
	if !in.BaselineSnapshot.IsZero() {
		at := in.BaselineSnapshot.UTC()
		resp.CopyUpdatedAt = &at
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	if !s.sqlGate(w, r, b) {
		return
	}
	if f := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format"))); f != "" && f != "csv" && f != "json" {
		writeJSONError(w, http.StatusBadRequest, "format="+f+" is not a form this route produces; use csv, or leave it out for JSON")
		return
	}
	var req sqlRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, sqlRequestMaxBytes)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "the request body must be JSON with a \"sql\" field: "+err.Error())
		return
	}
	if strings.TrimSpace(req.SQL) == "" {
		writeJSONError(w, http.StatusBadRequest, "the \"sql\" field is empty")
		return
	}
	if req.MaxRows < 0 || req.MaxRows > s.sqlLimits.MaxRows {
		writeJSONError(w, http.StatusBadRequest,
			fmt.Sprintf("max_rows must be between 1 and %d (this server's cap), or left out", s.sqlLimits.MaxRows))
		return
	}

	out, err := s.runSQL(r.Context(), b, consoleActor(r), req.SQL, "", req.MaxRows)
	if err != nil {
		var refusal *sqlRefusal
		if errors.As(err, &refusal) {
			writeJSONError(w, refusal.Status, refusal.Message)
			return
		}
		s.writeSQLError(w, r, err)
		return
	}
	res, copyUpdatedAt := out.Result, out.CopyUpdatedAt

	csv := wantsCSV(r)
	if csv {
		writeSQLCSV(w, res)
	} else {
		writeJSON(w, http.StatusOK, sqlResponse{
			Columns: res.Columns, Rows: res.Rows, Truncated: res.Truncated,
			TruncatedCells: res.TruncatedCells, ElapsedMS: res.Elapsed.Milliseconds(),
			CopyUpdatedAt: copyUpdatedAt, PhasesMS: sqlPhasesMS(out.ViewBuild, res.Phases),
		})
	}
	// After the response, like the events surface: the rows were read and
	// served. The statement is the reader's own text, not row data, and is
	// what an auditor needs to know what was asked. A CSV stream a client
	// disconnected from mid-way still audits the full row count: the rows
	// were read from the copy and handed to the socket, which is the fact an
	// auditor wants, and the transport's fate is not the console's to know.
	detail := map[string]string{
		"sql":        req.SQL,
		"rows":       strconv.Itoa(len(res.Rows)),
		"truncated":  strconv.FormatBool(res.Truncated),
		"elapsed_ms": strconv.FormatInt(res.Elapsed.Milliseconds(), 10),
	}
	if csv {
		detail["format"] = "csv"
	}
	recordConsoleAccess(r, "sql.run", "", "", detail)
}

// sqlRefusal is a gate the statement did not pass before anything ran: the
// HTTP route answers with Status and Message, the MySQL-protocol port with
// Message alone.
type sqlRefusal struct {
	Status  int
	Message string
}

func (e *sqlRefusal) Error() string { return e.Message }

// sqlOutcome is a statement that ran.
type sqlOutcome struct {
	Result sqlsandbox.Result
	// CopyUpdatedAt is the newest snapshot the state views were pinned to;
	// nil when the copy has no snapshot and only the change log is queryable.
	CopyUpdatedAt *time.Time
	// ViewBuild is the daemon's own part of the statement's time: snapshot
	// discovery, the archive reads when the statement names events, and the
	// views script. The sandbox's phases are in Result.Phases.
	ViewBuild time.Duration
}

// runSQL is POST /api/sql without the HTTP: the gates that need the copy
// (local, defines a view, holds no console configuration), the views, the
// job and the run. user keys the one-query-at-a-time gate; schema, when
// set, is where unqualified names resolve (the port's USE); maxRows 0 is
// the server's cap. The data-profile and archive gates are the caller's:
// they need the request, or the target, which this does not see. The
// errors are a *sqlRefusal or the runner's own typed errors, so each caller
// maps them to its wire.
func (s *Server) runSQL(ctx context.Context, b *bundle, user, statement, schema string, maxRows int) (sqlOutcome, error) {
	// What is known without any I/O is refused before the slot: a copy that
	// lives only on S3 is never served here, and listing it would hold one
	// of the daemon's two slots while it waits on the network.
	if strings.HasPrefix(b.baselineSrc, "s3://") {
		return sqlOutcome{}, &sqlRefusal{http.StatusConflict, sqlCopyNotLocalMessage}
	}
	// The slot first (#2026): a statement that will be refused as busy must
	// not pay for the view build below, which reads the index and walks the
	// copy. An unused slot is given back on every early return, and said so
	// at debug with how long it was held: the slot is one of two, so a
	// build that holds it for long is everybody else's "busy".
	slot, err := s.sqlRunner.Reserve(user)
	if err != nil {
		return sqlOutcome{}, err
	}
	ran := false
	viewsStart := time.Now()
	defer func() {
		slot.Release()
		if !ran {
			slog.Debug("console: sql slot released without a run", "user", user, "held", time.Since(viewsStart))
		}
	}()
	// The build has a deadline so a slow disk or listing cannot hold the
	// slot for long; the sandbox has its own, longer one for the statement.
	buildCtx, cancelBuild := context.WithTimeout(ctx, baselineListTimeout)
	in, eventsInS3, err := s.sqlViewsFor(buildCtx, b, statement)
	cancelBuild()
	switch {
	case errors.Is(err, errNoViewSources):
		return sqlOutcome{}, &sqlRefusal{http.StatusConflict,
			errNoViewSources.Error() + "; there is no copy to run SQL on yet"}
	case err != nil:
		return sqlOutcome{}, &sqlRefusal{http.StatusBadGateway, err.Error()}
	}
	if why := viewsLeftOutMessage(in); why != "" {
		return sqlOutcome{}, &sqlRefusal{http.StatusConflict, why}
	}
	if in.NeedsS3() || (!in.RendersAnyView() && eventsInS3) {
		// The tables themselves are in S3, or the change log in S3 is all
		// there is: nothing here is local.
		return sqlOutcome{}, &sqlRefusal{http.StatusConflict, sqlCopyNotLocalMessage}
	}
	if !in.RendersAnyView() {
		return sqlOutcome{}, &sqlRefusal{http.StatusConflict,
			"the copy defines no view to query: no snapshot was found to build state views from, and no archived partition"}
	}
	if s.sqlViewsObserver != nil {
		s.sqlViewsObserver(in)
	}
	copyDirs := sqlCopyDirs(in)
	if dir, file, covered := dirCoversConfig(copyDirs, s.protectedConfigFiles()); covered {
		return sqlOutcome{}, &sqlRefusal{http.StatusConflict,
			fmt.Sprintf("the copy directory %s contains the console's own configuration (%s), so SQL cannot run over it; "+
				"point this server's snapshot directory at a directory that holds only snapshots", dir, filepath.Base(file))}
	}
	var copyUpdatedAt *time.Time
	if !in.BaselineSnapshot.IsZero() {
		at := in.BaselineSnapshot.UTC()
		copyUpdatedAt = &at
	}
	viewBuild := time.Since(viewsStart)

	// The worker parses the statement first and asks for the views it names
	// (#2029): installing every table's view was half of a short statement's
	// time on a copy of a hundred tables. CopyDirs stay the whole copy's: they
	// bound what the worker may read, the script only what it defines.
	var viewsSQL string
	job := sqlsandbox.Job{
		// One query at a time per identity: the login identity, or the
		// shared automation token as one identity.
		User:     user,
		CopyDirs: copyDirs,
		ViewsFor: func(refs sqlsandbox.Refs) (string, error) {
			narrowed := in
			narrowed.OnlyViews = sqlWantedViews(in, refs)
			viewsSQL = views.Generate(narrowed) + sqlUsedSchema(in, narrowed.OnlyViews, schema)
			return viewsSQL, nil
		},
		SQL:    statement,
		Schema: schema,
		Limits: sqlsandbox.Limits{MaxRows: maxRows},
	}
	ran = true
	res, err := slot.Run(ctx, job)
	if err != nil {
		var qerr *sqlsandbox.QueryError
		if eventsInS3 && errors.As(err, &qerr) && sqlEventsMissing.MatchString(qerr.Message) {
			// The statement really read events, which was not installed
			// because the change log is in S3: say that, not DuckDB's
			// "does not exist".
			return sqlOutcome{}, &sqlRefusal{http.StatusUnprocessableEntity, sqlEventsInS3Message}
		}
		return sqlOutcome{}, err
	}
	// The measurement #2026 asks for, per statement, at debug so a run under
	// load can be read back from the log.
	slog.Debug("console: sql statement phases", "user", user, "events", sqlMentionsEvents(statement),
		"views_sql_bytes", len(viewsSQL), "phases_ms", sqlPhasesMS(viewBuild, res.Phases))
	for phase, d := range sqlPhases(viewBuild, res.Phases) {
		observe.ObserveSQLStatementPhase(phase, d)
	}
	return sqlOutcome{Result: res, CopyUpdatedAt: copyUpdatedAt, ViewBuild: viewBuild}, nil
}

// sqlWantedViews decides which of the copy's views a statement needs, from
// the relations the worker's parse of it names (#2029). nil means every view,
// and it is the answer whenever the set is not certain: a statement that can
// depend on what it does not name (Refs.Unsure), and a name that matches no
// view and no WITH of the statement, which is a system table, another
// catalog, or a typo: DuckDB's "Did you mean" is computed from the catalog,
// so a typo answered out of a narrowed one would suggest the wrong thing.
// An extra view costs about half a millisecond; a missing one turns a
// working statement into "does not exist".
//
// Names compare the way DuckDB compares them (ASCII case only). A name
// without a schema matches that view in every schema, not only the one the
// search path would pick: following DuckDB's resolution here could only
// leave out the view it picks. A view is matched BEFORE a WITH name, because
// `WITH events AS (SELECT * FROM events ...)` reads the view inside the
// clause that shadows it, and the parse lists the two under one name.
func sqlWantedViews(in views.Input, refs sqlsandbox.Refs) views.ViewSet {
	if refs.Unsure {
		return nil
	}
	names := in.ViewNames()
	ctes := map[string]bool{}
	for _, c := range refs.CTEs {
		ctes[sqlFoldName(c)] = true
	}
	want := views.ViewSet{}
	for _, r := range refs.Tables {
		catalog, schema, name := sqlFoldName(r.Catalog), sqlFoldName(r.Schema), sqlFoldName(r.Name)
		if catalog != "" && catalog != "memory" {
			return nil // the worker's DuckDB has one catalog, memory
		}
		matched := false
		for _, n := range names {
			if sqlFoldName(n.View) == name && (schema == "" || sqlFoldName(n.Schema) == schema) {
				want[n.Key] = true
				matched = true
			}
		}
		if !matched && !(schema == "" && ctes[name]) {
			return nil
		}
	}
	return want
}

// sqlUsedSchema keeps the port's USE working on a narrowed script: the worker
// applies a schema only if the views created it, so `USE shop` followed by a
// statement that names no view of shop (SELECT current_schema()) would
// otherwise fall back to main and say the schema does not exist. Returns the
// statement creating that schema, spelled as the copy spells it, when the
// copy has it; IF NOT EXISTS makes it a no-op when a wanted view already
// created it. A schema the copy does not have stays missing, and gets the
// worker's hint.
func sqlUsedSchema(in views.Input, wanted views.ViewSet, schema string) string {
	if schema == "" || wanted == nil {
		return ""
	}
	for _, n := range in.ViewNames() {
		if sqlFoldName(n.Schema) == sqlFoldName(schema) {
			return "\nCREATE SCHEMA IF NOT EXISTS " + sqlQuoteIdent(n.Schema) + ";\n"
		}
	}
	return ""
}

// sqlFoldName folds A-Z only, as DuckDB does when it compares names: Ñ and ñ
// are two names there.
func sqlFoldName(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// sqlQuoteIdent quotes an identifier for DuckDB.
func sqlQuoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// sqlCopyDirs lists the directories the generated views read, and nothing
// wider: each archive base (bintrail_id=<id>), the directory holding each
// pinned table file (its schema directory under the snapshot, where a
// legacy delta pair sits beside it), and the directory of each delta file
// the chain names. NEVER the snapshot ROOT (bundle.baselineSrc): the root is
// operator-editable with servers:write, and a root such as the state
// directory would put the servers registry (DSN passwords, S3 keys), the
// auth file and the MCP token within read_text's reach inside the sandbox.
// Order is stable: archives first, then tables in their listed order.
//
// Every directory is made absolute: a relative boot --baseline-dir, or a
// relative --archive-dir recorded in archive_state, is still a LOCAL copy,
// and the sandbox refuses a relative directory as "not local", which the
// user would read as "only on S3". A path that cannot be made absolute is
// kept as given and refused downstream.
func sqlCopyDirs(in views.Input) []string {
	var dirs []string
	seen := map[string]bool{}
	add := func(d string) {
		if d == "" {
			return
		}
		if abs, err := filepath.Abs(d); err == nil {
			d = abs
		}
		d = filepath.Clean(d)
		if d == "." || seen[d] {
			return
		}
		seen[d] = true
		dirs = append(dirs, d)
	}
	// The archive bases only when the events view is installed: they are
	// what it reads, and nothing else does.
	if in.RendersEventsView() {
		for _, src := range in.ArchiveSources {
			add(src)
		}
	}
	for _, t := range in.SelectedBaselines() {
		if t.Path != "" {
			add(filepath.Dir(t.Path))
		}
		for _, f := range t.DeltaFiles {
			for _, p := range []string{f.Posdel, f.Upserts} {
				if filepath.IsAbs(p) {
					add(filepath.Dir(p))
				}
			}
		}
	}
	return dirs
}

// protectedConfigFiles are the console's own files a copy directory must not
// contain: the servers registry, the auth file and the managed MCP token.
// The paths this process resolved at boot, or the defaults when a unit
// server did not resolve them.
func (s *Server) protectedConfigFiles() []string {
	files := []string{s.cm.reg.Path(), s.authPath, s.mcpTokenPath}
	if files[1] == "" {
		files[1] = DefaultAuthPath()
	}
	if files[2] == "" {
		files[2] = DefaultMCPTokenPath()
	}
	var out []string
	for _, f := range files {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

// dirCoversConfig reports the first copy directory that is the directory of
// one of files, or an ancestor of it, on the cleaned paths and on their
// resolved forms when they exist (a symlinked state directory is the same
// directory). A copy that covers a config file would let the sandbox read
// it, so the second belt behind sqlCopyDirs' narrowing.
//
// Two gaps this lexical check leaves, known and accepted: a config file
// path given as RELATIVE never matches (filepath.Rel across an absolute
// copy dir errors, which reads as not covered; the console resolves its
// config paths absolute at boot, and the first belt does not depend on
// this one), and a case-insensitive file system (macOS) can spell the same
// directory two ways; Linux is the shipping target.
func dirCoversConfig(copyDirs, files []string) (dir, file string, covered bool) {
	forms := func(p string) []string {
		out := []string{filepath.Clean(p)}
		if r, err := filepath.EvalSymlinks(p); err == nil && filepath.Clean(r) != out[0] {
			out = append(out, filepath.Clean(r))
		}
		return out
	}
	for _, d := range copyDirs {
		for _, f := range files {
			for _, dd := range forms(d) {
				for _, ff := range forms(filepath.Dir(f)) {
					rel, err := filepath.Rel(dd, ff)
					if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
						return d, f, true
					}
				}
			}
		}
	}
	return "", "", false
}

// sqlViewsFor resolves the views the worker installs for one statement, the
// way /api/views.sql resolves them: local paths (never the portable S3
// spellings), the state views pinned to the snapshot that exists now, no
// live leg.
//
// The events view is decided in two steps. First the layout WITHOUT it:
// that read never touches S3, and it says where the change log lives. Then
// the view is added only when the statement names it (sqlMentionsEvents) or
// when it is all the copy has, AND the change log is local. eventsInS3
// reports the other case: the statement names events (or nothing else is
// defined) and the change log is in S3, so the view was left out. A
// statement that merely contains the word then runs over the tables as it
// should, and one that really reads events gets sqlEventsInS3Message
// instead of "the copy is only on S3", which would be false for a server
// whose tables are local.
func (s *Server) sqlViewsFor(ctx context.Context, b *bundle, statement string) (in views.Input, eventsInS3 bool, err error) {
	// A statement that does not name events needs the state views alone:
	// no archive_state read, no per-file check of the change log, no listing
	// of the other snapshot location (#2026). That was most of the daemon's
	// fixed cost per statement, paid by every statement. A copy that gives
	// no state view this way (no snapshot, or every table in a schema
	// DuckDB keeps for itself) falls through to the archive, as before.
	tablesOnly := viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true}
	if !sqlMentionsEvents(statement) {
		in, err = s.buildViewsInput(ctx, b, tablesOnly)
		if err == nil && in.RendersAnyView() {
			return in, false, nil
		}
		if err != nil && !errors.Is(err, errNoViewSources) {
			return views.Input{}, false, err
		}
	}
	// The archive is needed. Read it WITHOUT the events view first: that
	// build does no S3 work (NeedsS3 is false with events omitted), so an
	// S3 change log is detected from the sources alone and answered with
	// the tables, exactly as before; only a local change log is then built
	// with the events view on.
	in, err = s.buildViewsInput(ctx, b, viewsRequest{PinSnapshot: true, OmitEvents: true, ForStatement: true})
	if err != nil {
		return views.Input{}, false, err
	}
	if len(in.ArchiveSources) == 0 {
		return in, false, nil
	}
	if !sqlArchivesLocal(in.ArchiveSources) {
		return in, true, nil
	}
	in, err = s.buildViewsInput(ctx, b, viewsRequest{PinSnapshot: true, ForStatement: true})
	if err != nil {
		return views.Input{}, false, err
	}
	return in, false, nil
}

// sqlEventsMissing matches DuckDB's answer to a statement that reads a
// table named events that is not defined.
var sqlEventsMissing = regexp.MustCompile(`(?i)table with name "?events"? does not exist`)

// sqlEventsInS3Message answers a statement that reads events on a server
// whose change log is in S3.
const sqlEventsInS3Message = "the change history for this server is on S3, so events cannot be read here; the tables can"

// sqlWorkerFailedMessage replaces a WorkerError on every wire: its text can
// carry host paths, so the log gets it and the client gets this.
const sqlWorkerFailedMessage = "the SQL worker failed before it could answer; DBTrail's log has the details"

// sqlCopyNotLocalMessage is the 409 for a copy the worker cannot reach.
const sqlCopyNotLocalMessage = "the copy for this server is only on S3; SQL in the browser needs a local copy"

// writeSQLError maps the runner's typed errors to actionable responses. A
// WorkerError never reaches the client (its stderr can carry host paths);
// the console log gets it.
func (s *Server) writeSQLError(w http.ResponseWriter, r *http.Request, err error) {
	var refused *sqlsandbox.RefusedError
	var qerr *sqlsandbox.QueryError
	var timeout *sqlsandbox.TimeoutError
	var werr *sqlsandbox.WorkerError
	switch {
	case errors.As(err, &refused):
		writeJSONError(w, http.StatusUnprocessableEntity, refused.Reason)
	case errors.As(err, &qerr):
		writeJSONError(w, http.StatusUnprocessableEntity, qerr.Message)
	case errors.As(err, &timeout):
		writeJSONError(w, http.StatusGatewayTimeout,
			fmt.Sprintf("the query ran longer than this server's cap of %.0f s and was stopped; narrow it "+
				"(a WHERE on a state view, or a smaller window on events)", timeout.Limit.Seconds()))
	case errors.Is(err, sqlsandbox.ErrBusy):
		writeJSONError(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, sqlsandbox.ErrCopyNotLocal):
		writeJSONError(w, http.StatusConflict, sqlCopyNotLocalMessage)
	case errors.Is(err, sqlsandbox.ErrResultTooLarge):
		writeJSONError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// The client went away; there is nobody to answer.
		slog.Debug("console: sql request cancelled by the client", "error", err)
	case errors.As(err, &werr):
		slog.Error("console: the SQL worker failed", "error", err, "server", r.Header.Get(serverHeader))
		writeJSONError(w, http.StatusInternalServerError, sqlWorkerFailedMessage)
	default:
		slog.Error("console: sql query failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "the query could not be run; DBTrail's log has the details")
	}
}

// writeSQLCSV renders the result as text/csv, one line per row, CRLF
// separated, cells rendered by csvCell: the SAME dialect the console's
// client-side events export writes (csvCell in assets/app.js), so a file
// from either download opens the same way. Streamed row by row.
func writeSQLCSV(w http.ResponseWriter, res sqlsandbox.Result) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="dbtrail-sql-`+time.Now().UTC().Format("2006-01-02T15-04-05Z")+`.csv"`)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	bw := bufio.NewWriter(w)
	names := make([]string, len(res.Columns))
	for i, c := range res.Columns {
		names[i] = csvCell(c.Name)
	}
	bw.WriteString(strings.Join(names, ","))
	for _, row := range res.Rows {
		bw.WriteString("\r\n")
		for i, v := range row {
			if i > 0 {
				bw.WriteByte(',')
			}
			bw.WriteString(csvCell(v))
		}
	}
	if err := bw.Flush(); err != nil {
		slog.Debug("console: client went away while receiving the CSV", "error", err)
	}
}

// csvCell renders one cell the way assets/app.js csvCell does: null is
// empty, a nested value is its JSON, a leading formula character (=, +, -,
// @, tab, CR) is neutralized with a quote (OWASP formula injection), and a
// value with a quote, comma or line break is quoted with quotes doubled.
// TestCSVCellMatchesTheClientDialect pins the two rule sets together.
func csvCell(v any) string {
	var s string
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		s = x
	case json.Number:
		s = x.String()
	case bool:
		s = strconv.FormatBool(x)
	case []any, map[string]any:
		// Not json.Marshal: it HTML-escapes <, > and &, which JSON.stringify
		// in the client twin does not.
		var buf strings.Builder
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(x); err != nil {
			s = fmt.Sprint(x)
		} else {
			s = strings.TrimSuffix(buf.String(), "\n")
		}
	default:
		s = fmt.Sprint(x)
	}
	if s != "" {
		switch s[0] {
		case '=', '+', '-', '@', '\t', '\r':
			s = "'" + s
		}
	}
	if strings.ContainsAny(s, "\",\r\n") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}
