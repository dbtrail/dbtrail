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
	"strconv"
	"strings"
	"time"

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
// so the handler's tests can run without a child process.
type sqlRunner interface {
	Run(ctx context.Context, job sqlsandbox.Job) (sqlsandbox.Result, error)
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

func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	if s.profileActiveFor(r) {
		recordProfileGateDeny(r, "sql")
		writeJSONError(w, http.StatusForbidden,
			"SQL on the copy is unavailable while a data profile is active: the profile withholds "+
				"tables and redacts columns on the console's own reads, and free SQL reads the raw "+
				"Parquet files, which it cannot filter")
		return
	}
	if b.noArchive {
		writeJSONError(w, http.StatusConflict,
			"archive access is disabled for this server, so its copy cannot be read")
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

	// The copy's layout, resolved the way /api/views.sql resolves it: local
	// paths (never the portable S3 spellings), the state views pinned to the
	// snapshot that exists now, no live leg.
	in, err := s.buildViewsInput(r.Context(), b, viewsRequest{PinSnapshot: true})
	switch {
	case errors.Is(err, errNoViewSources):
		writeJSONError(w, http.StatusConflict,
			errNoViewSources.Error()+"; there is no copy to run SQL on yet")
		return
	case err != nil:
		writeJSONError(w, http.StatusBadGateway, err.Error())
		return
	}
	if in.NeedsS3() {
		writeJSONError(w, http.StatusConflict, sqlCopyNotLocalMessage)
		return
	}
	if !in.RendersAnyView() {
		writeJSONError(w, http.StatusConflict,
			"the copy defines no view to query: no snapshot was found to build state views from, and no archived partition")
		return
	}
	if s.sqlViewsObserver != nil {
		s.sqlViewsObserver(in)
	}
	copyDirs := sqlCopyDirs(in)
	if dir, file, covered := dirCoversConfig(copyDirs, s.protectedConfigFiles()); covered {
		writeJSONError(w, http.StatusConflict,
			fmt.Sprintf("the copy directory %s contains the console's own configuration (%s), so SQL cannot run over it; "+
				"point this server's snapshot directory at a directory that holds only snapshots", dir, filepath.Base(file)))
		return
	}
	var copyUpdatedAt *time.Time
	if !in.BaselineSnapshot.IsZero() {
		at := in.BaselineSnapshot.UTC()
		copyUpdatedAt = &at
	}

	job := sqlsandbox.Job{
		// One query at a time per identity: the login identity, or the
		// shared automation token as one identity.
		User:     consoleActor(r),
		CopyDirs: copyDirs,
		ViewsSQL: views.Generate(in),
		SQL:      req.SQL,
		Limits:   sqlsandbox.Limits{MaxRows: req.MaxRows},
	}
	res, err := s.sqlRunner.Run(r.Context(), job)
	if err != nil {
		s.writeSQLError(w, r, err)
		return
	}

	csv := wantsCSV(r)
	if csv {
		writeSQLCSV(w, res)
	} else {
		writeJSON(w, http.StatusOK, sqlResponse{
			Columns: res.Columns, Rows: res.Rows, Truncated: res.Truncated,
			TruncatedCells: res.TruncatedCells, ElapsedMS: res.Elapsed.Milliseconds(),
			CopyUpdatedAt: copyUpdatedAt,
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
	for _, src := range in.ArchiveSources {
		add(src)
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
		writeJSONError(w, http.StatusInternalServerError,
			"the SQL worker failed before it could answer; the console log has the details")
	default:
		slog.Error("console: sql query failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "the query could not be run; the console log has the details")
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
