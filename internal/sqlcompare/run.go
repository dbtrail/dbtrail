package sqlcompare

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// Options configures one comparison run.
type Options struct {
	// SourceDSN is the MySQL source (go-sql-driver form). Reads only.
	SourceDSN string
	// CopyDSN is the console's MySQL-protocol port for the same server
	// (user = server id or name, password = the access token), with read
	// routing OFF so every statement runs on the copy.
	CopyDSN string
	// Schema, when set, is USE'd on both sides before the statements.
	Schema string
	// MaxRows bounds how many rows are read from the source per statement
	// (the copy has the port's own cap); more is INCONCLUSIVE. 0 = 1000.
	MaxRows int
	// Policy is the router's threshold, for the "what would the router do"
	// column. The zero value never picks the copy.
	Policy readrouter.Policy
	// NoRerun disables the second source run on a DIFFERENT statement.
	// The rerun tells a live write (the source changed between runs:
	// INCONCLUSIVE) from a real semantic difference.
	NoRerun bool
}

// Result is one statement's comparison.
type Result struct {
	Statement string  `json:"statement"`
	Verdict   Verdict `json:"verdict"`
	// Kind refines DIFFERENT (see Compare) and INCONCLUSIVE ("copy_cut",
	// "source_over_max_rows", "source_changed").
	Kind   string `json:"kind,omitempty"`
	Detail string `json:"detail,omitempty"`
	// Route is what the read router would do with this statement, "copy"
	// or "mysql", and RouteReason why.
	Route       string  `json:"route"`
	RouteReason string  `json:"route_reason"`
	PlanCost    float64 `json:"plan_cost,omitempty"`
	SourceRows  int     `json:"source_rows"`
	CopyRows    int     `json:"copy_rows"`
	SourceMS    float64 `json:"source_ms"`
	CopyMS      float64 `json:"copy_ms"`
}

// Report is the whole run.
type Report struct {
	Results []Result `json:"results"`
	// Counts per verdict.
	Counts map[Verdict]int `json:"counts"`
	// CopyDiffers is the number that matters: statements the router would
	// send to the copy whose answer DIFFERS from MySQL's in content.
	CopyDiffers int `json:"copy_differs"`
	// Compared is how many statements reached a comparison (EQUAL or
	// DIFFERENT); zero means the run measured nothing.
	Compared int `json:"compared"`
	// ExplainFailed counts statements whose EXPLAIN on the source failed:
	// the router would forward them, so no difference of theirs can count.
	ExplainFailed int `json:"explain_failed"`
	// CopyOrderDiffers counts copy-routed statements that return the same
	// rows in a different order: ties in the ORDER BY resolve differently
	// on each engine, so this is reported apart and never fails the run.
	CopyOrderDiffers int `json:"copy_order_differs"`
}

// Failed reports whether the run found what it exists to find.
func (r *Report) Failed() bool { return r.CopyDiffers > 0 }

// NothingCompared reports a run in which no statement reached a comparison
// (every one refused, errored or skipped): "no difference" would be a false
// reassurance.
func (r *Report) NothingCompared() bool { return r.Compared == 0 }

// Run compares every statement. Both connections are dedicated (USE and
// SHOW WARNINGS are per connection). The source is never written to: a
// statement that is not a plain read is SKIPPED before anything runs. A
// connection lost mid-run aborts with an error rather than filing every
// later statement as an error verdict.
func Run(ctx context.Context, opts Options, statements []string) (*Report, error) {
	if opts.MaxRows <= 0 {
		opts.MaxRows = 1000
	}
	src, err := openConn(ctx, opts.SourceDSN, opts.Schema)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	defer src.Close()
	cp, err := openConn(ctx, opts.CopyDSN, opts.Schema)
	if err != nil {
		return nil, fmt.Errorf("copy: %w", err)
	}
	defer cp.Close()
	if err := probeCopy(ctx, cp.c); err != nil {
		return nil, err
	}

	rep := &Report{Counts: map[Verdict]int{}}
	for i, stmt := range statements {
		res := compareOne(ctx, src, cp, opts, stmt)
		if lost := connectionLost(res); lost != "" {
			return nil, fmt.Errorf("the connection to the %s was lost after %d statement(s): %s", lost, i, res.Detail)
		}
		rep.Results = append(rep.Results, res)
		rep.Counts[res.Verdict]++
		if res.Verdict == Equal || res.Verdict == Different {
			rep.Compared++
		}
		if strings.HasPrefix(res.RouteReason, "could not explain") {
			rep.ExplainFailed++
		}
		if res.Verdict == Different && res.Route == "copy" {
			if res.Kind == "order" {
				rep.CopyOrderDiffers++
			} else {
				rep.CopyDiffers++
			}
		}
	}
	return rep, nil
}

// probeCopy makes sure --copy-dsn is the copy with routing OFF. With routing
// on, the port forwards every cheap statement to MySQL and this tool would
// compare MySQL with MySQL: every line EQUAL, exit 0, the worst possible
// outcome. `2 ^ 3` is vetoed by the router (so a routing port forwards it
// to MySQL, where ^ is XOR: 1), while on the copy ^ is power: 8. Independent
// of collation and sql_mode, which a probe such as 'a' = 'A' was not once
// the copy folded case; and not connection chatter the port answers itself
// (version() is).
func probeCopy(ctx context.Context, c *sql.Conn) error {
	var v sql.NullString
	if err := c.QueryRowContext(ctx, "SELECT 2 ^ 3").Scan(&v); err != nil {
		return fmt.Errorf("the copy cannot run SQL (%w); --copy-dsn must be the console's MySQL-protocol port for a server whose copy is on local disk", err)
	}
	if !v.Valid || !strings.HasPrefix(v.String, "8") {
		return fmt.Errorf("--copy-dsn answered like MySQL (2 ^ 3 = %q): the port has read routing on (--route-max-copy-age); sql-compare needs a port with routing off, so every statement runs on the copy", v.String)
	}
	return nil
}

// connectionLost names the side whose connection died, or "".
func connectionLost(r Result) string {
	switch r.Verdict {
	case SourceError:
		if isLostConn(r.Detail) {
			return "source"
		}
	case NotOnCopy:
		if isLostConn(r.Detail) {
			return "copy"
		}
	}
	return ""
}

func isLostConn(detail string) bool {
	for _, m := range []string{driver.ErrBadConn.Error(), "Error 2006", "Error 2013", "broken pipe", "connection reset", "unexpected EOF"} {
		if strings.Contains(detail, m) {
			return true
		}
	}
	return false
}

type conn struct {
	db *sql.DB
	c  *sql.Conn
}

// openConn opens one dedicated connection. config.OpenMySQL (local-file
// access off) rather than config.Connect: Connect injects parseTime=true,
// which would turn every DATETIME into a Go time value and reformat it,
// while this tool compares the text the protocol carried, on both sides
// alike.
func openConn(ctx context.Context, dsn, schema string) (*conn, error) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := config.OpenMySQL(cfg)
	if err != nil {
		return nil, err
	}
	c, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	if schema != "" {
		if _, err := c.ExecContext(ctx, "USE `"+strings.ReplaceAll(schema, "`", "``")+"`"); err != nil {
			c.Close()
			db.Close()
			return nil, fmt.Errorf("USE %s: %w", schema, err)
		}
	}
	return &conn{db: db, c: c}, nil
}

func (c *conn) Close() {
	c.c.Close()
	c.db.Close()
}

func compareOne(ctx context.Context, src, cp *conn, opts Options, stmt string) Result {
	res := Result{Statement: stmt}
	if readrouter.Classify(stmt) != readrouter.KindSelect {
		res.Verdict, res.Kind, res.Detail = Skipped, "not_a_select", "this tool only compares SELECTs"
		res.Route, res.RouteReason = "mysql", "not a select"
		return res
	}
	if why := ReadOnlyVeto(stmt); why != "" {
		res.Verdict, res.Kind, res.Detail = Skipped, "not_read_only", "not run: "+why
		res.Route, res.RouteReason = "mysql", "not a plain read"
		return res
	}
	res.Route, res.RouteReason, res.PlanCost = route(ctx, src, opts.Policy, stmt)

	srcRows, srcOver, srcMS, srcErr := fetch(ctx, src.c, stmt, opts.MaxRows)
	res.SourceMS = srcMS
	if srcErr != nil {
		res.Verdict, res.Detail = SourceError, srcErr.Error()
		return res
	}
	res.SourceRows = len(srcRows.Rows)
	// The copy gets the statement the ROUTER would send it: the same text,
	// untranslated. A statement with backtick names reads NOT_ON_COPY, which
	// is what the router does with it too (the copy refuses, MySQL answers).
	cpRows, cpOver, cpMS, cpErr := fetch(ctx, cp.c, stmt, opts.MaxRows)
	res.CopyMS = cpMS
	if cpErr != nil {
		res.Verdict, res.Detail = NotOnCopy, cpErr.Error()
		return res
	}
	res.CopyRows = len(cpRows.Rows)
	cut, err := warnings(ctx, cp.c)
	if err != nil {
		res.Verdict, res.Kind, res.Detail = Inconclusive, "copy_warnings_unreadable", "could not read the copy's warnings after the statement: "+err.Error()
		return res
	}
	cpCut := cut > 0 || cpOver
	// A row-count difference is provable across a cap: one side was cut
	// and the other, complete, has FEWER rows than the cut side returned.
	switch {
	case cpCut && !srcOver && len(srcRows.Rows) < len(cpRows.Rows):
		res.Verdict, res.Kind, res.Detail = Different, "rows", fmt.Sprintf("source returned %d rows, copy more than %d", len(srcRows.Rows), len(cpRows.Rows))
		return res
	case srcOver && !cpCut && len(cpRows.Rows) < len(srcRows.Rows):
		res.Verdict, res.Kind, res.Detail = Different, "rows", fmt.Sprintf("source returned more than %d rows, copy %d", len(srcRows.Rows), len(cpRows.Rows))
		return res
	case cut > 0:
		res.Verdict, res.Kind, res.Detail = Inconclusive, "copy_cut", fmt.Sprintf("the copy cut its result at the port's row or cell cap (%d warning(s)); narrow the query to compare", cut)
		return res
	case srcOver || cpOver:
		res.Verdict, res.Kind, res.Detail = Inconclusive, "over_max_rows", fmt.Sprintf("more than %d rows on a side; add a LIMIT or raise --max-rows", opts.MaxRows)
		return res
	}
	ordered := TopLevelOrderBy(stmt)
	verdict, kind, detail := Compare(srcRows, cpRows, ordered)
	res.Verdict, res.Kind, res.Detail = verdict, kind, detail
	if verdict == Different && !opts.NoRerun {
		// A live write between the two reads looks exactly like a semantic
		// difference. Read the source again: if the copy agrees with the
		// SECOND read, the first was stale and nothing can be concluded. If
		// the copy agrees with neither, the difference stands (noted when
		// the source moved too).
		again, _, _, err := fetch(ctx, src.c, stmt, opts.MaxRows)
		if err == nil {
			if v, _, _ := Compare(again, cpRows, ordered); v == Equal {
				res.Verdict, res.Kind, res.Detail = Inconclusive, "source_changed", "the copy matches a second read of the source: the first read was before a live write; compare on a quiet source or right after a snapshot"
			} else if v, _, _ := Compare(srcRows, again, ordered); v != Equal {
				res.Detail += "; the source also changed between two reads"
			}
		}
	}
	return res
}

// route is the read router's decision for the statement, taken the same way
// the port takes it: veto first, then EXPLAIN FORMAT=JSON on the source.
func route(ctx context.Context, src *conn, pol readrouter.Policy, stmt string) (string, string, float64) {
	if v := readrouter.Veto(stmt); v != "" {
		return "mysql", "veto: " + v, 0
	}
	var raw string
	if err := src.c.QueryRowContext(ctx, "EXPLAIN FORMAT=JSON "+stmt).Scan(&raw); err != nil {
		return "mysql", "could not explain: " + err.Error(), 0
	}
	plan, err := readrouter.ParsePlan([]byte(raw))
	if err != nil {
		return "mysql", "could not explain: " + err.Error(), 0
	}
	toCopy, reason := pol.Decide(plan)
	if toCopy {
		return "copy", reason, plan.Cost
	}
	return "mysql", reason, plan.Cost
}

// fetch runs the statement and reads up to max rows; over reports that a
// further row existed.
func fetch(ctx context.Context, c *sql.Conn, stmt string, max int) (out Rows, over bool, ms float64, err error) {
	start := time.Now()
	rows, err := c.QueryContext(ctx, stmt)
	if err != nil {
		return Rows{}, false, sinceMS(start), err
	}
	defer rows.Close()
	out.Columns, err = rows.Columns()
	if err != nil {
		return Rows{}, false, sinceMS(start), err
	}
	raw := make([]sql.RawBytes, len(out.Columns))
	ptrs := make([]any, len(raw))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	for rows.Next() {
		if len(out.Rows) >= max {
			over = true
			break
		}
		if err := rows.Scan(ptrs...); err != nil {
			return Rows{}, false, sinceMS(start), err
		}
		row := make([]*string, len(raw))
		for i, b := range raw {
			if b != nil {
				s := string(b)
				row[i] = &s
			}
		}
		out.Rows = append(out.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return Rows{}, false, sinceMS(start), err
	}
	return out, over, sinceMS(start), nil
}

func sinceMS(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

// warnings counts the warnings of the last statement on the connection;
// the port raises one when it cut a result at its row or cell cap. An
// error is returned, not swallowed: "0 warnings" must mean none.
func warnings(ctx context.Context, c *sql.Conn) (int, error) {
	rows, err := c.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	return n, rows.Err()
}

// SampleSource reads up to n distinct recent SELECTs the source actually ran,
// from performance_schema.events_statements_history_long (its consumer must
// be enabled; the error says which). With schema set, only statements run
// with that current schema. Statements this tool and the console emit
// themselves are left out.
func SampleSource(ctx context.Context, dsn string, n int, schema string) ([]string, error) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := config.OpenMySQL(cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	q := `SELECT SQL_TEXT FROM performance_schema.events_statements_history_long
	WHERE SQL_TEXT IS NOT NULL AND (? = '' OR CURRENT_SCHEMA = ?)
	  AND SQL_TEXT NOT LIKE 'EXPLAIN%' AND SQL_TEXT NOT LIKE 'SHOW%' AND SQL_TEXT NOT LIKE '%performance_schema%'
	  AND LENGTH(SQL_TEXT) < @@performance_schema_max_sql_text_length
	ORDER BY EVENT_ID DESC`
	rows, err := db.QueryContext(ctx, q, schema, schema)
	if err != nil {
		return nil, fmt.Errorf("reading performance_schema.events_statements_history_long (enable it with UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME='events_statements_history_long'): %w", err)
	}
	defer rows.Close()
	var out []string
	seen := map[string]bool{}
	for rows.Next() && len(out) < n {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		s = strings.TrimSpace(s)
		if seen[s] || readrouter.Classify(s) != readrouter.KindSelect {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, errors.New("performance_schema.events_statements_history_long holds no SELECT to sample: the consumer may be off (UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME='events_statements_history_long'), or nothing has run since")
	}
	return out, nil
}

// WriteText renders the report for a terminal: one line per statement, then
// the counts and the one number that matters.
func WriteText(w io.Writer, rep *Report) {
	for _, r := range rep.Results {
		stmt := strings.Join(strings.Fields(r.Statement), " ")
		if len(stmt) > 90 {
			stmt = stmt[:87] + "..."
		}
		fmt.Fprintf(w, "%-12s router=%-5s  %s\n", r.Verdict, r.Route, stmt)
		if r.Verdict == Equal || r.Verdict == Different {
			fmt.Fprintf(w, "             source %d rows %.1f ms, copy %d rows %.1f ms; router: %s\n", r.SourceRows, r.SourceMS, r.CopyRows, r.CopyMS, r.RouteReason)
		} else {
			fmt.Fprintf(w, "             router: %s\n", r.RouteReason)
		}
		if r.Detail != "" {
			if r.Kind != "" {
				fmt.Fprintf(w, "             %s: %s\n", r.Kind, r.Detail)
			} else {
				fmt.Fprintf(w, "             %s\n", r.Detail)
			}
		}
	}
	fmt.Fprintf(w, "\n%d statements:", len(rep.Results))
	for _, v := range []Verdict{Equal, Different, NotOnCopy, SourceError, Inconclusive, Skipped} {
		if n := rep.Counts[v]; n > 0 {
			fmt.Fprintf(w, " %s %d", v, n)
		}
	}
	fmt.Fprintln(w)
	if rep.CopyDiffers > 0 {
		fmt.Fprintf(w, "%d statement(s) the router would send to the copy answer DIFFERENTLY from MySQL. Each is a veto to add (internal/readrouter) or a difference to document.\n", rep.CopyDiffers)
	} else {
		fmt.Fprintln(w, "No statement the router would send to the copy answers differently from MySQL.")
	}
	if rep.CopyOrderDiffers > 0 {
		fmt.Fprintf(w, "%d copy-routed statement(s) return the same rows in a different order: ties in the ORDER BY, or a collation difference in the sort key; check by hand.\n", rep.CopyOrderDiffers)
	}
	if rep.ExplainFailed > 0 {
		fmt.Fprintf(w, "%d statement(s) could not be routed (EXPLAIN failed on the source); the router would forward them, so the exit status does not cover their differences.\n", rep.ExplainFailed)
	}
	if rep.NothingCompared() {
		fmt.Fprintln(w, "NOTHING WAS COMPARED: no statement reached both sides. Check the copy (NOT_ON_COPY lines), the source (SOURCE_ERROR) and the statement file (SKIPPED).")
	}
	fmt.Fprintln(w, "Not modelled here: the copy's age (the port forwards when the snapshot is older than --route-max-copy-age), transactions and session SETs (the port forwards those too). A copy behind the source shows up as a difference.")
}

// WriteJSON renders the report as JSON.
func WriteJSON(w io.Writer, rep *Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}
