// Package status — this file reports which tables capture does NOT cover
// (#1802). A schema snapshot taken after capture started (the stream's DDL
// hook, the console's schema re-read, file-mode indexing) leaves out a table it
// cannot key: no primary key, or not InnoDB. The exclusion is recorded in
// snapshot_exclusions, and until #1802 the only other trace was one line in
// the capture log, so a table created after capture started stopped being
// captured with nothing on any screen saying so.
//
// The rules this file defends:
//
//   - Only the CURRENT snapshot counts: the newest snapshot_id, the one
//     metadata.NewResolver(db, 0) and the stream's startup load decode
//     against. Older snapshots' rows stay on disk, and a table that got its
//     key and was re-read must drop out of the list.
//   - Nothing unread is ever reported as "every table is captured". An index
//     without snapshot_exclusions is "not checked"; a read error is
//     "unavailable"; a PostgreSQL index (one snapshot per table, so its newest
//     snapshot names one table) is "not applicable".
//   - Every recorded reason is shown. A reason this build cannot classify is
//     shown verbatim, without a fix, rather than dropped.
//   - A reader whose data access withholds a table learns how many such tables
//     there are, never their names, and never their fixes (the fix names the
//     table).
package status

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// Table capture states: what LoadTableCapture could establish.
const (
	// TableCaptureChecked: the current snapshot and its exclusions were read.
	TableCaptureChecked = "checked"
	// TableCaptureNotChecked: the current snapshot was read, but the index has
	// no snapshot_exclusions table, so which tables were left out is unknown.
	TableCaptureNotChecked = "not_checked"
	// TableCaptureNoSnapshot: the index holds no schema snapshot yet.
	TableCaptureNoSnapshot = "no_snapshot"
	// TableCaptureNotApplicable: the current snapshot is a PostgreSQL one.
	TableCaptureNotApplicable = "not_applicable"
	// TableCaptureUnavailable: the index could not be read.
	TableCaptureUnavailable = "unavailable"
)

// Kinds of uncaptured table, from the reason the snapshot recorded.
const (
	UncapturedNoPrimaryKey          = "no_primary_key"
	UncapturedNotInnoDB             = "not_innodb"
	UncapturedNotInnoDBNoPrimaryKey = "not_innodb_no_primary_key"
	UncapturedOther                 = "other"
)

// TableRef names one table exactly as the snapshot recorded it.
type TableRef struct {
	Schema, Table string
}

// UncapturedTable is one table the current snapshot left out, and why.
type UncapturedTable struct {
	Schema, Table string
	// Reason is snapshot_exclusions.reason verbatim.
	Reason string
	// PKColumn is the column name the snapshot picked for the primary key it
	// would take to capture this table: free of the table's existing columns,
	// which the snapshot could see and no reader of the index can (an excluded
	// table has no columns in the snapshot). Empty when the row predates that
	// record, and then NO statement is printed rather than one that may fail
	// with ERROR 1060 on the commonest shape of a key-less table, a plain
	// non-key `id` column.
	PKColumn string
}

// TableCapture is what the current schema snapshot captures and leaves out.
type TableCapture struct {
	State      string
	SnapshotID int
	// CoverageKnown says a count may be claimed: the reader knows the scope
	// capture runs with. It is false until WithFilter says otherwise, because
	// the schema snapshot records the SCHEMAS capture reads but not the
	// per-table filter it may also run with, and a table left out by that
	// filter sits in the snapshot looking captured (the parser drops its
	// events with no trace at all). Counting from the snapshot alone would
	// then state full coverage over tables nobody is watching.
	CoverageKnown bool
	// Captured lists the snapshot's tables, exact names, sorted. Views are not
	// in it: they have columns in the snapshot but no rows to capture.
	Captured []TableRef
	// Uncaptured lists the current snapshot's exclusions, sorted by schema
	// then table. Always empty unless State is checked.
	Uncaptured []UncapturedTable
	// Err is why the state is unavailable.
	Err error
}

// TablesCaptured is how many tables capture covers.
func (c *TableCapture) TablesCaptured() int { return len(c.Captured) }

// TablesTotal is captured plus left out: the tables the snapshot looked at.
func (c *TableCapture) TablesTotal() int { return len(c.Captured) + len(c.Uncaptured) }

// LoadTableCapture reads the current schema snapshot's captured tables and
// exclusions. It never returns nil and never fails: a read error is the
// unavailable state, carrying the error, so no caller can mistake an index it
// could not read for one with nothing left out.
func LoadTableCapture(ctx context.Context, db *sql.DB) *TableCapture {
	unavailable := func(err error) *TableCapture {
		return &TableCapture{State: TableCaptureUnavailable, Err: err}
	}

	// The current snapshot is the newest snapshot_id (the group id, NOT the
	// auto-increment row id), the same one NewResolver(db, 0) loads.
	var snapshotID int
	switch err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(snapshot_id), 0) FROM schema_snapshots").Scan(&snapshotID); {
	case isMissingTableErr(err):
		return &TableCapture{State: TableCaptureNoSnapshot}
	case err != nil:
		return unavailable(fmt.Errorf("read the newest schema snapshot: %w", err))
	case snapshotID == 0:
		return &TableCapture{State: TableCaptureNoSnapshot}
	}

	// A PostgreSQL snapshot covers ONE relation (metadata.WritePGSnapshot), so
	// counting its tables would report "1 of 1" on a healthy install. Its
	// columns carry a type OID; a MySQL snapshot's never do. An index older
	// than the column is a MySQL index.
	var pgColumns int
	switch err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM schema_snapshots WHERE snapshot_id = ? AND pg_type_oid IS NOT NULL", snapshotID).Scan(&pgColumns); {
	case isUnknownColumnErr(err):
	case err != nil:
		return unavailable(fmt.Errorf("read schema snapshot %d: %w", snapshotID, err))
	case pgColumns > 0:
		return &TableCapture{State: TableCaptureNotApplicable, SnapshotID: snapshotID}
	}

	captured, err := loadCapturedTables(ctx, db, snapshotID)
	if err != nil {
		return unavailable(err)
	}
	c := &TableCapture{State: TableCaptureChecked, SnapshotID: snapshotID, Captured: captured}

	// pk_column (#1802) post-dates the table, and the console never migrates a
	// registry server's index, so an index that has the table without the
	// column is an ordinary shape — not an error. Fall back to the columns
	// that have always been there; the report then prints no statement for a
	// table that needs a key, rather than guessing a column name.
	rows, err := db.QueryContext(ctx,
		"SELECT schema_name, table_name, reason, pk_column FROM snapshot_exclusions WHERE snapshot_id = ?", snapshotID)
	withPKColumn := true
	if isUnknownColumnErr(err) {
		withPKColumn = false
		rows, err = db.QueryContext(ctx,
			"SELECT schema_name, table_name, reason FROM snapshot_exclusions WHERE snapshot_id = ?", snapshotID)
	}
	if isMissingTableErr(err) {
		// An index last written before #1051. Its snapshots may well have
		// left nothing out, but nothing here can say so.
		c.State = TableCaptureNotChecked
		return c
	}
	if err != nil {
		return unavailable(fmt.Errorf("read the tables schema snapshot %d left out: %w", snapshotID, err))
	}
	defer rows.Close()
	for rows.Next() {
		var (
			u        UncapturedTable
			pkColumn sql.NullString
			scanErr  error
		)
		if withPKColumn {
			scanErr = rows.Scan(&u.Schema, &u.Table, &u.Reason, &pkColumn)
		} else {
			scanErr = rows.Scan(&u.Schema, &u.Table, &u.Reason)
		}
		if scanErr != nil {
			return unavailable(fmt.Errorf("read the tables schema snapshot %d left out: %w", snapshotID, scanErr))
		}
		u.PKColumn = pkColumn.String
		c.Uncaptured = append(c.Uncaptured, u)
	}
	if err := rows.Err(); err != nil {
		return unavailable(fmt.Errorf("read the tables schema snapshot %d left out: %w", snapshotID, err))
	}
	sort.Slice(c.Uncaptured, func(i, j int) bool {
		a, b := c.Uncaptured[i], c.Uncaptured[j]
		if a.Schema != b.Schema {
			return a.Schema < b.Schema
		}
		return a.Table < b.Table
	})
	return c
}

// loadCapturedTables lists the snapshot's tables that have a primary-key
// column. Every base table a MySQL snapshot keeps has one (validation drops
// the rest into snapshot_exclusions), and a view never does, so this is the
// set capture decodes rows for.
//
// The names are deduplicated HERE, byte for byte, not with SQL DISTINCT: the
// index database collates case-insensitively, and on a case-sensitive source
// `Orders` and `orders` are two tables that a DISTINCT would count as one.
func loadCapturedTables(ctx context.Context, db *sql.DB, snapshotID int) ([]TableRef, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT schema_name, table_name FROM schema_snapshots WHERE snapshot_id = ? AND column_key = 'PRI'", snapshotID)
	if err != nil {
		return nil, fmt.Errorf("read the tables in schema snapshot %d: %w", snapshotID, err)
	}
	defer rows.Close()
	seen := make(map[TableRef]bool)
	var out []TableRef
	for rows.Next() {
		var t TableRef
		if err := rows.Scan(&t.Schema, &t.Table); err != nil {
			return nil, fmt.Errorf("read the tables in schema snapshot %d: %w", snapshotID, err)
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the tables in schema snapshot %d: %w", snapshotID, err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Table < out[j].Table
	})
	return out, nil
}

// CaptureFilter is the scope capture runs with, as the process that started
// it knows it. Known is the load-bearing field: a reader that did not start
// capture (`bintrail status` against an index, a read-only console) cannot
// know the per-table half, and nothing records it, so it must claim no
// coverage at all rather than count the snapshot and call it coverage.
type CaptureFilter struct {
	Known bool
	// Schemas is the --schemas list; empty means every schema.
	Schemas []string
	// Tables is the --tables list, each "schema.table"; empty means every
	// table of Schemas.
	Tables []string
}

// WithFilter narrows the capture to the scope capture actually runs with and
// answers, ONCE, the question every surface then asks: may a count be
// claimed? Nothing downstream decides this again — the renderers read
// CoverageKnown and the View fields, and no surface re-derives the rule.
//
// A table the operator's own filter leaves out is a DELIBERATE choice, not a
// broken table: it leaves BOTH lists, exactly as a table in a schema outside
// the filter does, so it is never drawn as something to fix. A server whose
// filter changed after its last schema read still has the old scope in that
// snapshot, which is the other reason this narrowing exists.
//
// Names compare case-insensitively for the LIST. A case-insensitive MySQL
// stores them lowercased while the filter keeps what the operator typed, and
// an exact match would hide a table that is not captured; showing more is the
// safe direction for a list whose job is to not hide anything. The COUNT is
// stricter — see describesCapture.
func (c *TableCapture) WithFilter(f CaptureFilter) *TableCapture {
	if c == nil {
		return nil
	}
	out := *c
	out.CoverageKnown = false
	if c.State != TableCaptureChecked && c.State != TableCaptureNotChecked {
		// Nothing was counted in the first place (no snapshot, a PostgreSQL
		// index, a read that failed).
		return &out
	}
	schemas := nonBlank(f.Schemas)
	tables := nonBlank(f.Tables)
	if len(schemas) > 0 || len(tables) > 0 {
		in := func(schema, table string) bool {
			if len(schemas) > 0 && !containsFold(schemas, schema) {
				return false
			}
			return len(tables) == 0 || containsFold(tables, schema+"."+table)
		}
		out.Captured, out.Uncaptured = nil, nil
		for _, t := range c.Captured {
			if in(t.Schema, t.Table) {
				out.Captured = append(out.Captured, t)
			}
		}
		for _, u := range c.Uncaptured {
			if in(u.Schema, u.Table) {
				out.Uncaptured = append(out.Uncaptured, u)
			}
		}
	}
	out.CoverageKnown = f.Known && c.describesCapture(schemas, tables)
	return &out
}

// describesCapture reports whether a filter names what capture actually
// watches. Capture matches BOTH halves of its filter BYTE FOR BYTE
// (event.Filters.Matches, a map built from the raw flag strings), while this
// file folds case for the list, so the two can disagree in a way that costs
// everything: an entry that differs only in case drops EVERY event of that
// table or schema, with no counter, no log line and nothing in capture_skips,
// while the fold still reports it as captured.
//
// So an entry matching no snapshot name exactly withdraws the count — it is
// either mis-cased or names something the snapshot does not have, and either
// way the filter does not describe what is being watched. The same rule
// retires the "Capturing 0 of 0 tables" a filter matching nothing used to
// produce.
//
// The schema half is NOT covered by the snapshot refusing a mis-cased schema.
// That is true only at lower_case_table_names=0 (MySQL 8.4.9: the snapshot
// fails with "no columns found"). At lower_case_table_names=1 the server
// stores `CREATE DATABASE bt_Mixed` as `bt_mixed` while information_schema
// still answers a query for 'BT_MIXED', so the snapshot SUCCEEDS and records
// the lowercase name, the binlog events carry the lowercase name, and the
// operator's mis-cased --schemas entry matches neither. Verified against a
// real 8.4.9 started with --lower-case-table-names=1: snapshot ok, two
// tables, and Filters.Matches("bt_mixed", "orders") false.
func (c *TableCapture) describesCapture(schemas, tables []string) bool {
	knownSchemas := make(map[string]bool, len(c.Captured))
	knownTables := make(map[string]bool, len(c.Captured)+len(c.Uncaptured))
	note := func(schema, table string) {
		knownSchemas[schema] = true
		knownTables[schema+"."+table] = true
	}
	for _, t := range c.Captured {
		note(t.Schema, t.Table)
	}
	for _, u := range c.Uncaptured {
		note(u.Schema, u.Table)
	}
	for _, name := range schemas {
		if !knownSchemas[name] {
			return false
		}
	}
	for _, name := range tables {
		if !knownTables[name] {
			return false
		}
	}
	return true
}

// nonBlank drops the empty entries a comma-separated flag leaves behind.
func nonBlank(in []string) []string {
	var out []string
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// containsFold matches with the rule MySQL itself uses for identifiers, not
// Unicode's: under plain EqualFold a filter typed `shop.INVOICES` against a
// real `shop.ınvoices` matched nothing, so a table that IS not captured left
// the list — this card's own failure mode, in miniature. The count is a
// separate, stricter question (describesCapture).
func containsFold(list []string, want string) bool {
	for _, s := range list {
		if metadata.IdentEqualFold(s, want) {
			return true
		}
	}
	return false
}

// reasonParts splits a recorded reason into the known parts it names. ok is
// false when any part is one this build does not know, or nothing was
// recorded: such a reason is shown verbatim and gets no fix, because a fix for
// only the part we recognize would leave the table uncaptured after it ran.
func reasonParts(reason string) (notInnoDB, noPK, ok bool) {
	parts := strings.Split(reason, strings.TrimSpace(metadata.ExclusionReasonSeparator))
	for _, p := range parts {
		switch p = strings.TrimSpace(p); {
		case strings.EqualFold(p, metadata.ExclusionReasonNotInnoDB):
			notInnoDB = true
		case strings.EqualFold(p, metadata.ExclusionReasonNoPrimaryKey):
			noPK = true
		default:
			return false, false, false
		}
	}
	return notInnoDB, noPK, notInnoDB || noPK
}

// Kind classifies the recorded reason.
func (u UncapturedTable) Kind() string {
	notInnoDB, noPK, ok := reasonParts(u.Reason)
	switch {
	case !ok:
		return UncapturedOther
	case notInnoDB && noPK:
		return UncapturedNotInnoDBNoPrimaryKey
	case notInnoDB:
		return UncapturedNotInnoDB
	default:
		return UncapturedNoPrimaryKey
	}
}

// Name is the table as the snapshot recorded it, schema first. For reading,
// not for running: FixSQL quotes it.
func (u UncapturedTable) Name() string { return u.Schema + "." + u.Table }

// Summary is the one sentence every surface shows for this table.
func (u UncapturedTable) Summary() string {
	switch u.Kind() {
	case UncapturedNoPrimaryKey:
		return u.Name() + " is not captured: no primary key."
	case UncapturedNotInnoDB:
		return u.Name() + " is not captured: not an InnoDB table."
	case UncapturedNotInnoDBNoPrimaryKey:
		return u.Name() + " is not captured: not an InnoDB table, and no primary key."
	}
	if strings.TrimSpace(u.Reason) == "" {
		return u.Name() + " is not captured, and no reason was recorded."
	}
	return u.Name() + " is not captured: \"" + u.Reason + "\"."
}

// FixIntro says what happens to the table's changes until the fix runs, and
// what running it costs, before the statement is shown: the ALTER rebuilds
// the table, and MySQL does not let writes to it through while it does. Empty
// when there is no fix.
func (u UncapturedTable) FixIntro() string {
	const cost = " Run this on your MySQL. On a large table it takes a while, and writes to the table wait until it is done."
	// A key is needed and no free column name was recorded: there is no
	// statement, so the intro carries the step that produces one instead of
	// leaving the table named with nothing to do about it.
	const noStatement = " DBTrail cannot write the statement for it: read this server's tables again, and it will."
	switch u.Kind() {
	case UncapturedNoPrimaryKey:
		if u.PKColumn == "" {
			return "Its changes are not kept until it has a primary key." + noStatement
		}
		return "Its changes are not kept until it has a primary key." + cost
	case UncapturedNotInnoDB:
		return "Its changes are not kept until it is an InnoDB table." + cost
	case UncapturedNotInnoDBNoPrimaryKey:
		if u.PKColumn == "" {
			return "Its changes are not kept until it is an InnoDB table with a primary key." + noStatement
		}
		return "Its changes are not kept until it is an InnoDB table with a primary key." + cost
	}
	return ""
}

// addPrimaryKey adds the surrogate key doctor's remediation also names: a new
// first column, since a table with no key has no unique NOT NULL column to
// promote either (MySQL reports one as PRI, and such a table would not be
// excluded at all). The NAME is the snapshot's, not a guess: the writer saw
// the table's columns and picked one they do not already use.
func addPrimaryKey(column string) string {
	return "ADD COLUMN " + QuoteIdentifier(column) + " BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST"
}

// FixSQL is the statement that makes the table capturable, with the real
// names quoted. Empty for a reason this build does not know, and for a table
// that needs a key with no column name recorded: a guessed `id` fails with
// ERROR 1060 on a table that already has a non-key `id` column, which is the
// commonest shape of a key-less table, and a statement that fails is worse
// than none (FixIntro then carries the step that produces a real one).
func (u UncapturedTable) FixSQL() string {
	target := "ALTER TABLE " + QuoteIdentifier(u.Schema) + "." + QuoteIdentifier(u.Table) + " "
	switch u.Kind() {
	case UncapturedNoPrimaryKey:
		if u.PKColumn == "" {
			return ""
		}
		return target + addPrimaryKey(u.PKColumn) + ";"
	case UncapturedNotInnoDB:
		return target + "ENGINE=InnoDB;"
	case UncapturedNotInnoDBNoPrimaryKey:
		if u.PKColumn == "" {
			return ""
		}
		return target + "ENGINE=InnoDB, " + addPrimaryKey(u.PKColumn) + ";"
	}
	return ""
}

// QuoteIdentifier quotes a MySQL identifier with backticks, doubling any
// backtick inside it. Always quoted: an unquoted name is a syntax error for a
// reserved word (a table named `order`) or a name with a space, and deciding
// when quoting is "needed" would be a second opinion on MySQL's grammar. Case
// is kept as recorded, since on a case-sensitive server it names a different
// table.
func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// TableCaptureView is the wire shape: `bintrail status --format json` under
// table_capture, and the console's GET /api/uncaptured-tables.
type TableCaptureView struct {
	State      string `json:"state"`
	SnapshotID int    `json:"snapshot_id,omitempty"`
	// TablesCaptured is set whenever a snapshot was read (checked and
	// not_checked). TablesTotal only when checked: without the exclusions
	// the total is not known, and a total equal to the captured count would
	// read as "nothing left out".
	TablesCaptured *int `json:"tables_captured,omitempty"`
	TablesTotal    *int `json:"tables_total,omitempty"`
	// Headline states the counts in words: "Capturing 14 of 15 tables." when
	// checked and countable, "Capturing 14 tables." when not checked, empty
	// when nothing may be counted.
	Headline string `json:"headline,omitempty"`
	// CoverageNote replaces the headline when no count may be claimed: it
	// says WHY, so an absent count never reads as a missing feature. Empty
	// whenever a headline is shown.
	CoverageNote string `json:"coverage_note,omitempty"`
	// AllCapturedNote is set only when a count was claimed AND the list is
	// complete and empty. The bare count leaves "and nothing else?" hanging,
	// and the terminal already answered it; both surfaces read this field, so
	// they cannot answer it differently.
	AllCapturedNote string                `json:"all_captured_note,omitempty"`
	Uncaptured      []UncapturedTableView `json:"uncaptured"`
	// UncapturedWithheld counts uncaptured tables the reader may not see:
	// they are in TablesTotal, and named nowhere in this view.
	// WithheldSummary says so in a sentence.
	UncapturedWithheld int    `json:"uncaptured_withheld,omitempty"`
	WithheldSummary    string `json:"withheld_summary,omitempty"`
	// UncapturedOmitted counts tables left out of the LIST by the cap, which
	// is a different thing from a withheld one: nobody is hiding them, the
	// payload is bounded (the console's other results are capped the same
	// way). They are in TablesTotal and OmittedSummary says how many.
	UncapturedOmitted int    `json:"uncaptured_omitted,omitempty"`
	OmittedSummary    string `json:"omitted_summary,omitempty"`
	Error             string `json:"error,omitempty"`
}

// MaxUncapturedListed bounds the list one response carries. A schema of
// key-less tables would otherwise ship thousands of entries, each with its own
// statement, into a page that can only show a handful; the counts above still
// cover every one of them.
const MaxUncapturedListed = 100

// UncapturedTableView is one uncaptured table on the wire. The sentences come
// from here rather than being written again in the console's JavaScript, so
// the terminal and the page cannot tell two stories about one table.
type UncapturedTableView struct {
	Schema   string `json:"schema"`
	Table    string `json:"table"`
	Reason   string `json:"reason"`
	Kind     string `json:"kind"`
	Summary  string `json:"summary"`
	FixIntro string `json:"fix_intro,omitempty"`
	FixSQL   string `json:"fix_sql,omitempty"`
	// Decision is empty today. It is where "Leave it out" (#1805) records that
	// someone decided, which turns the entry from red to gray; the console
	// already reads it for the tone.
	Decision string `json:"decision,omitempty"`
}

// View renders c for the wire, withholding every uncaptured table visible
// refuses (nil: withhold nothing). Counts stay whole; a count is not a name.
// A nil capture renders as unavailable.
func (c *TableCapture) View(visible func(schema, table string) bool) TableCaptureView {
	if c == nil {
		return TableCaptureView{State: TableCaptureUnavailable, Uncaptured: []UncapturedTableView{}}
	}
	v := TableCaptureView{State: c.State, SnapshotID: c.SnapshotID, Uncaptured: []UncapturedTableView{}}
	if c.Err != nil {
		v.Error = c.Err.Error()
	}
	switch {
	case c.State != TableCaptureChecked && c.State != TableCaptureNotChecked:
		return v
	case c.CoverageKnown && c.State == TableCaptureChecked:
		captured, total := c.TablesCaptured(), c.TablesTotal()
		v.TablesCaptured, v.TablesTotal = &captured, &total
		v.Headline = fmt.Sprintf("Capturing %d of %d %s.", captured, total, tablesWord(total))
	case c.CoverageKnown:
		// not_checked: what capture watches is known, what was left out is
		// not, so there is a count and deliberately no total — "of" would
		// read as "and nothing else was left out".
		captured := c.TablesCaptured()
		v.TablesCaptured = &captured
		v.Headline = fmt.Sprintf("Capturing %d %s.", captured, tablesWord(captured))
	default:
		// No count may be claimed. The note carries ONLY what is knowable,
		// and the two claims stay apart: what the last schema read left out
		// (this index's own record) and how many tables capture watches (set
		// where capture runs, recorded nowhere). Welding them put a finding
		// on every healthy index.
		v.CoverageNote = c.uncountedNote()
	}
	for _, u := range c.Uncaptured {
		if visible != nil && !visible(u.Schema, u.Table) {
			v.UncapturedWithheld++
			continue
		}
		if len(v.Uncaptured) >= MaxUncapturedListed {
			v.UncapturedOmitted++
			continue
		}
		v.Uncaptured = append(v.Uncaptured, UncapturedTableView{
			Schema: u.Schema, Table: u.Table, Reason: u.Reason, Kind: u.Kind(),
			Summary: u.Summary(), FixIntro: u.FixIntro(), FixSQL: u.FixSQL(),
		})
	}
	if v.UncapturedWithheld > 0 {
		v.WithheldSummary = withheldSentence(v.UncapturedWithheld)
	}
	if v.UncapturedOmitted > 0 {
		v.OmittedSummary = fmt.Sprintf("%d more %s not captured, not listed here.", v.UncapturedOmitted,
			map[bool]string{true: "table is", false: "tables are"}[v.UncapturedOmitted == 1])
	}
	// Only a CHECKED snapshot may say this: on a not-checked one, whether
	// anything was left out is precisely what is unknown, so an empty list is
	// not an empty answer.
	if c.State == TableCaptureChecked && v.Headline != "" &&
		len(v.Uncaptured) == 0 && v.UncapturedWithheld == 0 && v.UncapturedOmitted == 0 {
		v.AllCapturedNote = "Every table in that scope is captured."
	}
	return v
}

// uncountedWhy is the half that is the same everywhere: no count, and why.
// The wording avoids "watch" on purpose: the first-run walk's closed word
// list (test/console-e2e/first_run_scoreboard.mjs) bans it, along with index,
// source, baseline, backup, daemon, monitor, stream and the rest, and this
// card renders on a first-run screen. Checked against that rule itself, not
// by eye.
const uncountedWhy = "How many tables are captured is set where capture runs, and is not kept here, so nothing here counts them."

// uncountedNote says what is knowable when no count may be claimed, and never
// more. The two claims stay apart: with nothing left out it says THAT, which
// this index does record; on a not-checked index it says neither, because
// whether anything was left out is precisely what that state does not know
// (the state's own sentence follows it).
func (c *TableCapture) uncountedNote() string {
	switch {
	case c.State == TableCaptureNotChecked:
		return uncountedWhy
	case len(c.Uncaptured) > 0:
		return "These tables are left out of what DBTrail reads. " + uncountedWhy
	default:
		return "The last schema read left no table out. " + uncountedWhy
	}
}

// tablesWord agrees with the number in front of it: "1 of 1 table" is the
// one case where the total is singular.
func tablesWord(n int) string {
	if n == 1 {
		return "table"
	}
	return "tables"
}

// withheldSentence counts uncaptured tables a reader may not see, in the
// words the capture-skip explanation uses for the same situation.
func withheldSentence(n int) string {
	if n == 1 {
		return "1 table outside your access is not captured."
	}
	return fmt.Sprintf("%d tables outside your access are not captured.", n)
}

// writeTableCapture prints the text report's section. nil prints nothing: the
// caller did not load it.
func writeTableCapture(w io.Writer, c *TableCapture, visible func(schema, table string) bool) {
	if c == nil {
		return
	}
	v := c.View(visible)
	switch v.State {
	case TableCaptureChecked, TableCaptureNotChecked, TableCaptureUnavailable:
	default:
		// Nothing to report, and the console's card draws nothing for these
		// either: no schema read yet (the first-run list covers that), a
		// PostgreSQL index (its capture reads what its publication names), or
		// a state this build does not know. The two surfaces agree.
		return
	}
	fmt.Fprintln(w, "=== Tables captured ===")
	switch v.State {
	case TableCaptureUnavailable:
		msg := "unknown error"
		if v.Error != "" {
			msg = v.Error
		}
		fmt.Fprintf(w, "  Which tables are captured could not be read: %s\n", msg)
	default:
		if v.Headline != "" {
			fmt.Fprintf(w, "  %s Schema snapshot %d.\n", v.Headline, v.SnapshotID)
		} else {
			// No count may be claimed: print what IS known, never a total
			// nobody could verify.
			fmt.Fprintf(w, "  Schema snapshot %d.\n", v.SnapshotID)
			for _, line := range wrapAt(v.CoverageNote, 72) {
				fmt.Fprintf(w, "  %s\n", line)
			}
		}
		if v.State == TableCaptureNotChecked {
			// NOT "an older version": a current build that took this snapshot
			// without migrating the index leaves the same shape, and the
			// console never migrates a registry server's index.
			fmt.Fprintln(w, "  Whether any table was left out is not known:")
			fmt.Fprintln(w, "  this index has not recorded it (no snapshot_exclusions table).")
			fmt.Fprintln(w, "  The first schema read that leaves one out records it, and this")
			fmt.Fprintln(w, "  section names it from then on.")
			break
		}
		for _, u := range v.Uncaptured {
			fmt.Fprintf(w, "  %s\n", u.Summary)
			if u.FixIntro != "" {
				for _, line := range wrapAt(u.FixIntro, 72) {
					fmt.Fprintf(w, "    %s\n", line)
				}
			}
			if u.FixSQL != "" {
				fmt.Fprintf(w, "      %s\n", u.FixSQL)
			}
		}
		if v.OmittedSummary != "" {
			fmt.Fprintf(w, "  %s\n", v.OmittedSummary)
		}
		if v.WithheldSummary != "" {
			fmt.Fprintf(w, "  %s\n", v.WithheldSummary)
		}
		if v.AllCapturedNote != "" {
			fmt.Fprintf(w, "  %s\n", v.AllCapturedNote)
		}
	}
	fmt.Fprintln(w)
}
