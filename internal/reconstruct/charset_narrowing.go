package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/latin1"
	"github.com/dbtrail/dbtrail/internal/query"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// A narrowing ALTER (#2735) is an ALTER TABLE that left a text column with a
// character set that cannot hold every character of the one it had: utf8mb4
// to latin1, utf8mb4 to utf8mb3. Where the server lets it through (a
// non-strict sql_mode, MariaDB's ALTER IGNORE) it writes '?' in place of every
// character the new set cannot hold, and no row event. A fold that carries a
// value from before that ALTER into its output gives text the table no longer
// has.
//
// Whether a value lost letters is decided by the value, not by the statement.
// Measured on MySQL 8.0.46 and MariaDB 11.4: under a strict sql_mode both
// MODIFY ... CHARACTER SET and CONVERT TO CHARACTER SET refuse with error 1366
// when a value has a character the new set cannot hold, so an ALTER that ran
// lost letters exactly when such a value existed, whatever its sql_mode. The
// index does not record the statement's sql_mode, and would not need to.
//
// So a reader that holds the values it outputs (the full-table fold, the
// shim's _snapshot) checks each one written before the ALTER, and refuses
// only on a value that really lost a character. A widening ALTER (latin1 to
// utf8mb4) and a narrowing one over values the new set holds change nothing
// there and refuse nothing: refusing them would only force a full read.

// charsetNarrowing is one ALTER TABLE inside the window that left column with
// the character set To, which may not hold every character of From.
type charsetNarrowing struct {
	Column   string // as the schema snapshot names it
	From, To string // baseline.ComparableCharset spelling; From is "" when not known
	DDL      destructiveDDL
}

// CharsetNarrowings is what FindCharsetNarrowings found for one table: the
// narrowing ALTERs whose values can be checked one by one, and a note for
// each one that cannot.
type CharsetNarrowings struct {
	schema, table string
	checked       []charsetNarrowing
	notes         []string
}

// Checked reports whether there is at least one narrowing CheckRow checks.
func (n *CharsetNarrowings) Checked() bool { return n != nil && len(n.checked) > 0 }

// Notes are the narrowings this build cannot check value by value, each said
// in words a reader prints as a warning: the result may keep text the source
// replaced with '?'.
func (n *CharsetNarrowings) Notes() []string {
	if n == nil {
		return nil
	}
	return n.notes
}

// fullUnicodeCharsets hold every character, so nothing narrows to them.
var fullUnicodeCharsets = map[string]bool{
	"utf8mb4": true, "utf16": true, "utf16le": true, "utf32": true, "gb18030": true,
}

// bmpCharsets hold every character of the Basic Multilingual Plane and
// nothing past it: utf8mb3 is three bytes at most, ucs2 two.
var bmpCharsets = map[string]bool{"utf8mb3": true, "ucs2": true}

// utf8ValueCharsets are the character sets whose values reach the index and
// the copy as UTF-8 text, so their characters can be read off the value:
// UTF-8 itself, and latin1, decoded on capture and in the copy (#2634).
// Another set's values may arrive as their stored bytes.
var utf8ValueCharsets = map[string]bool{"utf8mb4": true, "utf8mb3": true, "latin1": true, "ascii": true}

// charsetHolds reports whether the character set cs has every character of
// s. known is false when this build does not know cs's characters.
func charsetHolds(cs, s string) (holds, known bool) {
	switch {
	case fullUnicodeCharsets[cs]:
		return true, true
	case bmpCharsets[cs]:
		for _, r := range s {
			if r > 0xFFFF {
				return false, true
			}
		}
		return true, true
	case cs == "latin1":
		for _, r := range s {
			if !latin1.Holds(r) {
				return false, true
			}
		}
		return true, true
	case cs == "ascii":
		return isASCII(s), true
	}
	return false, false
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// knownCharset reports whether charsetHolds can answer for cs.
func knownCharset(cs string) bool {
	_, known := charsetHolds(cs, "")
	return known
}

// Kinds of character set change, for one column of one ALTER.
const (
	charsetKept        = iota // nothing a value had can be lost
	charsetChecked            // CheckRow reads each value
	charsetUncheckable        // a value may have lost letters, and this build cannot tell
)

// charsetChangeKind classifies a column's change from one character set to
// another. from is "" when the character set before the ALTER is not known:
// the value check does not need it, because a value the new set cannot hold
// lost letters whatever set held it before.
func charsetChangeKind(from, to string) int {
	switch {
	case to == "" || to == from || fullUnicodeCharsets[to]:
		return charsetKept
	case from == "ascii" && knownCharset(to),
		from == "latin1" && bmpCharsets[to],
		bmpCharsets[from] && bmpCharsets[to]:
		return charsetKept
	case !knownCharset(to):
		return charsetUncheckable
	case from != "" && !utf8ValueCharsets[from]:
		return charsetUncheckable
	}
	return charsetChecked
}

// charsetTextTypes are the DATA_TYPEs whose values a character set applies to.
var charsetTextTypes = map[string]bool{
	"char": true, "varchar": true, "tinytext": true, "text": true, "mediumtext": true,
	"longtext": true, "enum": true, "set": true,
}

// FindCharsetNarrowings reads the ALTER TABLE statements schema_changes
// records for schema.table inside the window (DDLWindow.place, the rule
// CheckDestructiveDDL places statements by), and the character set each left
// on every text column: the one in the schema snapshot capture took for the
// statement, against the one in the snapshot before it. A table with none
// returns an empty result, as does an index with no schema_changes table or
// no character sets recorded (one written before #756): nothing to check, as
// CheckDestructiveDDL answers there.
//
// The window's mark is not used: it can only leave a statement out, and a
// statement counted here once too often only makes values be checked.
//
// What this cannot see is said, not guessed (Notes): an ALTER whose schema
// snapshot was not recorded and whose text may change a character set, and a
// change to or from a character set charsetHolds does not know. A snapshot
// taken late, after a second ALTER had run, shows the second one's set; a
// change and its undo inside that delay are not seen.
func FindCharsetNarrowings(ctx context.Context, db *sql.DB, schema, table string, w DDLWindow) (*CharsetNarrowings, error) {
	out := &CharsetNarrowings{schema: schema, table: table}
	w.Mark = nil
	alters, err := loadAlterRows(ctx, db, schema, table, w)
	if err != nil {
		if isMissingTableOrColumn(err) {
			return out, nil
		}
		return nil, fmt.Errorf("check schema_changes for a character set change on %s.%s: %w", schema, table, err)
	}
	// noCharsets is an index whose schema snapshots record no character set
	// (written before #756): every statement is then one with no snapshot.
	noCharsets := false
	for _, a := range alters {
		if w.place(a.ddl) == ddlOutside {
			continue
		}
		if !a.snapshotID.Valid || noCharsets {
			if (a.recordedAgain && !noCharsets) || !mayChangeCharset(a.text) {
				continue
			}
			out.notes = append(out.notes, fmt.Sprintf(
				"the ALTER TABLE on %s.%s %s has no schema snapshot with character sets recorded, so whether it narrowed a column's character set "+
					"cannot be checked; if it did, a character the new set cannot hold is '?' in the source and not in this result",
				schema, table, a.ddl.where()))
			continue
		}
		after, err := snapshotCharsets(ctx, db, schema, table, int(a.snapshotID.Int64))
		if err != nil {
			if isMissingTableOrColumn(err) {
				noCharsets = true
				if mayChangeCharset(a.text) {
					out.notes = append(out.notes, fmt.Sprintf(
						"the ALTER TABLE on %s.%s %s has no schema snapshot with character sets recorded, so whether it narrowed a column's character set "+
							"cannot be checked; if it did, a character the new set cannot hold is '?' in the source and not in this result",
						schema, table, a.ddl.where()))
				}
				continue
			}
			return nil, fmt.Errorf("read the character sets of %s.%s after the ALTER TABLE %s: %w", schema, table, a.ddl.where(), err)
		}
		before, err := previousSnapshotCharsets(ctx, db, schema, table, int(a.snapshotID.Int64))
		if err != nil {
			return nil, fmt.Errorf("read the character sets of %s.%s before the ALTER TABLE %s: %w", schema, table, a.ddl.where(), err)
		}
		for _, c := range after {
			from := before[strings.ToLower(c.name)]
			switch charsetChangeKind(from, c.charset) {
			case charsetChecked:
				out.checked = append(out.checked, charsetNarrowing{Column: c.name, From: from, To: c.charset, DDL: a.ddl})
			case charsetUncheckable:
				was := from
				if was == "" {
					was = "a character set not recorded"
				}
				out.notes = append(out.notes, fmt.Sprintf(
					"the ALTER TABLE on %s.%s %s changed column %s from %s to %s, which this build cannot check value by value; "+
						"a character %s cannot hold in a value written before it is '?' in the source and not in this result",
					schema, table, a.ddl.where(), c.name, was, c.charset, c.charset))
			}
		}
	}
	return out, nil
}

// alterRow is one ALTER TABLE row of schema_changes.
type alterRow struct {
	ddl        destructiveDDL
	snapshotID sql.NullInt64
	text       string
	// recordedAgain marks a row without a snapshot whose statement has
	// another row, at the same position, that has one: capture records the
	// attempt when a snapshot fails and records it again on the retry.
	recordedAgain bool
}

func loadAlterRows(ctx context.Context, db *sql.DB, schema, table string, w DDLWindow) ([]alterRow, error) {
	sinceOp := ">"
	if w.sinceInclusive {
		sinceOp = ">="
	}
	// schema_name = '' for rows indexed before #1435, as in
	// loadDestructiveDDLs. Ordered by id: the order capture recorded them in,
	// which is the binary log's.
	rows, err := db.QueryContext(ctx, `SELECT id, ddl_type, detected_at, binlog_file, binlog_pos,
			detected_at `+sinceOp+` ?, detected_at <= ?, snapshot_id, LEFT(ddl_query, 4000)
		FROM schema_changes
		WHERE (schema_name = ? OR schema_name = '') AND table_name = ? AND ddl_type = 'ALTER TABLE'
		ORDER BY id`, w.Since, w.Until, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []alterRow
	withSnapshot := map[string]bool{}
	for rows.Next() {
		var a alterRow
		if err := rows.Scan(&a.ddl.ID, &a.ddl.Type, &a.ddl.DetectedAt, &a.ddl.File, &a.ddl.Pos,
			&a.ddl.AfterSince, &a.ddl.AtOrBeforeUntil, &a.snapshotID, &a.text); err != nil {
			return nil, err
		}
		if a.snapshotID.Valid {
			withSnapshot[a.ddl.positionKey()] = true
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if !out[i].snapshotID.Valid && out[i].ddl.hasPos() && withSnapshot[out[i].ddl.positionKey()] {
			out[i].recordedAgain = true
		}
	}
	return out, nil
}

// mayChangeCharset reports whether an ALTER TABLE's text could change a
// column's character set: by naming one or a collation, by converting the
// table, or by redefining a column, which gives it the table's default when
// it names none. Read only for a statement whose snapshot was not recorded,
// to decide whether that is worth a warning; a word in a comment or a name
// gives a warning too many, never one too few.
func mayChangeCharset(text string) bool {
	t := strings.ToLower(text)
	for _, w := range []string{"character", "charset", "collate", "convert", "modify", "change"} {
		if strings.Contains(t, w) {
			return true
		}
	}
	return false
}

type columnCharset struct{ name, charset string }

// snapshotCharsets is the character set of every text column of schema.table
// in schema snapshot id, in the snapshot's column order.
func snapshotCharsets(ctx context.Context, db *sql.DB, schema, table string, id int) ([]columnCharset, error) {
	rows, err := db.QueryContext(ctx, `SELECT column_name, data_type, COALESCE(character_set_name, '')
		FROM schema_snapshots WHERE schema_name = ? AND table_name = ? AND snapshot_id = ?
		ORDER BY ordinal_position`, schema, table, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []columnCharset
	for rows.Next() {
		var name, dataType, cs string
		if err := rows.Scan(&name, &dataType, &cs); err != nil {
			return nil, err
		}
		if !charsetTextTypes[strings.ToLower(dataType)] {
			continue
		}
		out = append(out, columnCharset{name: name, charset: baseline.ComparableCharset(cs)})
	}
	return out, rows.Err()
}

// previousSnapshotCharsets is snapshotCharsets of the newest snapshot before
// id that describes the table, by lower-cased column name; empty when there
// is none.
func previousSnapshotCharsets(ctx context.Context, db *sql.DB, schema, table string, id int) (map[string]string, error) {
	var prev sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(snapshot_id) FROM schema_snapshots
		WHERE schema_name = ? AND table_name = ? AND snapshot_id < ?`, schema, table, id).Scan(&prev); err != nil {
		return nil, err
	}
	out := map[string]string{}
	if !prev.Valid {
		return out, nil
	}
	cols, err := snapshotCharsets(ctx, db, schema, table, int(prev.Int64))
	if err != nil {
		return nil, err
	}
	for _, c := range cols {
		out[strings.ToLower(c.name)] = c.charset
	}
	return out, nil
}

// isMissingTableOrColumn is an index too old to answer: no schema_changes or
// schema_snapshots table (1146), or no character_set_name column (1054, a
// snapshot table from before #756). Graded on the error number, as
// loadDestructiveDDLs does.
func isMissingTableOrColumn(err error) bool {
	var me *mysqldriver.MySQLError
	return errors.As(err, &me) && (me.Number == 1146 || me.Number == 1054)
}

// where names a statement's place in the binary log and in schema_changes.
func (d destructiveDDL) where() string {
	s := "run at " + d.DetectedAt.UTC().Format("2006-01-02T15:04:05Z") + " ("
	if d.hasPos() {
		s += fmt.Sprintf("recorded at %s:%d, ", d.File, d.Pos)
	}
	return s + fmt.Sprintf("row %d of schema_changes)", d.ID)
}

func (d destructiveDDL) positionKey() string { return fmt.Sprintf("%s:%d", d.File, d.Pos) }

// writtenBefore reports whether the value a change wrote, or a baseline row
// (ev nil) holds, was there before the statement d ran. By binary log
// position when both are in one sequence of files (the stored position of a
// statement is its end, so a change that starts there is after it), by time
// otherwise. The same second counts as before: a value counted before the
// ALTER once too often is only checked, and it fails the check only when it
// holds a character the new set cannot, which a value written after the ALTER
// cannot.
func writtenBefore(ev *query.ResultRow, d destructiveDDL) bool {
	if ev == nil {
		return true
	}
	if d.hasPos() && ev.BinlogFile != "" && sameBinlogSequence(ev.BinlogFile, d.File) {
		end := query.BinlogPos{File: d.File, Pos: d.Pos}
		return !end.AtOrBefore(query.BinlogPos{File: ev.BinlogFile, Pos: ev.StartPos})
	}
	if ev.EventTimestamp.IsZero() {
		return true
	}
	return !ev.EventTimestamp.After(d.DetectedAt)
}

// CheckRow refuses a row the output is about to carry when one of its values
// was written before a narrowing ALTER and holds a character that ALTER's
// character set cannot: the server replaced it with '?' and wrote no row
// event, so the row would keep text the table no longer has. ev is the change
// the row comes from, nil for a row passed through from the baseline. A nil
// receiver checks nothing.
//
// The refusal names the column, the ALTER and both character sets, never the
// value or the row's key: it reaches clients (the shim, MCP) whose session may
// not be allowed to read the table's rows.
func (n *CharsetNarrowings) CheckRow(row map[string]any, ev *query.ResultRow) error {
	if c := n.lost(row, ev); c != nil {
		return n.lostErr(c)
	}
	return nil
}

// lost is the narrowing CheckRow refuses row on, nil when there is none.
func (n *CharsetNarrowings) lost(row map[string]any, ev *query.ResultRow) *charsetNarrowing {
	if !n.Checked() {
		return nil
	}
	for i := range n.checked {
		c := &n.checked[i]
		v, ok := columnValue(row, c.Column)
		if !ok || v == nil || !writtenBefore(ev, c.DDL) {
			continue
		}
		var s string
		switch t := v.(type) {
		case string:
			s = t
		case []byte:
			s = string(t)
		default:
			continue
		}
		if !utf8.ValidString(s) {
			// The stored bytes of a change captured before #2634, not text:
			// its characters cannot be read off it, except that ascii holds
			// no byte past 0x7F in any character set.
			if c.To == "ascii" && !isASCII(s) {
				return c
			}
			continue
		}
		if holds, known := charsetHolds(c.To, s); known && !holds {
			return c
		}
	}
	return nil
}

// columnValue reads a column off a row map, whose keys are spelled as the
// baseline's CREATE TABLE or the change's image spells them: the same name,
// case aside (MySQL column names compare without case).
func columnValue(row map[string]any, name string) (any, bool) {
	if v, ok := row[name]; ok {
		return v, true
	}
	for k, v := range row {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return nil, false
}

func (n *CharsetNarrowings) lostErr(c *charsetNarrowing) error {
	from := c.From
	if from == "" {
		from = "a character set not recorded"
	}
	return fmt.Errorf("%w: ALTER TABLE on %s.%s changed column %s from %s to %s, %s, between the snapshot this starts from and the requested point in time. "+
		"A value of %s written before it holds a character %s cannot store, which the server replaced with '?' without writing row changes, "+
		"so the result would keep text the table no longer has. Take a new snapshot of the table, which will hold it as it is after the alter table, and start from that one",
		ErrDestructiveDDL, n.schema, n.table, c.Column, from, c.To, c.DDL.where(), c.Column, c.To)
}

// UntouchedNote is the warning of a reader that checks the rows it writes but
// not the ones it leaves in place (the Iceberg export's increments): a row no
// change in the window touched keeps what the output already held, and if
// that held a character a narrowing ALTER's set cannot, the source has '?'
// there. "" when there is no narrowing to check.
func (n *CharsetNarrowings) UntouchedNote() string {
	if !n.Checked() {
		return ""
	}
	seen := map[string]bool{}
	var parts []string
	for _, c := range n.checked {
		p := fmt.Sprintf("column %s to %s by the ALTER TABLE %s", c.Column, c.To, c.DDL.where())
		if !seen[p] {
			seen[p] = true
			parts = append(parts, p)
		}
	}
	return fmt.Sprintf("an ALTER TABLE narrowed %s.%s's %s. Rows this run did not change keep the text written before it; "+
		"where that text held a character the new set cannot store, the source now has '?' and this output does not",
		n.schema, n.table, strings.Join(parts, "; "))
}
