// Package hiderules holds the per-server rules that hide tables and columns
// on the analytical surfaces (#2529): SQL on the copy, the MySQL-protocol
// port and the MCP sql tool.
//
// Hiding works like a MySQL view, not like a column grant: a hidden table has
// no view, and a hidden column is not part of its table's view, so SELECT *
// returns the table without it. Nothing here changes what the copy stores; the
// Parquet files, the index and the archives keep everything, and readers with
// direct access to the files see everything.
//
// Identifiers are compared without case. The rules are a text list an
// operator types, so this package is only parsing, a canonical spelling that
// parses back to itself, and matching.
package hiderules

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
)

// MaxEntries bounds each list. The rules travel in the server registry file
// and are matched per statement; a list this long is a mistake, not a policy.
const MaxEntries = 1000

// Rules is the stored and wire form: one entry per item.
//
//   - Tables: "schema.table" hides that table; "schema.*" hides every table
//     of the schema.
//   - Columns: "schema.table.column" hides that column; "schema.*.column"
//     hides it in every table of the schema that has it.
//
// A name holding a dot, a space, a backtick or a lone "*" is written in
// backticks, as in MySQL ("`my.db`.t"; a backtick inside is doubled).
type Rules struct {
	Tables  []string `yaml:"tables,omitempty" json:"tables"`
	Columns []string `yaml:"columns,omitempty" json:"columns"`
	// Extra keeps keys a newer build wrote under the rules, the way
	// ServerEntry.Extra does for the entry, so an older build that rewrites
	// the registry does not drop a restriction it cannot read.
	//
	// Format rule for every later build: EVERYTHING under `hidden:` restricts.
	// A build that finds a key here it does not know cannot enforce it, so
	// Load refuses the rules and the surfaces refuse that server's statements
	// rather than serve without a restriction someone asked for. Anything
	// informational (a note, an edit date) belongs on the server entry, never
	// here, or every older build would stop serving the server.
	Extra map[string]any `yaml:",inline" json:"-"`
}

// UnknownKeys returns the keys under the rules this build cannot read,
// sorted; nil when none.
func (r *Rules) UnknownKeys() []string {
	if r == nil || len(r.Extra) == 0 {
		return nil
	}
	keys := make([]string, 0, len(r.Extra))
	for k := range r.Extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Load is how every enforcing reader turns STORED rules into a Set. It fails
// when a stored entry does not parse (a hand edit) and when the rules hold
// keys this build cannot read (a newer build wrote them): either way some
// restriction cannot be enforced, and the caller must refuse, never read the
// error as "hide nothing". A nil r loads as an empty set.
func Load(r *Rules) (*Set, error) {
	if r == nil {
		return &Set{}, nil
	}
	if keys := r.UnknownKeys(); len(keys) > 0 {
		return nil, fmt.Errorf("the hidden rules hold %s, which this build cannot read: a newer DBTrail wrote them; "+
			"upgrade DBTrail to serve this server's analytical surfaces again", strings.Join(keys, ", "))
	}
	set, err := Parse(Rules{Tables: r.Tables, Columns: r.Columns})
	if err != nil {
		return nil, fmt.Errorf("the stored hidden rules do not parse: %w", err)
	}
	return set, nil
}

type part struct {
	name   string
	quoted bool
}

func (p part) wild() bool { return !p.quoted && p.name == "*" }

type tableRule struct {
	schema, table string
	allTables     bool
}

type columnRule struct {
	schema, table, column string
	allTables             bool
}

// Set is a parsed, deduplicated rule list. A nil *Set hides nothing.
type Set struct {
	tables  []tableRule
	columns []columnRule
}

// Parse validates and canonicalizes r. Blank entries are dropped; an entry
// repeated with another case keeps its first spelling. Every refused entry is
// named in the error, not only the first.
func Parse(r Rules) (*Set, error) {
	s := &Set{}
	var errs []string
	seenT := map[string]bool{}
	for _, raw := range r.Tables {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		ps, err := splitEntry(raw)
		if err == nil {
			switch {
			case len(ps) == 1:
				err = errors.New("write it as schema.table")
			case len(ps) == 3:
				err = errors.New("has three parts; a column goes in the columns list as schema.table.column")
			case len(ps) != 2:
				err = errors.New("write it as schema.table")
			case ps[0].wild():
				err = errors.New("a schema cannot be a wildcard; list each schema as schema.*")
			}
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("table entry %q: %v", raw, err))
			continue
		}
		tr := tableRule{schema: ps[0].name, table: ps[1].name, allTables: ps[1].wild()}
		key := strings.ToLower(canonTable(tr))
		if seenT[key] {
			continue
		}
		seenT[key] = true
		s.tables = append(s.tables, tr)
	}
	seenC := map[string]bool{}
	for _, raw := range r.Columns {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		ps, err := splitEntry(raw)
		if err == nil {
			switch {
			case len(ps) != 3:
				err = errors.New("write it as schema.table.column")
			case ps[0].wild():
				err = errors.New("a schema cannot be a wildcard; list each schema as schema.*.column")
			case ps[2].wild():
				err = errors.New("to hide every column, put the table in the tables list")
			}
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("column entry %q: %v", raw, err))
			continue
		}
		cr := columnRule{schema: ps[0].name, table: ps[1].name, column: ps[2].name, allTables: ps[1].wild()}
		key := strings.ToLower(canonColumn(cr))
		if seenC[key] {
			continue
		}
		seenC[key] = true
		s.columns = append(s.columns, cr)
	}
	if len(s.tables) > MaxEntries || len(s.columns) > MaxEntries {
		errs = append(errs, fmt.Sprintf("each list holds at most %d entries", MaxEntries))
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "; "))
	}
	return s, nil
}

// splitEntry splits one entry on the dots outside backticks.
func splitEntry(raw string) ([]part, error) {
	if strings.ContainsAny(raw, "\r\n") {
		return nil, errors.New("one entry per line")
	}
	var parts []part
	var cur strings.Builder
	quoted, inQuote, closed := false, false, false
	flush := func() error {
		name := cur.String()
		if !quoted {
			name = strings.TrimSpace(name)
			switch {
			case name == "":
				return errors.New("a name is empty")
			case strings.Contains(name, ","):
				return errors.New("one entry per line")
			case strings.IndexFunc(name, unicode.IsSpace) >= 0:
				return errors.New("a name with spaces goes in backticks")
			case name != "*" && strings.Contains(name, "*"):
				return errors.New(`"*" stands alone for every table; a name holding "*" goes in backticks`)
			}
		} else if name == "" {
			return errors.New("a name is empty")
		}
		parts = append(parts, part{name: name, quoted: quoted})
		cur.Reset()
		quoted, closed = false, false
		return nil
	}
	rs := []rune(raw)
	for i := 0; i < len(rs); i++ {
		c := rs[i]
		switch {
		case inQuote && c == '`' && i+1 < len(rs) && rs[i+1] == '`':
			cur.WriteRune('`')
			i++
		case inQuote && c == '`':
			inQuote, closed = false, true
		case inQuote:
			cur.WriteRune(c)
		case c == '`':
			if quoted || strings.TrimSpace(cur.String()) != "" {
				return nil, errors.New("a backtick may only wrap a whole name")
			}
			cur.Reset()
			inQuote, quoted = true, true
		case c == '.':
			if err := flush(); err != nil {
				return nil, err
			}
		case closed && (c == ' ' || c == '\t'):
			// spaces after a closing backtick, before the dot
		case closed:
			return nil, errors.New("a backtick may only wrap a whole name")
		default:
			cur.WriteRune(c)
		}
	}
	if inQuote {
		return nil, errors.New("a backtick is not closed")
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return parts, nil
}

// quote spells one name the way splitEntry reads it back.
func quote(name string, wild bool) string {
	if wild {
		return "*"
	}
	if strings.ContainsAny(name, ".`,*") || strings.IndexFunc(name, unicode.IsSpace) >= 0 {
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return name
}

func canonTable(t tableRule) string {
	return quote(t.schema, false) + "." + quote(t.table, t.allTables)
}

func canonColumn(c columnRule) string {
	return quote(c.schema, false) + "." + quote(c.table, c.allTables) + "." + quote(c.column, false)
}

// Rules returns the canonical lists, nil when empty so YAML leaves them out.
func (s *Set) Rules() Rules {
	var r Rules
	if s == nil {
		return r
	}
	for _, t := range s.tables {
		r.Tables = append(r.Tables, canonTable(t))
	}
	for _, c := range s.columns {
		r.Columns = append(r.Columns, canonColumn(c))
	}
	return r
}

// Empty reports whether the set hides nothing.
func (s *Set) Empty() bool { return s == nil || (len(s.tables) == 0 && len(s.columns) == 0) }

// TableHidden reports whether schema.table is hidden.
func (s *Set) TableHidden(schema, table string) bool {
	if s == nil {
		return false
	}
	for _, t := range s.tables {
		if strings.EqualFold(t.schema, schema) && (t.allTables || strings.EqualFold(t.table, table)) {
			return true
		}
	}
	return false
}

// HiddenColumns returns the column names hidden in schema.table, as the rules
// spell them, in rule order, without repeats; nil when none. The caller
// matches them against the table's real columns without case.
func (s *Set) HiddenColumns(schema, table string) []string {
	if s == nil {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, c := range s.columns {
		if !strings.EqualFold(c.schema, schema) || !(c.allTables || strings.EqualFold(c.table, table)) {
			continue
		}
		if k := strings.ToLower(c.column); !seen[k] {
			seen[k] = true
			out = append(out, c.column)
		}
	}
	return out
}

// ColumnHidden reports whether column is hidden in schema.table.
func (s *Set) ColumnHidden(schema, table, column string) bool {
	for _, c := range s.HiddenColumns(schema, table) {
		if strings.EqualFold(c, column) {
			return true
		}
	}
	return false
}

// TableRule is one table entry, for a reader that renders the rules (the
// events view filters rows by them in SQL).
type TableRule struct {
	Schema, Table string
	AllTables     bool // schema.*: Table is empty
}

// ColumnRule is one column entry; AllTables means schema.*.column.
type ColumnRule struct {
	Schema, Table, Column string
	AllTables             bool
}

// TableRules returns the table entries in rule order.
func (s *Set) TableRules() []TableRule {
	if s == nil {
		return nil
	}
	out := make([]TableRule, 0, len(s.tables))
	for _, t := range s.tables {
		r := TableRule{Schema: t.schema, Table: t.table, AllTables: t.allTables}
		if r.AllTables {
			r.Table = ""
		}
		out = append(out, r)
	}
	return out
}

// ColumnRules returns the column entries in rule order.
func (s *Set) ColumnRules() []ColumnRule {
	if s == nil {
		return nil
	}
	out := make([]ColumnRule, 0, len(s.columns))
	for _, c := range s.columns {
		r := ColumnRule{Schema: c.schema, Table: c.table, Column: c.column, AllTables: c.allTables}
		if r.AllTables {
			r.Table = ""
		}
		out = append(out, r)
	}
	return out
}
