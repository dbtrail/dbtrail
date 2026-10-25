package console

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/hiderules"
)

// hiddenGrantCheck is what the forwarding account's grants say about a
// server's hidden rules (#2529). With Engine Routing the MySQL-protocol port
// forwards some statements to the source with that account, and there the
// rules do not apply: MySQL's grants do. So the console names every hidden
// table or column the account can read, and the grant that lets it.
type hiddenGrantCheck struct {
	// Exposed: "<rule>: <grant line>" for every rule the account can read.
	Exposed []string `json:"exposed,omitempty"`
	// Unsure: what the grants cannot settle, each with what to check. A
	// grant list can show that the account reads something; it can never
	// show that it does not (views and routines run with their creator's
	// rights, roles and grants to everyone are not all listed), so this is
	// never empty when there are rules.
	Unsure []string `json:"unsure,omitempty"`
}

// harmlessPrivileges show nothing of a table's values beyond what SELECT
// grants, which is judged on its own (readable). Everything else is named.
var harmlessPrivileges = map[string]bool{
	"USAGE": true, "SELECT": true, "ALL": true, "ALL PRIVILEGES": true, "EXECUTE": true,
	"SHOW VIEW": true, "SHOW DATABASES": true, "REPLICATION CLIENT": true,
	"REFERENCES": true, "SHOW_ROUTINE": true,
}

// hiddenGrantsAlwaysUnsure is what no SHOW GRANTS settles: it is said on
// every server with rules, beside whatever the grants did show.
const hiddenGrantsAlwaysUnsure = "SHOW GRANTS does not list MySQL's mandatory roles or MariaDB's grants to PUBLIC: " +
	"check @@mandatory_roles and SHOW GRANTS FOR PUBLIC for anything that reads what this server hides"

type grantPriv struct {
	name string
	cols []string // a column-level grant's columns; nil for the whole table
}

type grantLine struct {
	shown        string
	privs        []grantPriv
	db, table    string // table "*" for a database-level grant, db "*" global
	dbIsWildcard bool   // a database-level grant: db is a LIKE pattern
}

func hiddenExposures(lines []string, set *hiderules.Set) hiddenGrantCheck {
	var out hiddenGrantCheck
	if set.Empty() {
		return out
	}
	var parsed []grantLine
	var roles []string
	var execute, readsElsewhere bool
	var unreadable, serverWide []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToUpper(line), "GRANT ") {
			continue // a REVOKE (partial revoke) only takes away
		}
		body := line[len("GRANT "):]
		on, to := keywordAt(body, "ON"), keywordAt(body, "TO")
		if on < 0 || (to >= 0 && to < on) {
			if to >= 0 {
				roles = append(roles, splitQuoted(body[:to])...)
			}
			continue
		}
		obj := body[on+len(" ON "):]
		if to >= 0 {
			obj = body[on+len(" ON ") : to]
		}
		obj = strings.TrimSpace(obj)
		privs := splitPrivilegesCols(body[:on])
		for _, p := range privs {
			if p.name == "EXECUTE" {
				execute = true
			}
		}
		// Every privilege that is not a plain read, or one known to show
		// nothing of a table's values, is named, once per line: a list of
		// the dangerous ones is never complete (binary log readers, definer
		// setters, role and audit administration, the next MySQL adds), so
		// the check lists what it KNOWS is harmless and names the rest.
		for _, p := range privs {
			if !harmlessPrivileges[p.name] {
				serverWide = append(serverWide, grantSecret.ReplaceAllString(line, "${1}'<hidden>'"))
				break
			}
		}
		// A routine grant (ON PROCEDURE/FUNCTION ...) splits into a db that
		// is not a schema name and never matches; EXECUTE does not read.
		if up := strings.ToUpper(obj); strings.HasPrefix(up, "TABLE ") {
			obj = strings.TrimSpace(obj[len("TABLE "):])
		}
		parts := splitDotted(obj)
		shown := grantSecret.ReplaceAllString(line, "${1}'<hidden>'")
		if len(parts) != 2 {
			// A line that grants a read on something this cannot name is
			// said, never dropped: dropping it would read as "safe".
			for _, p := range privs {
				if p.name == "SELECT" || p.name == "ALL" || p.name == "ALL PRIVILEGES" {
					unreadable = append(unreadable, shown)
					break
				}
			}
			continue
		}
		g := grantLine{shown: shown, privs: privs, db: parts[0], table: parts[1]}
		g.dbIsWildcard = g.table == "*" && g.db != "*"
		parsed = append(parsed, g)
	}

	readable := func(g grantLine, column string) bool {
		for _, p := range g.privs {
			if p.name != "SELECT" && p.name != "ALL" && p.name != "ALL PRIVILEGES" {
				continue
			}
			if p.cols == nil || column == "" {
				return true
			}
			for _, c := range p.cols {
				if strings.EqualFold(c, column) {
					return true
				}
			}
		}
		return false
	}
	// covers reports whether g reaches a table of schema (any table when
	// allTables, else table) and, for a column rule, the column.
	covers := func(g grantLine, schema, table string, allTables bool, column string) bool {
		switch {
		case g.db == "*":
			return readable(g, "")
		case g.dbIsWildcard:
			return likeFold(g.db, schema) && readable(g, "")
		case !strings.EqualFold(g.db, schema):
			return false
		case !allTables && !strings.EqualFold(g.table, table):
			return false
		}
		return readable(g, column)
	}
	name := func(schema, table string, all bool, column string) string {
		if all {
			table = "*"
		}
		n := schema + "." + table
		if column != "" {
			n += "." + column
		}
		return n
	}
	// Any read at all: a view there may select what the rules hide, with
	// its creator's rights (SQL SECURITY DEFINER, MySQL's default).
	var logsReadable bool
	for _, g := range parsed {
		if !readable(g, "") {
			continue
		}
		readsElsewhere = true
		db := strings.ToLower(g.db)
		tbl := strings.ToLower(g.table)
		switch {
		case db == "*",
			db == "performance_schema",
			g.dbIsWildcard && (likeFold(g.db, "mysql") || likeFold(g.db, "performance_schema")),
			db == "mysql" && (tbl == "*" || tbl == "general_log" || tbl == "slow_log"):
			logsReadable = true
		}
	}
	for _, r := range set.TableRules() {
		for _, g := range parsed {
			if covers(g, r.Schema, r.Table, r.AllTables, "") {
				out.Exposed = append(out.Exposed, name(r.Schema, r.Table, r.AllTables, "")+": "+g.shown)
				break
			}
		}
	}
	for _, r := range set.ColumnRules() {
		for _, g := range parsed {
			if covers(g, r.Schema, r.Table, r.AllTables, r.Column) {
				out.Exposed = append(out.Exposed, name(r.Schema, r.Table, r.AllTables, r.Column)+": "+g.shown)
				break
			}
		}
	}
	if len(roles) > 0 {
		out.Unsure = append(out.Unsure, fmt.Sprintf("the account holds the role(s) %s, and SHOW GRANTS does not list what a role allows: "+
			"check that none of them can read what this server hides", strings.Join(roles, ", ")))
	}
	if logsReadable {
		out.Unsure = append(out.Unsure, "the account can read MySQL's own log tables (mysql.general_log, mysql.slow_log or performance_schema's statement history), "+
			"which hold other sessions' statements with their values: revoke that read")
	}
	if readsElsewhere {
		out.Unsure = append(out.Unsure, "the account can read tables, and a VIEW among them runs with its creator's rights: "+
			"check that no view it can read selects what this server hides")
	}
	if execute {
		out.Unsure = append(out.Unsure, "the account has EXECUTE, and a stored function or procedure runs with its creator's rights: "+
			"check that none it can call returns what this server hides")
	}
	for _, l := range serverWide {
		out.Unsure = append(out.Unsure, "this grant allows more than reading tables, and some such privileges reach hidden values "+
			"(FILE reads the server's files; PROCESS and SUPER show other sessions' statements; replication and binlog privileges read "+
			"statement text with its values; definer, role and audit privileges, and writes, can reach or confirm them): "+l)
	}
	for _, l := range unreadable {
		out.Unsure = append(out.Unsure, "this grant reads something this check cannot name: "+l)
	}
	out.Unsure = append(out.Unsure, hiddenGrantsAlwaysUnsure)
	return out
}

// splitDotted splits `db`.`table` (or db.table, *.*) at the dot outside
// quotes, unquoting each part.
func splitDotted(s string) []string {
	var parts []string
	var cur strings.Builder
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0 && c == quote && i+1 < len(s) && s[i+1] == quote:
			cur.WriteByte(c)
			i++
		case quote != 0 && c == quote:
			quote = 0
		case quote != 0:
			cur.WriteByte(c)
		case c == '`' || c == '\'' || c == '"':
			quote = c
		case c == '.':
			parts = append(parts, strings.TrimSpace(cur.String()))
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	return append(parts, strings.TrimSpace(cur.String()))
}

// splitPrivilegesCols is splitPrivileges keeping each column list.
func splitPrivilegesCols(s string) []grantPriv {
	var out []grantPriv
	for _, raw := range splitQuotedDepth(s) {
		p := grantPriv{}
		if i := strings.IndexByte(raw, '('); i >= 0 {
			if j := strings.LastIndexByte(raw, ')'); j > i {
				for _, c := range splitQuoted(raw[i+1 : j]) {
					p.cols = append(p.cols, strings.Trim(c, "`'\""))
				}
			}
			raw = raw[:i]
		}
		p.name = strings.ToUpper(strings.Join(strings.Fields(raw), " "))
		if p.name != "" {
			out = append(out, p)
		}
	}
	return out
}

// splitQuotedDepth splits at commas outside quotes and parentheses.
func splitQuotedDepth(s string) []string {
	var out []string
	var quote byte
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '`' || c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			if depth > 0 {
				depth--
			}
		case c == ',' && depth == 0:
			out = append(out, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}

// likeFold matches name against a database-level grant's name, where _ and %
// are wildcards unless escaped with a backslash, without case.
func likeFold(pattern, name string) bool {
	p, n := []rune(strings.ToLower(pattern)), []rune(strings.ToLower(name))
	var match func(i, j int) bool
	match = func(i, j int) bool {
		for i < len(p) {
			switch {
			case p[i] == '\\' && i+1 < len(p):
				if j >= len(n) || n[j] != p[i+1] {
					return false
				}
				i, j = i+2, j+1
			case p[i] == '%':
				// A run of % matches what one does: skipped, so the
				// backtracking stays linear on a pattern like a%%%%%b. An
				// escaped \% never reaches here, so it is kept.
				for i+1 < len(p) && p[i+1] == '%' {
					i++
				}
				for k := j; k <= len(n); k++ {
					if match(i+1, k) {
						return true
					}
				}
				return false
			case p[i] == '_':
				if j >= len(n) {
					return false
				}
				i, j = i+1, j+1
			default:
				if j >= len(n) || n[j] != p[i] {
					return false
				}
				i, j = i+1, j+1
			}
		}
		return j == len(n)
	}
	return match(0, 0)
}
