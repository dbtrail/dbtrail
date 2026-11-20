package pgshim

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The catalog queries tools send to list what a server holds (#2769): psql's
// \l, \dn, \dt, \d and \d <table>, JDBC's DatabaseMetaData (getSchemas,
// getCatalogs, getTables, getColumns, getPrimaryKeys, and the type lookups
// that follow), SQLAlchemy's inspector (get_schema_names, get_table_names,
// get_columns, get_pk_constraint, has_table and the queries around them), and
// Grafana's table and column lists.
//
// They are recognized, statement by statement, and answered here from the
// copy's real tables, never run on the copy's engine: that engine has a
// pg_catalog of its own that names its internal schemas and its own types,
// and lacks most of the functions these statements call. Each recognized
// statement is one a client was seen sending (the captures are in
// testdata/catalog_2769), read in a canonical form (catalogCanon), with the
// parts that vary (a name, a pattern, an OID, the conditions psql adds for a
// pattern) read out of it. A statement that reads the catalog and is not one
// of these is refused by name (catalogRefusal), never answered with an
// empty result that would read as "there are no tables".
//
// What the catalog says:
//   - the schemas and tables a statement on the copy can read: the server's
//     hidden tables and columns are not in it, and neither are the copy's own
//     schemas (main);
//   - each column with the type the port announces for it in a result
//     (columnType), so what a tool lists is what it then reads: a varchar(n),
//     a jsonb or an enum column is text here as it is in a result;
//   - NOT NULL, the default and the primary key as the source recorded them in
//     the snapshot (baseline.PGRelation), and the table's OID there. The
//     primary key is named as PostgreSQL names one by default
//     (<table>_pkey): the snapshot does not record the source's name for it;
//   - no other index, constraint, view, sequence, policy or comment: the copy
//     has none;
//   - the owner of everything is the connection's user, and the one database
//     is the one the connection opened.

// CatalogTable is one table of the copy, for the catalog.
type CatalogTable struct {
	Schema, Name string
	// OID is the table's OID on the source, 0 when the snapshot did not
	// record it.
	OID uint32
	// Columns are the table's columns as a statement reads them, in order.
	Columns []CatalogColumn
	// PrimaryKey names the primary key's columns in key order, empty when
	// there is none or it was not recorded.
	PrimaryKey []string
}

// CatalogColumn is one column of a CatalogTable.
type CatalogColumn struct {
	Name string
	// Type is the copy engine's type for the column (DECIMAL(10,2),
	// TIMESTAMP WITH TIME ZONE, INTEGER[]), which columnType announces.
	Type string
	// NotNull, Default and Identity are what the source recorded
	// (attnotnull, pg_get_expr of the default, attidentity).
	NotNull  bool
	Default  string
	Identity string
}

// catalogCacheFor is how long one connection reuses the catalog it read: a
// tool's listing is a burst of statements (psql's \d sends nine).
const catalogCacheFor = 10 * time.Second

// catalogRel is one row of the catalog's pg_class: a table, or its primary
// key's index.
type catalogRel struct {
	schema, name string
	kind         byte // 'r' a table, 'i' an index
	oid, nspOID  uint32
	table        *CatalogTable
}

// catalogView is the catalog one statement is answered from.
type catalogView struct {
	s        *session
	tables   []CatalogTable
	rels     []catalogRel
	schemas  []string
	database string
}

// catalog reads (or reuses) the copy's catalog for this connection.
func (s *session) catalog() (*catalogView, *pgErr) {
	if s.catalogAt.IsZero() || time.Since(s.catalogAt) > catalogCacheFor {
		ctx, done := s.statementContext()
		tables, err := s.bound.Catalog(ctx)
		done()
		if err != nil {
			return nil, s.statementError(ctx, err)
		}
		s.catalogTables, s.catalogAt = tables, time.Now()
	}
	v := &catalogView{s: s, tables: s.catalogTables, database: s.database}
	seen := map[string]bool{}
	for i := range v.tables {
		t := &v.tables[i]
		if !seen[t.Schema] {
			seen[t.Schema] = true
			v.schemas = append(v.schemas, t.Schema)
		}
		v.rels = append(v.rels, catalogRel{schema: t.Schema, name: t.Name, kind: 'r', oid: tableOID(t), nspOID: namespaceOID(t.Schema), table: t})
		if len(t.PrimaryKey) > 0 {
			v.rels = append(v.rels, catalogRel{schema: t.Schema, name: pkeyName(t), kind: 'i', oid: indexOID(t), nspOID: namespaceOID(t.Schema), table: t})
		}
	}
	sort.Strings(v.schemas)
	sort.Slice(v.rels, func(i, j int) bool {
		if v.rels[i].schema != v.rels[j].schema {
			return v.rels[i].schema < v.rels[j].schema
		}
		return v.rels[i].name < v.rels[j].name
	})
	return v, nil
}

// tableOID is the table's OID on the source, or one made from its name.
func tableOID(t *CatalogTable) uint32 {
	if t.OID != 0 {
		return t.OID
	}
	return madeOID("table:" + t.Schema + "." + t.Name)
}

// indexOID is an OID for a table's primary key index, which the snapshot
// does not record.
func indexOID(t *CatalogTable) uint32 { return madeOID("pkey:" + t.Schema + "." + t.Name) }

// namespaceOID is public's OID, which is the same on every PostgreSQL, or
// one made from the schema's name.
func namespaceOID(schema string) uint32 {
	if schema == "public" {
		return 2200
	}
	return madeOID("schema:" + schema)
}

// madeOID is an OID in the range PostgreSQL gives objects users create
// (16384 and up), from a name, so that it stays the same between statements.
func madeOID(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return 1<<30 | h.Sum32()&(1<<30-1)
}

func pkeyName(t *CatalogTable) string { return t.Name + "_pkey" }

// relByOID finds the relation an OID names.
func (v *catalogView) relByOID(oid string) *catalogRel {
	n, err := strconv.ParseUint(strings.Trim(oid, "'"), 10, 32)
	if err != nil {
		return nil
	}
	for i := range v.rels {
		if uint64(v.rels[i].oid) == n {
			return &v.rels[i]
		}
	}
	return nil
}

// visible is pg_table_is_visible: the relation is in the session's schema.
func (v *catalogView) visible(r *catalogRel) bool { return r.schema == v.s.schema() }

// --- the statement in canonical form -----------------------------------------

// catalogCanon writes a statement in the form the recognized statements are
// written in: comments gone, every run of blanks one space (none after an
// opening parenthesis or a comma, none before a closing one or a comma), everything
// lowercase but string literals and quoted names, no trailing semicolon.
func catalogCanon(q string) string {
	var b strings.Builder
	space := false
	// A blank next to a parenthesis or a comma is not kept: clients
	// break their statements across lines in such places, and differently
	// from one version to the next.
	emit := func(s string) {
		if space && b.Len() > 0 && s[0] != ',' && s[0] != ')' {
			if last := b.String()[b.Len()-1]; last != '(' && last != ',' {
				b.WriteByte(' ')
			}
		}
		space = false
		b.WriteString(s)
	}
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			space = true
			i++
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			for i < len(q) && q[i] != '\n' {
				i++
			}
			space = true
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			depth := 0
			for i < len(q) {
				if i+1 < len(q) && q[i] == '/' && q[i+1] == '*' {
					depth++
					i += 2
				} else if i+1 < len(q) && q[i] == '*' && q[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
			space = true
		case c == '\'' || c == '"':
			j := i + 1
			for j < len(q) {
				if q[j] == c {
					if j+1 < len(q) && q[j+1] == c {
						j += 2
						continue
					}
					break
				}
				if c == '\'' && q[j] == '\\' && i > 0 && (q[i-1] == 'e' || q[i-1] == 'E') && j+1 < len(q) {
					j += 2
					continue
				}
				j++
			}
			if j < len(q) {
				j++
			}
			// An E'' string's e is written with the literal, no space.
			if c == '\'' && b.Len() > 0 && !space && strings.HasSuffix(b.String(), "e") {
				b.WriteString(q[i:j])
			} else {
				emit(q[i:j])
			}
			i = j
		default:
			j := i
			for j < len(q) && q[j] != ' ' && q[j] != '\t' && q[j] != '\n' && q[j] != '\r' && q[j] != '\f' && q[j] != '\'' && q[j] != '"' &&
				!(q[j] == '-' && j+1 < len(q) && q[j+1] == '-') && !(q[j] == '/' && j+1 < len(q) && q[j+1] == '*') {
				j++
			}
			emit(strings.ToLower(q[i:j]))
			i = j
		}
	}
	return strings.TrimRight(strings.TrimSpace(b.String()), "; ")
}

// catalogWords are the names that make a statement a catalog statement: one
// that names them and is not recognized is refused.
var catalogWords = regexp.MustCompile(`(^|[^a-z0-9_.])(pg_catalog|information_schema|pg_(prepared_statements|prepared_xacts|class|namespace|attribute|attrdef|type|index|indexes|database|proc|roles|user|settings|tables|views|constraint|description|am|inherits|extension|matviews|sequence|sequences|enum|collation|policy|publication|publication_rel|statistic_ext|stat_activity|stat_user_tables|authid|trigger|rewrite|depend|tablespace|locks|shdescription|range|operator|opclass|language|cast|event_trigger|foreign_table|foreign_server|partitioned_table))($|[^a-z0-9_])`)

// catalogFuncs are the catalog's functions, which name it as well as a
// table does.
var catalogFuncs = regexp.MustCompile(`(^|[^a-z0-9_.])(current_schemas|format_type|pg_get_[a-z_]+|pg_table_is_visible|pg_type_is_visible|pg_function_is_visible|obj_description|col_description|shobj_description|has_[a-z_]+_privilege|pg_encoding_to_char|pg_relation_size|pg_total_relation_size|to_regclass|to_regtype) *\(`)

// readsCatalog says whether a statement names the catalog, a table or a
// function of it, outside a string literal and a comment, quoted or not.
func readsCatalog(q string) bool {
	plain := plainText(q)
	return catalogWords.MatchString(plain) || catalogFuncs.MatchString(plain)
}

// plainText is a statement lowercased with its comments, string literals
// (E” escapes and dollar quotes included) blanked and its double quotes
// taken out, so a quoted name reads as the name.
func plainText(q string) string {
	var b strings.Builder
	for i := 0; i < len(q); {
		c := q[i]
		switch {
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			for i < len(q) && q[i] != '\n' {
				i++
			}
			b.WriteByte(' ')
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			for depth := 0; i < len(q); {
				if i+1 < len(q) && q[i] == '/' && q[i+1] == '*' {
					depth++
					i += 2
				} else if i+1 < len(q) && q[i] == '*' && q[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
			b.WriteByte(' ')
		case c == '\'':
			escapes := i > 0 && (q[i-1] == 'e' || q[i-1] == 'E')
			j := i + 1
			for j < len(q) {
				if escapes && q[j] == '\\' {
					j += 2
					continue
				}
				if q[j] == '\'' {
					if j+1 < len(q) && q[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			b.WriteString(" '' ")
			i = j + 1
		case c == '$':
			// A dollar quote: $$...$$ or $tag$...$tag$.
			j := i + 1
			for j < len(q) && (q[j] == '_' || q[j] >= 'a' && q[j] <= 'z' || q[j] >= 'A' && q[j] <= 'Z' || q[j] >= '0' && q[j] <= '9' && j > i+1) {
				j++
			}
			if j < len(q) && q[j] == '$' {
				tag := q[i : j+1]
				if end := strings.Index(q[j+1:], tag); end >= 0 {
					b.WriteString(" '' ")
					i = j + 1 + end + len(tag)
					continue
				}
			}
			b.WriteByte(c)
			i++
		case c == '"':
			i++
		default:
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// catalogRefusal is the error of a catalog statement this port does not
// recognize.
var catalogRefusal = &pgErr{"0A000", "this statement reads the system catalog (pg_catalog or information_schema), which this port answers only for the statements psql, JDBC, SQLAlchemy and Grafana send to list schemas, tables and columns; " +
	"it is not run on the copy, whose engine has a catalog of its own. List the copy's tables with SHOW TABLES, a table's columns with DESCRIBE <table>"}

// --- recognized statements -----------------------------------------------------

// catCol is one column of a catalog statement's answer.
type catCol struct {
	name string
	oid  uint32
}

// catalogStmt is one recognized statement: its text as a client sends it,
// with $s for a string literal, $n for a number (or a string: a parameter
// not yet bound), $w for the conditions psql or JDBC add, and its answer.
type catalogStmt struct {
	name string
	text string
	re   *regexp.Regexp
	cols []catCol
	// rows answers the statement; m are the parts $s, $n and $w read.
	rows func(v *catalogView, m []string) ([][]any, *pgErr)
}

// The type OIDs the catalog's own answers use beyond the ones params.go and
// coltypes.go name.
const (
	oidRegclass   = 2205
	oidNodeTree   = 194
	oidTextArray  = 1009
	oidInt4Vector = 22
)

var (
	litRe  = `('(?:[^']|'')*')`
	numRe  = `(\d+|'(?:[^']|'')*'|\(-?\d+\))`
	condRe = `(.*?)`
)

func compileCatalogStmt(st *catalogStmt) {
	q := regexp.QuoteMeta(catalogCanon(st.text))
	q = strings.ReplaceAll(q, `\$s`, litRe)
	q = strings.ReplaceAll(q, `\$n`, numRe)
	q = strings.ReplaceAll(q, `\$w`, condRe)
	st.re = regexp.MustCompile("^" + q + "$")
}

// matchCatalog finds the recognized statement qstr is, and what it read.
func matchCatalog(canon string) (*catalogStmt, []string) {
	for _, st := range catalogStmts {
		if m := st.re.FindStringSubmatch(canon); m != nil {
			return st, m[1:]
		}
	}
	return nil, nil
}

// unquote is a string literal's value.
func unquote(lit string) string {
	return strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(lit, "'"), "'"), "''", "'")
}

// --- the conditions psql and JDBC add ------------------------------------------

// relFilter is what a statement's conditions keep of the relations.
type relFilter struct {
	kinds       map[byte]bool // nil: every kind
	visibleOnly bool
	// names and nsps are what the relation's name and its schema must
	// each match, every one of them.
	names, nsps []*regexp.Regexp
}

func (f relFilter) keep(v *catalogView, r *catalogRel) bool {
	if f.kinds != nil && !f.kinds[r.kind] {
		return false
	}
	if f.visibleOnly && !v.visible(r) {
		return false
	}
	return matchAll(f.names, r.name) && matchAll(f.nsps, r.schema)
}

func matchAll(res []*regexp.Regexp, s string) bool {
	for _, re := range res {
		if !re.MatchString(s) {
			return false
		}
	}
	return true
}

// splitAnd splits conditions joined by AND at the top level.
func splitAnd(w string) []string {
	var out []string
	depth, in, start := 0, false, 0
	for i := 0; i < len(w); i++ {
		switch c := w[i]; {
		case c == '\'':
			in = !in
		case in:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(w[i:], " and "):
			out = append(out, strings.TrimSpace(w[start:i]))
			start = i + 5
			i += 4
		}
	}
	return append(out, strings.TrimSpace(w[start:]))
}

var (
	condRelkindIn  = regexp.MustCompile(`^c\.relkind in \(((?:'[^']*'(?:, ?)?)*)\)$`)
	condRegex      = regexp.MustCompile(`^(c\.relname|n\.nspname) operator\(pg_catalog\.~\) ` + litRe + ` collate pg_catalog\.default$`)
	condLike       = regexp.MustCompile(`^(c\.relname|n\.nspname) like ` + litRe + `$`)
	condEq         = regexp.MustCompile(`^\(?(c\.relname|n\.nspname|ct\.relname) ?= ?` + litRe + `\)?$`)
	condSystemOnly = map[string]bool{
		"n.nspname <> 'pg_catalog'": true, "n.nspname !~ '^pg_toast'": true, "n.nspname <> 'information_schema'": true,
		"n.nspname !~ '^pg_'": true, "true": true, "c.relnamespace = n.oid": true,
	}
	kindRe     = regexp.MustCompile(`'([^']*)'`)
	jdbcKindRe = regexp.MustCompile(`^c\.relkind (?:= '([a-z])'|in \(([^)]*)\))$`)
)

// relFilterOf reads the conditions of a psql or JDBC relation list. A
// condition it does not know refuses the statement.
func relFilterOf(w string) (relFilter, *pgErr) {
	var f relFilter
	if strings.TrimSpace(w) == "" {
		return f, nil
	}
	for _, c := range splitAnd(w) {
		switch {
		case condSystemOnly[c]:
		case c == "pg_catalog.pg_table_is_visible(c.oid)", c == "(pg_catalog.pg_table_is_visible(c.oid))":
			f.visibleOnly = true
		case condRelkindIn.MatchString(c):
			f.kinds = map[byte]bool{}
			for _, k := range kindRe.FindAllStringSubmatch(c, -1) {
				if k[1] != "" {
					f.kinds[k[1][0]] = true
				}
			}
		case condRegex.MatchString(c):
			m := condRegex.FindStringSubmatch(c)
			re, err := regexp.Compile(unquote(m[2]))
			if err != nil {
				return f, &pgErr{"2201B", fmt.Sprintf("invalid regular expression: %v", err)}
			}
			if m[1] == "c.relname" {
				f.names = append(f.names, re)
			} else {
				f.nsps = append(f.nsps, re)
			}
		case condLike.MatchString(c):
			m := condLike.FindStringSubmatch(c)
			re, perr := likeRegexp(unquote(m[2]))
			if perr != nil {
				return f, perr
			}
			if m[1] == "c.relname" {
				f.names = append(f.names, re)
			} else {
				f.nsps = append(f.nsps, re)
			}
		case condEq.MatchString(c):
			m := condEq.FindStringSubmatch(c)
			re := regexp.MustCompile("^" + regexp.QuoteMeta(unquote(m[2])) + "$")
			if m[1] == "n.nspname" {
				f.nsps = append(f.nsps, re)
			} else {
				f.names = append(f.names, re)
			}
		case strings.HasPrefix(c, "(false or "):
			kinds, perr := jdbcTypeKinds(c)
			if perr != nil {
				return f, perr
			}
			f.kinds = kinds
		default:
			return f, catalogRefusal
		}
	}
	return f, nil
}

// jdbcTypeKinds reads JDBC's table-type condition: (false OR (<kind> AND
// <schemas>) OR ...). Only the types of user schemas apply here: the copy has
// no system, temporary or TOAST relations.
func jdbcTypeKinds(c string) (map[byte]bool, *pgErr) {
	inner := strings.TrimSuffix(strings.TrimPrefix(c, "(false or "), ")")
	kinds := map[byte]bool{}
	for _, d := range splitOr(inner) {
		d = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(d), "("), ")")
		user := strings.Contains(d, "n.nspname !~ '^pg_'") || strings.Contains(d, "n.nspname <> 'pg_catalog'")
		if !user {
			continue
		}
		// A user schemas' type: its kinds, and nothing but the schema
		// conditions beside them; anything else is not read.
		found := false
		for _, part := range splitAnd(d) {
			switch {
			case condSystemOnly[part]:
			case jdbcKindRe.MatchString(part):
				k := jdbcKindRe.FindStringSubmatch(part)
				for _, x := range kindRe.FindAllStringSubmatch("'"+k[1]+"' "+k[2], -1) {
					if x[1] != "" {
						kinds[x[1][0]] = true
						found = true
					}
				}
			default:
				return nil, catalogRefusal
			}
		}
		if !found {
			return nil, catalogRefusal
		}
	}
	return kinds, nil
}

func splitOr(w string) []string {
	var out []string
	depth, in, start := 0, false, 0
	for i := 0; i < len(w); i++ {
		switch c := w[i]; {
		case c == '\'':
			in = !in
		case in:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(w[i:], " or "):
			out = append(out, w[start:i])
			start = i + 4
			i += 3
		}
	}
	return append(out, w[start:])
}

// likeRegexp is LIKE's pattern as a regular expression: % any run, _ any
// one character, a backslash taking the next one as it is.
func likeRegexp(p string) (*regexp.Regexp, *pgErr) {
	var b strings.Builder
	b.WriteString("^(?s)")
	// By character, not by byte: a byte of a letter like é quoted on its
	// own is not that letter (and, after a backslash, not valid UTF-8).
	rs := []rune(p)
	for i := 0; i < len(rs); i++ {
		switch c := rs[i]; c {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		case '\\':
			if i+1 >= len(rs) {
				return nil, &pgErr{"22025", "LIKE pattern must not end with escape character"}
			}
			i++
			b.WriteString(regexp.QuoteMeta(string(rs[i])))
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	return regexp.MustCompile(b.String()), nil
}

// --- answering -------------------------------------------------------------------

// catalogColumns is what a recognized statement returns, said without
// running it.
func catalogColumns(st *catalogStmt) ([]string, []colType) {
	cols := make([]string, len(st.cols))
	types := make([]colType, len(st.cols))
	for i, c := range st.cols {
		cols[i] = c.name
		types[i] = catalogColType(c.oid)
	}
	return cols, types
}

func catalogColType(oid uint32) colType {
	switch oid {
	case oidName:
		return colType{oidName, 64, -1}
	case oidBool, oidChar:
		return colType{oid, 1, -1}
	case oidInt2:
		return colType{oid, 2, -1}
	case oidInt4, oidOID, oidRegclass:
		return colType{oid, 4, -1}
	case oidInt8:
		return colType{oid, 8, -1}
	}
	return colType{oid, -1, -1}
}

// answerCatalog answers a recognized catalog statement from the copy's
// tables.
func (s *session) answerCatalog(st *catalogStmt, m []string) (cols []string, types []colType, rows [][][]byte, tag string, perr *pgErr) {
	v, perr := s.catalog()
	if perr != nil {
		return nil, nil, nil, "", perr
	}
	vals, perr := st.rows(v, m)
	if perr != nil {
		return nil, nil, nil, "", perr
	}
	cols, types = catalogColumns(st)
	rows = make([][][]byte, len(vals))
	for i, r := range vals {
		cells := make([][]byte, len(r))
		for j, x := range r {
			cells[j] = catalogCell(x)
		}
		rows[i] = cells
	}
	return cols, types, rows, "SELECT " + strconv.Itoa(len(rows)), nil
}

// catalogCell writes one value as PostgreSQL writes it as text.
func catalogCell(x any) []byte {
	switch v := x.(type) {
	case nil:
		return nil
	case bool:
		if v {
			return []byte("t")
		}
		return []byte("f")
	case int:
		return []byte(strconv.Itoa(v))
	case uint32:
		return []byte(strconv.FormatUint(uint64(v), 10))
	case byte:
		return []byte{v}
	case string:
		return []byte(v)
	}
	return []byte(fmt.Sprint(x))
}

// --- types ---------------------------------------------------------------------------

// pgTypeInfo is what the catalog says about a type the port announces.
type pgTypeInfo struct {
	name    string // pg_type.typname
	format  string // format_type() without a modifier
	infoTyp string // information_schema's data_type
	length  int    // typlen
	storage byte   // typstorage
}

var pgTypes = map[uint32]pgTypeInfo{
	oidBool:        {"bool", "boolean", "boolean", 1, 'p'},
	oidBytea:       {"bytea", "bytea", "bytea", -1, 'x'},
	oidInt2:        {"int2", "smallint", "smallint", 2, 'p'},
	oidInt4:        {"int4", "integer", "integer", 4, 'p'},
	oidInt8:        {"int8", "bigint", "bigint", 8, 'p'},
	oidText:        {"text", "text", "text", -1, 'x'},
	oidFloat4:      {"float4", "real", "real", 4, 'p'},
	oidFloat8:      {"float8", "double precision", "double precision", 8, 'p'},
	oidNumeric:     {"numeric", "numeric", "numeric", -1, 'm'},
	oidDate:        {"date", "date", "date", 4, 'p'},
	oidTime:        {"time", "time without time zone", "time without time zone", 8, 'p'},
	oidTimetz:      {"timetz", "time with time zone", "time with time zone", 12, 'p'},
	oidTimestamp:   {"timestamp", "timestamp without time zone", "timestamp without time zone", 8, 'p'},
	oidTimestamptz: {"timestamptz", "timestamp with time zone", "timestamp with time zone", 8, 'p'},
	oidUUID:        {"uuid", "uuid", "uuid", 16, 'p'},
}

// typeOf is a column's announced type and what the catalog says of it.
func typeOf(c CatalogColumn) (colType, pgTypeInfo, bool) {
	t := columnType(c.Type)
	if elem, ok := arrayElement[t.oid]; ok {
		e := pgTypes[elem]
		return t, pgTypeInfo{"_" + e.name, e.format + "[]", "ARRAY", -1, 'x'}, true
	}
	info, ok := pgTypes[t.oid]
	return t, info, ok
}

// formatType is format_type(): the type's name with its modifier.
func formatType(t colType, info pgTypeInfo) string {
	if (t.oid == oidNumeric || t.oid == arrayOIDs[oidNumeric]) && t.typmod >= 4 {
		p, sc := (t.typmod-4)>>16, (t.typmod-4)&0xffff
		f := fmt.Sprintf("numeric(%d,%d)", p, sc)
		if t.oid != oidNumeric {
			f += "[]"
		}
		return f
	}
	return info.format
}

// typeByOID is a type the port announces, by OID, for the type lookups JDBC
// sends; ok is false for any other.
func typeByOID(oid uint32) (pgTypeInfo, bool) {
	if info, ok := pgTypes[oid]; ok {
		return info, true
	}
	if elem, ok := arrayElement[oid]; ok {
		e := pgTypes[elem]
		return pgTypeInfo{"_" + e.name, e.format + "[]", "ARRAY", -1, 'x'}, true
	}
	return pgTypeInfo{}, false
}

// quoteName quotes a name as PostgreSQL's quote_ident does when it must.
func quoteName(s string) string { return quoteIdent(s) }

// columnsList is a primary key's columns, quoted, for pg_get_indexdef.
func columnsList(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = quoteName(c)
	}
	return strings.Join(q, ", ")
}

// qualifiedName is schema.table as regclass writes it: bare when visible.
func (v *catalogView) qualifiedName(r *catalogRel) string {
	if v.visible(r) {
		return quoteName(r.name)
	}
	return quoteName(r.schema) + "." + quoteName(r.name)
}
