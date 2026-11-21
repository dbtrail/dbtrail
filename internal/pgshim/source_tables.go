package pgshim

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Which tables a connection read from its source (#2985).
//
// The rule that a connection never reads a value go back after the source
// answered it (waitForCopy, copyOlderThanSource) holds per table: the copy
// answers a statement without waiting, however far behind it is, when none
// of the tables it reads was read from the source on this connection. Two
// answers about different tables can then be of different moments, the
// source's of now and the copy's of a second or two ago.
//
// Marking too much is safe (the copy waits, or the source answers, as
// before); marking too little is a wrong answer. So a statement the source
// answered marks EVERY table unless two things both hold:
//
//  1. its text is one this file reads whole (readsOf): one SELECT whose
//     tables are all named in its FROM clauses, calling only functions known
//     to read no table. Anything else, a function in FROM, a nested WITH,
//     TABLE x, LATERAL, ONLY, a cursor, several statements, a name written
//     with its schema before a function, is not read, and marks every table;
//  2. the source itself confirms it (proofSQL), on the same connection and
//     right behind the statement: each name resolves, as PostgreSQL resolves
//     it under the session's search_path, to a plain permanent table that
//     has no child table and no row-level security; and nothing in the
//     database can run a user's code where the text shows none (an operator
//     or a cast made in SQL or PL/pgSQL, a domain with a constraint, a
//     function of one row called as a column, a function of the user's with
//     the name of one the statement calls). The tables marked are the ones
//     the source names in that answer, not the ones read from the text.
//
// A view, a temporary table, a foreign table, a partitioned table, an
// inheritance parent, a name that another schema of the path shadows: each
// fails the second check, and every table is marked. Functions and operators
// written in C (an extension's) are taken to read no table of the user's
// when they stand behind an operator or a cast; called by name they are not
// in pureFuncs, and mark every table.
//
// On the copy's side (apartFromSource) the statement about to run is read
// the same way, its names matched against the copy's catalog without regard
// to case and, when written without a schema, in every schema: more tables
// than it reads, never fewer.

// relRef is one relation a statement names in a FROM clause: one to three
// names, as written.
type relRef struct {
	parts []rtToken
	// at is the token it starts at.
	at int
}

// written is the reference as PostgreSQL reads it: each name quoted when it
// was, and joined by dots (to_regclass folds an unquoted one as the parser
// does).
func (r relRef) written() string {
	out := make([]string, len(r.parts))
	for i, p := range r.parts {
		if p.kind == rtQuoted {
			out[i] = `"` + strings.ReplaceAll(p.text, `"`, `""`) + `"`
		} else {
			out[i] = p.text
		}
	}
	return strings.Join(out, ".")
}

// cteDef is one name a statement's opening WITH defines; end is the token
// that closes its body.
type cteDef struct {
	name string
	end  int
}

// stmtReads is what a statement's text says it reads.
type stmtReads struct {
	refs []relRef
	// funcs are the functions it calls by name, lowercased, each once.
	funcs []string
	ctes  []cteDef
	// recursive: WITH RECURSIVE, whose names are in scope in every body.
	recursive bool
}

// isCTE reports whether r names one of the statement's WITH queries where it
// stands: a single unquoted or quoted name, after that query's body ended
// (in any body under RECURSIVE). Inside its own body, or an earlier one, the
// name is a relation of the database.
func (s stmtReads) isCTE(r relRef) bool {
	if len(r.parts) != 1 {
		return false
	}
	for _, c := range s.ctes {
		if c.name == r.parts[0].text && (s.recursive || c.end < r.at) {
			return true
		}
	}
	return false
}

// unprovenWords are words that make a statement one this file does not read.
var unprovenWords = map[string]string{
	"table":       "TABLE",
	"lateral":     "LATERAL",
	"only":        "ONLY",
	"tablesample": "TABLESAMPLE",
	"into":        "INTO",
	"insert":      "INSERT",
	"update":      "UPDATE",
	"delete":      "DELETE",
	"merge":       "MERGE",
	"execute":     "EXECUTE",
	"call":        "CALL",
	"do":          "DO",
	"copy":        "COPY",
	"declare":     "a cursor",
	"fetch":       "FETCH",
	"move":        "MOVE",
	"operator":    "OPERATOR()",
	"pivot":       "PIVOT",
	"unpivot":     "UNPIVOT",
	"summarize":   "SUMMARIZE",
	"describe":    "DESCRIBE",
}

// syntaxParen are the words that stand before a parenthesis as part of the
// grammar: none is a function looked up by its name.
var syntaxParen = map[string]bool{
	"select": true, "where": true, "and": true, "or": true, "not": true, "in": true, "exists": true,
	"any": true, "all": true, "some": true, "as": true, "on": true, "over": true, "filter": true,
	"when": true, "then": true, "else": true, "using": true, "by": true, "union": true,
	"intersect": true, "except": true, "distinct": true, "between": true, "is": true, "having": true,
	"limit": true, "offset": true, "case": true, "like": true, "ilike": true, "similar": true, "to": true,
	"values": true, "group": true, "array": true, "row": true,
	"cast": true, "coalesce": true, "nullif": true, "greatest": true, "least": true,
}

// pureFuncs are functions of PostgreSQL itself that read no table, whatever
// their arguments. A statement that calls any other by name marks every
// table: a user's function can read anything, and so can some built-in ones
// (query_to_xml, currval, pg_relation_size). The source confirms that no
// function of the user's shares a name the statement calls (proofSQL).
var pureFuncs = func() map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(`
		count sum avg min max bool_and bool_or every string_agg array_agg json_agg jsonb_agg
		json_object_agg jsonb_object_agg stddev stddev_pop stddev_samp variance var_pop var_samp
		corr covar_pop covar_samp percentile_cont percentile_disc mode bit_and bit_or
		row_number rank dense_rank percent_rank cume_dist ntile lag lead first_value last_value nth_value
		abs ceil ceiling floor round trunc mod power sqrt cbrt exp ln log log10 sign pi random div
		width_bucket degrees radians gcd lcm
		length char_length character_length octet_length bit_length lower upper initcap substring
		substr position strpos trim btrim ltrim rtrim lpad rpad left right repeat replace reverse
		split_part concat concat_ws md5 chr ascii translate overlay starts_with format to_hex encode
		decode quote_literal quote_ident quote_nullable regexp_replace regexp_match
		regexp_split_to_array string_to_array array_to_string
		now clock_timestamp statement_timestamp transaction_timestamp timeofday age date_trunc
		date_part extract make_date make_time make_timestamp make_timestamptz make_interval to_char
		to_date to_timestamp to_number isfinite justify_days justify_hours justify_interval date_bin
		timezone
		array_length cardinality array_position array_append array_prepend array_cat array_remove
		json_build_object jsonb_build_object json_build_array jsonb_build_array to_json to_jsonb
		json_extract_path jsonb_extract_path json_extract_path_text jsonb_extract_path_text
		json_typeof jsonb_typeof json_array_length jsonb_array_length row_to_json
		num_nulls num_nonnulls version pg_backend_pid pg_is_in_recovery current_setting
		current_database current_schema pg_typeof pg_sleep
		numeric decimal varchar char character bpchar text int2 int4 int8 float4 float8 float date
		time timestamp timestamptz interval bool bit`) {
		m[w] = true
	}
	return m
}()

// fromInCall are the functions whose arguments PostgreSQL writes with FROM.
var fromInCall = map[string]bool{"extract": true, "substring": true, "trim": true, "overlay": true}

// readsMaxRefs bounds the relations one statement may name and still be
// read: past it, it marks every table.
const readsMaxRefs = 32

// asciiOnly reports whether s holds no byte past ASCII: the lexer lowercases
// an unquoted name as Unicode, PostgreSQL only its ASCII letters.
func asciiOnly(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// readsOf reads which relations a statement names and which functions it
// calls. why, when not empty, says what in it is not read: its tables are
// then not known, and the caller takes it to read every table. alsoPure are
// more functions known to read no table (the copy's own).
func readsOf(q string, alsoPure map[string]bool) (r stmtReads, why string) {
	toks, why := lexSQL(q)
	if why != "" {
		return r, why
	}
	for len(toks) > 0 && isOpTok(toks, len(toks)-1, ";") {
		toks = toks[:len(toks)-1]
	}
	if len(toks) == 0 {
		return r, "an empty statement"
	}
	depth := 0
	for _, t := range toks {
		if t.kind != rtOp {
			continue
		}
		switch t.text {
		case ";":
			return r, "more than one statement"
		case "(":
			depth++
		case ")":
			depth--
			if depth < 0 {
				return r, "a parenthesis that closes nothing"
			}
		}
	}
	if depth != 0 {
		return r, "a parenthesis left open"
	}
	if w := firstWord(toks); w != "select" && w != "with" {
		return r, "not a SELECT"
	}
	word := func(i int, w string) bool { return i >= 0 && i < len(toks) && toks[i].kind == rtWord && toks[i].text == w }

	// The opening WITH: each name, an optional column list, AS, and a body
	// in parentheses. skip are the header's tokens; bodies the "(" of each
	// body, with the index of its definition.
	skip := map[int]bool{}
	bodies := map[int]int{}
	if word(0, "with") {
		i := 1
		skip[0] = true
		if word(i, "recursive") {
			r.recursive = true
			skip[i] = true
			i++
		}
		for {
			if i >= len(toks) || toks[i].kind != rtWord && toks[i].kind != rtQuoted {
				return r, "a WITH this port does not read"
			}
			name := toks[i]
			if name.kind == rtWord && !asciiOnly(name.text) || name.text == "" {
				return r, "a WITH name outside ASCII"
			}
			skip[i] = true
			i++
			if isOpTok(toks, i, "(") {
				// The column list: names and commas.
				end := closingParen(toks, i)
				if end < 0 {
					return r, "a WITH this port does not read"
				}
				for j := i; j <= end; j++ {
					if j > i && j < end && toks[j].kind != rtWord && toks[j].kind != rtQuoted && !isOpTok(toks, j, ",") {
						return r, "a WITH this port does not read"
					}
					skip[j] = true
				}
				i = end + 1
			}
			if !word(i, "as") {
				return r, "a WITH this port does not read"
			}
			skip[i] = true
			i++
			if word(i, "not") && word(i+1, "materialized") {
				skip[i], skip[i+1] = true, true
				i += 2
			} else if word(i, "materialized") {
				skip[i] = true
				i++
			}
			if !isOpTok(toks, i, "(") {
				return r, "a WITH this port does not read"
			}
			end := closingParen(toks, i)
			if end < 0 {
				return r, "a WITH this port does not read"
			}
			bodies[i] = len(r.ctes)
			r.ctes = append(r.ctes, cteDef{name: name.text, end: end})
			i = end + 1
			if isOpTok(toks, i, ",") {
				skip[i] = true
				i++
				continue
			}
			break
		}
	}

	const (
		fQuery = iota // a query: SELECT ... FROM ...
		fItems        // a parenthesized join in a FROM clause
		fExpr         // an expression, or a call's arguments
	)
	const (
		cStart = iota // before SELECT
		cSelect       // the select list
		cFrom         // the FROM clause
		cOther        // WHERE and after
		cSetOp        // after UNION, INTERSECT or EXCEPT
	)
	type frame struct {
		kind   int
		clause int
		// expect: in a FROM clause, the next token starts an item.
		expect bool
		// opener is the word before an expression's parenthesis.
		opener string
	}
	stack := []frame{{kind: fQuery, clause: cStart}}
	seenFunc := map[string]bool{}
	// isDistinctFrom: the FROM at i ends IS [NOT] DISTINCT FROM.
	isDistinctFrom := func(i int) (operator, distinct bool) {
		if !word(i-1, "distinct") {
			return false, false
		}
		return word(i-2, "is") || word(i-2, "not") && word(i-3, "is"), true
	}

	for i := 0; i < len(toks); i++ {
		if skip[i] {
			continue
		}
		t := toks[i]
		top := &stack[len(stack)-1]
		expecting := top.expect && (top.kind == fItems || top.kind == fQuery && top.clause == cFrom)
		switch {
		case t.kind == rtOp && t.text == "(":
			j := i + 1
			for isOpTok(toks, j, "(") {
				j++
			}
			isQuery := word(j, "select") || word(j, "values")
			if _, body := bodies[i]; body {
				if !isQuery {
					return r, "a WITH whose body is not a SELECT"
				}
				stack = append(stack, frame{kind: fQuery, clause: cStart})
				continue
			}
			if expecting {
				top.expect = false
				if isQuery {
					stack = append(stack, frame{kind: fQuery, clause: cStart})
				} else {
					stack = append(stack, frame{kind: fItems, clause: cFrom, expect: true})
				}
				continue
			}
			opener := ""
			if i > 0 {
				switch prev := toks[i-1]; prev.kind {
				case rtQuoted:
					return r, fmt.Sprintf("the function %q", prev.text)
				case rtWord:
					opener = prev.text
					switch {
					case isOpTok(toks, i-2, "."):
						return r, "a function written with its schema"
					case syntaxParen[opener]:
					case pureFuncs[opener] || alsoPure[opener]:
						if !seenFunc[opener] {
							seenFunc[opener] = true
							r.funcs = append(r.funcs, opener)
						}
					default:
						return r, fmt.Sprintf("the function %s()", opener)
					}
				}
			}
			if isQuery {
				stack = append(stack, frame{kind: fQuery, clause: cStart})
			} else {
				stack = append(stack, frame{kind: fExpr, opener: opener})
			}
		case t.kind == rtOp && t.text == ")":
			if len(stack) == 1 {
				return r, "a parenthesis that closes nothing"
			}
			if expecting {
				return r, "a FROM clause this port does not read"
			}
			stack = stack[:len(stack)-1]
		case t.kind == rtOp && t.text == ":":
			// The copy's engine reads "alias: table" in FROM; nothing here
			// does.
			return r, "a colon"
		case t.kind == rtWord && !expecting && isOpTok(toks, i-1, "."):
			// A name after a dot is a column's, whatever word it is: a
			// column may be called "order" or "from" there.
		case t.kind == rtOp && t.text == ",":
			if top.kind == fItems || top.kind == fQuery && top.clause == cFrom {
				if top.expect {
					return r, "a FROM clause this port does not read"
				}
				top.expect = true
			}
		case t.kind == rtWord && unprovenWords[t.text] != "":
			return r, unprovenWords[t.text]
		case t.kind == rtWord && t.text == "with":
			if !word(i+1, "time") {
				return r, "a WITH inside the statement"
			}
		case t.kind == rtWord && t.text == "select":
			if top.kind != fQuery || top.clause != cStart && top.clause != cSetOp {
				return r, "SELECT where this port does not expect one"
			}
			top.clause = cSelect
		case t.kind == rtWord && t.text == "values":
			if top.kind != fQuery || top.clause != cStart && top.clause != cSetOp {
				return r, "VALUES where this port does not expect it"
			}
			top.clause = cOther
		case t.kind == rtWord && t.text == "from":
			operator, distinct := isDistinctFrom(i)
			switch {
			case operator:
			case distinct:
				return r, "DISTINCT FROM this port does not read"
			case top.kind == fExpr && fromInCall[top.opener]:
			case top.kind == fQuery && top.clause == cSelect:
				top.clause, top.expect = cFrom, true
			default:
				return r, "FROM where this port does not expect one"
			}
		case t.kind == rtWord && t.text == "join":
			if !(top.kind == fItems || top.kind == fQuery && top.clause == cFrom) || top.expect {
				return r, "JOIN where this port does not expect one"
			}
			top.expect = true
		case t.kind == rtWord && top.kind == fQuery && !expecting &&
			(t.text == "where" || t.text == "group" || t.text == "having" || t.text == "order" || t.text == "limit" ||
				t.text == "offset" || t.text == "for" || t.text == "window"):
			top.clause = cOther
		case t.kind == rtWord && top.kind == fQuery && !expecting &&
			(t.text == "union" || t.text == "intersect" || t.text == "except"):
			top.clause = cSetOp
		case expecting:
			// A FROM item: a name, written with up to three parts.
			if t.kind != rtWord && t.kind != rtQuoted {
				return r, "something other than a table's name in FROM"
			}
			ref := relRef{at: i}
			j := i
			for {
				p := toks[j]
				if p.text == "" || p.kind == rtWord && !asciiOnly(p.text) {
					return r, "a table's name outside ASCII, or empty"
				}
				ref.parts = append(ref.parts, p)
				j++
				if isOpTok(toks, j, ".") && j+1 < len(toks) && (toks[j+1].kind == rtWord || toks[j+1].kind == rtQuoted) && len(ref.parts) < 3 {
					j++
					continue
				}
				break
			}
			if isOpTok(toks, j, ".") {
				return r, "a name of more than three parts in FROM"
			}
			if isOpTok(toks, j, "(") {
				return r, "a function in FROM"
			}
			r.refs = append(r.refs, ref)
			if len(r.refs) > readsMaxRefs {
				return r, "more tables than this port follows for one statement"
			}
			top.expect = false
			i = j - 1
		}
	}
	if len(stack) != 1 {
		return r, "a parenthesis left open"
	}
	if top := stack[0]; top.expect {
		return r, "a FROM clause this port does not read"
	}
	return r, ""
}

// proofSQL is the statement the source confirms a stmtReads with, right
// behind the statement itself and on its connection: one cell, NULL when the
// tables it read cannot be told from its text, else a JSON array of
// [schema, table] for each table it names, as the source's catalog names
// them. See the top of this file for what each line rules out.
//
// 16384 is the first OID PostgreSQL gives an object made after initdb; 12
// and 13 are the languages internal and C. An aggregate of the user's is
// internal by its catalog row, whatever its functions are written in.
func proofSQL(r stmtReads) string {
	var b strings.Builder
	b.WriteString("SELECT CASE WHEN" +
		" EXISTS (SELECT 1 FROM pg_catalog.pg_operator o JOIN pg_catalog.pg_proc p ON p.oid = o.oprcode WHERE o.oid >= 16384 AND p.prolang NOT IN (12, 13))" +
		" OR EXISTS (SELECT 1 FROM pg_catalog.pg_cast k JOIN pg_catalog.pg_proc p ON p.oid = k.castfunc WHERE k.oid >= 16384 AND p.prolang NOT IN (12, 13))" +
		" OR EXISTS (SELECT 1 FROM pg_catalog.pg_constraint k WHERE k.contypid <> 0 AND k.oid >= 16384)" +
		" OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_type y ON y.oid = p.proargtypes[0] WHERE p.oid >= 16384 AND p.prolang NOT IN (12, 13) AND p.pronargs = 1 AND y.typtype IN ('c', 'p'))")
	if len(r.funcs) > 0 {
		names := make([]string, len(r.funcs))
		for i, f := range r.funcs {
			names[i] = quoteLiteral(f)
		}
		b.WriteString(" OR EXISTS (SELECT 1 FROM pg_catalog.pg_proc p WHERE p.oid >= 16384 AND (p.prolang NOT IN (12, 13) OR p.prokind = 'a') AND p.proname = ANY (ARRAY[" +
			strings.Join(names, ", ") + "]::pg_catalog.name[]))")
	}
	b.WriteString(" THEN NULL ELSE ")
	if len(r.refs) == 0 {
		b.WriteString("'[]'::pg_catalog.text END")
		return b.String()
	}
	// A name of a WITH query in scope may be left unresolved: when a table
	// has that name too, it is marked as well.
	rows := make([]string, len(r.refs))
	for i, ref := range r.refs {
		optional := "false"
		if r.isCTE(ref) {
			optional = "true"
		}
		rows[i] = "(" + quoteLiteral(ref.written()) + ", " + optional + ")"
	}
	b.WriteString("(SELECT CASE WHEN pg_catalog.count(*) = pg_catalog.count(*) FILTER (WHERE r.ok) THEN" +
		" COALESCE(pg_catalog.json_agg(pg_catalog.json_build_array(r.nspname, r.relname)) FILTER (WHERE r.relname IS NOT NULL), '[]'::pg_catalog.json)::pg_catalog.text END" +
		" FROM (SELECT n.nspname, c.relname, CASE WHEN c.oid IS NULL THEN v.optional" +
		" ELSE c.relkind = 'r' AND c.relpersistence = 'p' AND NOT c.relhassubclass AND NOT c.relrowsecurity END AS ok" +
		" FROM (VALUES " + strings.Join(rows, ", ") + ") AS v(ref, optional)" +
		" LEFT JOIN pg_catalog.pg_class c ON c.oid = pg_catalog.to_regclass(v.ref)" +
		" LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace) r) END")
	return b.String()
}

// parseProof reads proofSQL's cell: the tables, ok false when the source did
// not confirm them (NULL, or anything this does not read).
func parseProof(cell []byte) (tables [][2]string, ok bool) {
	if len(cell) == 0 {
		return nil, false
	}
	var raw [][]string
	if err := json.Unmarshal(cell, &raw); err != nil {
		return nil, false
	}
	for _, t := range raw {
		if len(t) != 2 || t[1] == "" {
			return nil, false
		}
		tables = append(tables, [2]string{t[0], t[1]})
	}
	return tables, true
}

// sourceReadsMax bounds the tables a connection keeps by name as read from
// its source: past it, every table is.
const sourceReadsMax = 64

// sourceProof is one statement the source is answering whose tables may be
// confirmed: what its text reads, the search_path it ran under, and whether
// the source sent the client anything of it.
type sourceProof struct {
	reads    stmtReads
	path     string
	answered bool
}

// sourceReads is what a connection read from its source.
type sourceReads struct {
	// all: every table (a statement whose tables are not known).
	all bool
	// tables are the tables read, by tableKey.
	tables map[string]bool
	// pending are the statements of an extended batch the source answered
	// and has not confirmed yet (confirmPending, at the batch's Sync). While
	// there is one, every table counts as read.
	pending []*sourceProof
}

// tableKey is what a table is kept and compared by: its schema and name
// without regard to case. PostgreSQL tells "Orders" from "orders" and the
// copy does not, so two tables that differ by case alone count as one:
// more is marked, never less.
func tableKey(schema, name string) string {
	return strings.ToLower(schema) + "\x00" + strings.ToLower(name)
}

// any reports whether the source answered this connection anything.
func (r *sourceReads) any() bool { return r.all || len(r.pending) > 0 || len(r.tables) > 0 }

// markAll marks every table.
func (r *sourceReads) markAll() {
	r.all, r.tables, r.pending = true, nil, nil
}

// add marks tables, or every table past sourceReadsMax.
func (r *sourceReads) add(tables [][2]string) {
	if r.all {
		return
	}
	if r.tables == nil {
		r.tables = map[string]bool{}
	}
	for _, t := range tables {
		r.tables[tableKey(t[0], t[1])] = true
	}
	if len(r.tables) > sourceReadsMax {
		r.markAll()
	}
}

// names are the tables marked, sorted, for the log.
func (r *sourceReads) names() []string {
	out := make([]string, 0, len(r.tables))
	for k := range r.tables {
		out = append(out, strings.ReplaceAll(k, "\x00", "."))
	}
	sort.Strings(out)
	return out
}

// proveReads is the proof a statement about to be sent to the source may be
// given, nil when its tables cannot be told and every table is marked: the
// connection already marks every table, the host keeps no rule the marks
// serve, the text is not read whole, or the search_path names pg_catalog or
// pg_temp, which then no longer come first.
func (s *session) proveReads(qstr string) *sourceProof {
	if s.src.all || s.bound == nil || s.bound.RunAge == nil && s.bound.CopyWait == nil {
		return nil
	}
	for _, p := range s.set.searchPath {
		if p == "pg_catalog" || p == "pg_temp" {
			return nil
		}
	}
	reads, why := readsOf(qstr, nil)
	if why != "" {
		s.logger.Debug("pgshim: a statement for the source marks every table as read from it", "why", why)
		return nil
	}
	return &sourceProof{reads: reads, path: showSearchPath(s.set.searchPath)}
}

// confirmProof takes the source's answer to a proof's proofSQL: the tables
// are marked when the source confirmed them, every table when it did not.
// Nothing is marked for a statement the source sent the client nothing of.
func (s *session) confirmProof(p *sourceProof, cell []byte, perr *pgErr) {
	if p == nil || !p.answered || s.src.all {
		return
	}
	tables, ok := parseProof(cell)
	if perr != nil || !ok || len(s.src.pending) != 1 || s.src.pending[0] != p {
		// Not confirmed, or statements of a batch that never reached its
		// Sync are still waiting to be.
		s.logger.Debug("pgshim: the source did not confirm which tables a statement read; every table counts as read from it")
		s.src.markAll()
		return
	}
	s.src.pending = nil
	s.src.add(tables)
}

// confirmPending confirms, once an extended batch is over, the statements
// of it the source answered: one proof for all of them, on the batch's
// connection. Whatever keeps it from being confirmed marks every table.
func (s *session) confirmPending(up *upstream) *errSourceLost {
	if len(s.src.pending) == 0 || s.src.all {
		return nil
	}
	var all stmtReads
	seen := map[string]bool{}
	for _, p := range s.src.pending {
		if up.sent == nil || up.sent["search_path"] != p.path {
			// The names were read under another search_path than the
			// source has now.
			s.src.markAll()
			return nil
		}
		for _, ref := range p.reads.refs {
			if p.reads.isCTE(ref) {
				// A WITH name of one statement is a table's in another.
				continue
			}
			all.refs = append(all.refs, relRef{parts: ref.parts})
		}
		for _, f := range p.reads.funcs {
			if !seen[f] {
				seen[f] = true
				all.funcs = append(all.funcs, f)
			}
		}
	}
	if len(all.refs) > readsMaxRefs || up.unsafe {
		s.src.markAll()
		return nil
	}
	cell, perr, lost := s.cellOnSource(up, proofSQL(all))
	if lost != nil {
		s.src.markAll()
		return lost
	}
	tables, ok := parseProof(cell)
	if perr != nil || !ok {
		s.logger.Debug("pgshim: the source did not confirm which tables a batch read; every table counts as read from it")
		s.src.markAll()
		return nil
	}
	s.src.pending = nil
	s.src.add(tables)
	return nil
}

// apartFromSource reports whether the statement the copy is about to run
// reads no table this connection read from its source, so that it neither
// waits for the copy nor is held back (#2985). named is how many tables of
// the copy it names. False whenever that cannot be told: its text is not
// read whole, a name in it is not a table of the copy's catalog, or the
// catalog cannot be read.
func (s *session) apartFromSource(qstr string) (apart bool, named int) {
	if s.src.all || len(s.src.pending) > 0 || s.bound == nil || s.bound.Catalog == nil {
		return false, 0
	}
	reads, why := readsOf(s.copyText(qstr), copyFuncs)
	if why != "" {
		return false, 0
	}
	v, perr := s.catalog()
	if perr != nil {
		return false, 0
	}
	hit := map[string]bool{}
	for _, ref := range reads.refs {
		schema, name := "", ref.parts[len(ref.parts)-1].text
		switch len(ref.parts) {
		case 1:
		case 2:
			schema = ref.parts[0].text
		default:
			return false, 0
		}
		found := false
		for i := range v.tables {
			t := &v.tables[i]
			// As the copy matches names: without regard to case, and a name
			// without a schema in every schema.
			if strings.EqualFold(t.Name, name) && (schema == "" || strings.EqualFold(t.Schema, schema)) {
				found = true
				hit[tableKey(t.Schema, t.Name)] = true
			}
		}
		if !found && !reads.isCTE(ref) {
			return false, 0
		}
	}
	for k := range hit {
		if s.src.tables[k] {
			return false, len(hit)
		}
	}
	return true, len(hit)
}
