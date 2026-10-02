package views

import (
	"fmt"
	"slices"
	"strings"
)

// A database (Input.Database) holds one server's views. Everything else this
// file creates has ONE name per DuckDB session whatever database the views are
// in: an attached catalog, a secret, a session variable. So when
// Input.Database is set, each of those is derived from it here, and with it
// empty each keeps the name it always had.
//
// The S3 secret is the one name deliberately NOT derived. It carries no
// server's identity (it is "use the credential chain"), so two files emit the
// same statement and the second is a no-op. Deriving it would leave two
// unscoped S3 secrets in one session and DuckDB choosing between them.
//
// Why a DATABASE per server and not a schema, as `--schema` was before #2013:
// the source's schemas are DuckDB schemas now (demo.prices), so the server
// needs the level above them. In DuckDB that is an attached database, queried
// as wp.demo.prices.

// maxSchemaNameLen bounds the name. DuckDB has no limit of its own that this
// protects; the bound is for the names derived from it and for the refusal,
// which echoes what was typed.
const maxSchemaNameLen = 63

// liveAliasSuffix is what the index's catalog is named after the database.
const liveAliasSuffix = "_live"

// builtinSchemaNames are the names DuckDB already has. `main` is the default
// schema, so `main.events` would mean two things. `temp`, `system` and
// `memory` are databases every session has (`memory` for one opened with no
// file), so attaching another under that name fails. The last two are system
// schemas nothing can be created in. TestDuckDB_namesItAlreadyHas measures
// each.
var builtinSchemaNames = []string{"main", "temp", "system", "memory", "information_schema", "pg_catalog"}

// bareKeywords are the SQL keywords DuckDB cannot read unquoted as the first
// part of a name. The generated file quotes every name, so any of them would
// LOAD; what breaks is the reader's own `SELECT ... FROM order.events`, after
// the file loaded cleanly. typedIdent uses the same list to decide which source
// names a reader has to quote. Most keywords are fine in that position (`data`,
// `index`, `user`) and are not here.
//
// Not a list to maintain by hand: TestDuckDB_keywordsThatCannotBeTypedBare
// asks the linked DuckDB about every keyword it has and fails on a difference
// in either direction.
var bareKeywords = []string{
	"all", "analyse", "analyze", "and", "anti", "any", "array", "as",
	"asc", "asof", "asymmetric", "at", "authorization", "binary", "both", "by",
	"case", "cast", "check", "collate", "collation", "column", "concurrently", "constraint",
	"create", "cross", "default", "deferrable", "desc", "describe", "distinct", "do",
	"else", "end", "except", "false", "fetch", "for", "foreign", "freeze",
	"from", "full", "glob", "group", "having", "ilike", "in", "initially",
	"inner", "intersect", "into", "is", "isnull", "join", "lambda", "lateral",
	"leading", "left", "like", "limit", "natural", "not", "notnull", "null",
	"offset", "on", "only", "or", "order", "outer", "overlaps", "pivot",
	"pivot_longer", "pivot_wider", "placing", "positional", "primary", "qualify", "references", "returning",
	"right", "select", "semi", "show", "similar", "some", "summarize", "symmetric",
	"table", "tablesample", "then", "to", "trailing", "true", "union", "unique",
	"unpack", "unpivot", "using", "variadic", "verbose", "when", "where", "window",
	"with",
}

// ValidateDatabaseName reports why name cannot be Input.Database, or nil.
//
// The rule is narrower than what DuckDB accepts, on purpose: a quoted
// identifier can hold almost anything, and the generator quotes. What is
// refused is every name that would load and then surprise the reader, and
// each refusal says which surprise.
//
// The checks run in the order that gives the most useful reason. Characters
// before case, so a name with a dash is told about the dash; case before the
// taken names, so there is no spelling of `main` that reaches the list.
func ValidateDatabaseName(name string) error {
	if name == "" {
		return fmt.Errorf("the database name is empty")
	}
	shown := shownName(name)
	upper := false
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
		case c >= 'A' && c <= 'Z':
			upper = true
		default:
			// Bytes, not runes: every byte of a multi-byte character is
			// outside the three ranges, so a non-ASCII letter stops here
			// whatever it would lowercase to.
			return fmt.Errorf("database name %s can hold only letters, digits and underscore (a to z, 0 to 9, _)", shown)
		}
	}
	if name[0] >= '0' && name[0] <= '9' {
		return fmt.Errorf("database name %s must start with a letter or an underscore: DuckDB cannot read a name that starts with a digit unless every query quotes it", shown)
	}
	if upper {
		return fmt.Errorf("database name %s must be lowercase: DuckDB reads %s and %s as the same database, so two servers named that way would replace each other's views",
			shown, shown, shownName(strings.ToLower(name)))
	}
	if len(name) > maxSchemaNameLen {
		return fmt.Errorf("database name %s is too long: %d characters, and the most is %d", shown, len(name), maxSchemaNameLen)
	}
	if slices.Contains(builtinSchemaNames, name) {
		return fmt.Errorf("database name %s is one DuckDB already has, so attaching another one under it fails or makes every name ambiguous. Use the server's name", shown)
	}
	if slices.Contains(bareKeywords, name) {
		return fmt.Errorf("database name %s is a SQL keyword DuckDB cannot read unquoted, so `FROM %s.events` would be a syntax error. Use the server's name", shown, name)
	}
	if strings.HasSuffix(name, liveAliasSuffix) {
		return fmt.Errorf("database name %s ends in %s, which is how a server's index is attached (database %s attaches %s), and DuckDB refuses a name that is both. Pick a name with another ending",
			shown, liveAliasSuffix, shownName(strings.TrimSuffix(name, liveAliasSuffix)), shown)
	}
	return nil
}

// shownName quotes a refused name for the terminal: %q keeps a newline or a
// tab on the line, and the cut keeps a very long value from burying the reason
// it was refused for.
func shownName(name string) string {
	const most = 40
	if len(name) > most {
		return fmt.Sprintf("%q...", strings.ToValidUTF8(name[:most], ""))
	}
	return fmt.Sprintf("%q", name)
}

// viewRef is the name the events view is CREATED under: quoted, and
// qualified with the server's database when there is one. Three parts there,
// never two: DuckDB reads `wp.events` as schema wp of the CURRENT database
// first, which is not where it belongs.
func (in Input) viewRef(name string) string {
	if in.Database == "" {
		return quoteIdent(name)
	}
	return quoteIdent(in.Database) + `."main".` + quoteIdent(name)
}

// viewLabel is the name a COMMENT calls the events view by, the way a reader
// types it.
func (in Input) viewLabel(name string) string {
	if in.Database == "" {
		return name
	}
	return in.Database + "." + name
}

// liveAlias is the catalog the index is attached as. DuckDB attaches a
// database once per name, so two files that both said "bintrail_live" would
// not load into one session.
func (in Input) liveAlias() string {
	if in.Database == "" {
		return liveAttachAlias
	}
	return in.Database + liveAliasSuffix
}

// liveSecret is the secret that ATTACH reads. It names one server's index
// host, so a shared name would hold whichever server's file ran last.
func (in Input) liveSecret() string {
	return in.sessionName(liveSecretName)
}

// sessionName derives a session-wide name (a variable, a secret) from the
// database. The following state views read their variable on EVERY query, not
// when the file is loaded, so a shared one would point every server's views at
// the snapshot of whichever file was loaded last: views listed under one
// server, returning another's rows.
func (in Input) sessionName(base string) string {
	if in.Database == "" {
		return base
	}
	// sanitizeIdent because a variable's name is written bare and inside a
	// string literal, where quoting is not available. A validated name comes
	// out unchanged.
	return base + "_" + sanitizeIdent(in.Database)
}

// writeDatabase emits the database the views below are created in.
func writeDatabase(b *strings.Builder, in Input) {
	if in.Database == "" {
		return
	}
	d := commentSafe(in.Database)
	fmt.Fprintf(b, "-- Every view below is created in database %s, so this file can be loaded\n", d)
	b.WriteString("-- into one DuckDB session next to the files of other servers, each generated\n")
	if in.rendersEvents() {
		fmt.Fprintf(b, "-- with its own name. Query the tables as %s.<schema>.<table> and the change\n", d)
		fmt.Fprintf(b, "-- log as %s.events.\n", d)
	} else {
		fmt.Fprintf(b, "-- with its own name. Query the tables as %s.<schema>.<table>.\n", d)
	}
	b.WriteString("--\n")
	fmt.Fprintf(b, "-- %s is attached in memory unless a database by that name is already open,\n", d)
	b.WriteString("-- so the views last as long as the session. To keep them in a file, attach it\n")
	fmt.Fprintf(b, "-- under that name first (ATTACH '%s.db' AS %s; then .read this file), or open\n", d, d)
	fmt.Fprintf(b, "-- %s.db itself (duckdb %s.db).\n", d, d)
	writeDatabaseStatement(b, in)
	b.WriteString("\n")
}

// writeDatabaseStatement is the statement alone, for the entry point that
// emits no comments of its own.
//
// IF NOT EXISTS is what lets a reader keep the views on disk: a database file
// already open under this name (attached by hand, or opened as `duckdb wp.db`,
// which DuckDB names wp) is used as it is, and the views land in it.
func writeDatabaseStatement(b *strings.Builder, in Input) {
	if in.Database == "" {
		return
	}
	fmt.Fprintf(b, "ATTACH IF NOT EXISTS ':memory:' AS %s;\n", quoteIdent(in.Database))
}
