package views

import (
	"fmt"
	"slices"
	"strings"
)

// A schema holds views. Everything else this file creates has ONE name per
// DuckDB session whatever schema the views are in: an attached catalog, a
// secret, a session variable. So when Input.Schema is set, each of those is
// derived from it here, and with it empty each keeps the name it always had.
//
// The S3 secret is the one name deliberately NOT derived. It carries no
// server's identity (it is "use the credential chain"), so two files emit the
// same statement and the second is a no-op. Deriving it would leave two
// unscoped S3 secrets in one session and DuckDB choosing between them.

// maxSchemaNameLen bounds the name. DuckDB has no limit of its own that this
// protects; the bound is for the names derived from it and for the refusal,
// which echoes what was typed.
const maxSchemaNameLen = 63

// liveAliasSuffix is what the index's catalog is named after the schema.
const liveAliasSuffix = "_live"

// builtinSchemaNames are the names DuckDB already has. `main` is the default
// schema, so a file generated into it would replace the views of a file
// generated with no schema, without an error. `temp`, `system` and `memory`
// are catalogs every session has (`memory` for one opened with no file), and
// DuckDB refuses a two-part name whose first part is both a catalog and a
// schema as ambiguous. The last two are system schemas nothing can be created
// in. TestDuckDB_namesItAlreadyHas measures each.
var builtinSchemaNames = []string{"main", "temp", "system", "memory", "information_schema", "pg_catalog"}

// bareKeywords are the SQL keywords DuckDB cannot read unquoted as the first
// part of a name. The generated file quotes the schema, so any of them would
// LOAD; what breaks is the reader's own `SELECT ... FROM order.events`, after
// the file loaded cleanly. Most keywords are fine in that position (`data`,
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

// ValidateSchemaName reports why name cannot be Input.Schema, or nil.
//
// The rule is narrower than what DuckDB accepts, on purpose: a quoted
// identifier can hold almost anything, and the generator quotes. What is
// refused is every name that would load and then surprise the reader, and
// each refusal says which surprise.
//
// The checks run in the order that gives the most useful reason. Characters
// before case, so a name with a dash is told about the dash; case before the
// taken names, so there is no spelling of `main` that reaches the list.
func ValidateSchemaName(name string) error {
	if name == "" {
		return fmt.Errorf("the schema name is empty")
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
			return fmt.Errorf("schema name %s can hold only letters, digits and underscore (a to z, 0 to 9, _)", shown)
		}
	}
	if name[0] >= '0' && name[0] <= '9' {
		return fmt.Errorf("schema name %s must start with a letter or an underscore: DuckDB cannot read a name that starts with a digit unless every query quotes it", shown)
	}
	if upper {
		return fmt.Errorf("schema name %s must be lowercase: DuckDB reads %s and %s as the same schema, so two servers named that way would replace each other's views",
			shown, shown, shownName(strings.ToLower(name)))
	}
	if len(name) > maxSchemaNameLen {
		return fmt.Errorf("schema name %s is too long: %d characters, and the most is %d", shown, len(name), maxSchemaNameLen)
	}
	if slices.Contains(builtinSchemaNames, name) {
		return fmt.Errorf("schema name %s is one DuckDB already has, so views created under it would replace or collide with what is there. Use the server's name", shown)
	}
	if slices.Contains(bareKeywords, name) {
		return fmt.Errorf("schema name %s is a SQL keyword DuckDB cannot read unquoted, so `FROM %s.events` would be a syntax error. Use the server's name", shown, name)
	}
	if strings.HasSuffix(name, liveAliasSuffix) {
		return fmt.Errorf("schema name %s ends in %s, which is how a server's index is attached (schema %s attaches %s), and DuckDB refuses a name that is both. Pick a name with another ending",
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

// viewRef is the name a view is CREATED under: quoted, and qualified with the
// schema when there is one.
func (in Input) viewRef(name string) string {
	if in.Schema == "" {
		return quoteIdent(name)
	}
	return quoteIdent(in.Schema) + "." + quoteIdent(name)
}

// viewLabel is the name a COMMENT calls a view by, the way a reader types it.
func (in Input) viewLabel(name string) string {
	if in.Schema == "" {
		return name
	}
	return in.Schema + "." + name
}

// liveAlias is the catalog the index is attached as. DuckDB attaches a
// database once per name, so two files that both said "bintrail_live" would
// not load into one session.
func (in Input) liveAlias() string {
	if in.Schema == "" {
		return liveAttachAlias
	}
	return in.Schema + liveAliasSuffix
}

// liveSecret is the secret that ATTACH reads. It names one server's index
// host, so a shared name would hold whichever server's file ran last.
func (in Input) liveSecret() string {
	return in.sessionName(liveSecretName)
}

// sessionName derives a session-wide name (a variable, a secret) from the
// schema. The following state views read their variable on EVERY query, not
// when the file is loaded, so a shared one would point every server's views at
// the snapshot of whichever file was loaded last: views listed under one
// server, returning another's rows.
func (in Input) sessionName(base string) string {
	if in.Schema == "" {
		return base
	}
	// sanitizeIdent because a variable's name is written bare and inside a
	// string literal, where quoting the schema is not available. A validated
	// name comes out unchanged.
	return base + "_" + sanitizeIdent(in.Schema)
}

// writeSchema emits the schema the views below are created in.
func writeSchema(b *strings.Builder, in Input) {
	if in.Schema == "" {
		return
	}
	s := commentSafe(in.Schema)
	fmt.Fprintf(b, "-- Every view below is created in schema %s, so this file can be loaded\n", s)
	b.WriteString("-- into one database next to the files of other servers, each generated with\n")
	fmt.Fprintf(b, "-- its own name. Query them as %s.<view>.\n", s)
	fmt.Fprintf(b, "-- The database itself must not be named %s: DuckDB names a database after\n", s)
	fmt.Fprintf(b, "-- its file, so %s.db would make every name here ambiguous and the load would\n", s)
	b.WriteString("-- stop at the first view. Any other file name works.\n")
	writeSchemaStatement(b, in)
	b.WriteString("\n")
}

// writeSchemaStatement is the statement alone, for the entry point that emits
// no comments of its own.
func writeSchemaStatement(b *strings.Builder, in Input) {
	if in.Schema == "" {
		return
	}
	fmt.Fprintf(b, "CREATE SCHEMA IF NOT EXISTS %s;\n", quoteIdent(in.Schema))
}
