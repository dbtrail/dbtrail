package baseline

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Real names (#2006). mydumper does not put every table name in a file name:
// measured with mydumper 1.0.3-1 against MariaDB 11.8, a name holding a dot,
// a slash, "@", a non-ASCII letter or 64 characters is written as a made-up
// "mydumper_N", and so is a real table already called mydumper_N (the numbers
// are reassigned on every dump). A schema name with a dot gets the same
// treatment. A space or "-" is kept as is. The real name is still in the dump:
// the -schema.sql file's CREATE TABLE names the table, metadata repeats it as
// real_table_name, and <db>-schema-create.sql's CREATE DATABASE names the
// schema when mydumper writes that file (it does not under --regex).
//
// The converter used to take the file name for the table name, so a dotted
// table entered the snapshot as "mydumper_0": a table that does not exist,
// with the real one missing, and every refresh then refused it.

// generatedName matches the names mydumper makes up.
var generatedName = regexp.MustCompile(`^mydumper_[0-9]+$`)

// longestDeltaSuffix is the longest thing appended to a table's name to make
// a file name in a snapshot: a range pair of a table delta,
// "<table>.<6 digits>-<6 digits>.upserts".
var longestDeltaSuffix = len("." + strings.Repeat("0", TableDeltaSeqWidth) + "-" +
	strings.Repeat("0", TableDeltaSeqWidth) + TableDeltaUpsertsSuffix)

// maxFileNameBytes is the longest file name the common file systems take.
const maxFileNameBytes = 255

// storableName reports why a schema or table name cannot be a file name in
// a snapshot (<schema>/<table>.parquet and the files beside it), nil when it
// can. A separator would put the file in another directory, and "." or ".."
// would name a directory itself.
func storableName(name string) error {
	switch {
	case name == "":
		return errors.New("the name is empty")
	case name == "." || name == "..":
		return fmt.Errorf("the name %q is a directory name", name)
	case strings.ContainsRune(name, '/'):
		return fmt.Errorf(`the name holds a "/", which a snapshot cannot store as a file name`)
	case strings.ContainsRune(name, '\\'):
		// Storable on Linux, but the snapshot's change files (table deltas)
		// are matched by name with "\" taken as a folder separator
		// (TableDeltaNameFilter): table `a\b` would read table `b`'s
		// changes as its own, and `b` would read `a\b`'s.
		return fmt.Errorf(`the name holds a "\", which the snapshot's change files treat as a folder separator, so this table and the table named after the "\" would read each other's changes`)
	case strings.ContainsRune(name, 0):
		return errors.New("the name holds a NUL byte")
	case len(name)+longestDeltaSuffix > maxFileNameBytes:
		return fmt.Errorf("the name is %d bytes long, and a snapshot's file names for it would pass the %d bytes a file name can hold",
			len(name), maxFileNameBytes)
	}
	return nil
}

// createdName reads the name a CREATE <kind> statement (kind "TABLE" or
// "DATABASE") in a mydumper schema file gives, unquoted. The statement is
// the file's first CREATE; a file whose first CREATE is something else, or
// that has none, is an error.
func createdName(path, kind string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(strings.ToUpper(line), "CREATE ") {
			continue
		}
		rest := skipSpaceAndComments(line[len("CREATE "):])
		if !hasWordPrefix(rest, kind) {
			return "", fmt.Errorf("%s: its first CREATE is not a CREATE %s", filepath.Base(path), kind)
		}
		rest = skipSpaceAndComments(rest[len(kind):])
		if hasWordPrefix(rest, "IF") {
			rest = skipSpaceAndComments(rest[len("IF"):])
			if !hasWordPrefix(rest, "NOT") {
				return "", fmt.Errorf("%s: cannot read the CREATE %s statement", filepath.Base(path), kind)
			}
			rest = skipSpaceAndComments(rest[len("NOT"):])
			if !hasWordPrefix(rest, "EXISTS") {
				return "", fmt.Errorf("%s: cannot read the CREATE %s statement", filepath.Base(path), kind)
			}
			rest = skipSpaceAndComments(rest[len("EXISTS"):])
		}
		name, rest, ok := readIdent(rest)
		if !ok {
			return "", fmt.Errorf("%s: cannot read the name in its CREATE %s", filepath.Base(path), kind)
		}
		// `db`.`table`: the table is the second part.
		if strings.HasPrefix(rest, ".") {
			if name, _, ok = readIdent(rest[1:]); !ok {
				return "", fmt.Errorf("%s: cannot read the name in its CREATE %s", filepath.Base(path), kind)
			}
		}
		return name, nil
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("read %s: %w", filepath.Base(path), err)
	}
	return "", fmt.Errorf("%s holds no CREATE %s", filepath.Base(path), kind)
}

// hasWordPrefix reports whether s starts with word (any case) followed by
// something that ends a word.
func hasWordPrefix(s, word string) bool {
	if len(s) < len(word) || !strings.EqualFold(s[:len(word)], word) {
		return false
	}
	if len(s) == len(word) {
		return true
	}
	c := s[len(word)]
	return c == ' ' || c == '\t' || c == '`' || c == '"' || c == '/' || c == '('
}

// skipSpaceAndComments drops leading white space and /*...*/ comments, such
// as mydumper's /*!32312 IF NOT EXISTS*/.
func skipSpaceAndComments(s string) string {
	for {
		s = strings.TrimLeft(s, " \t")
		if !strings.HasPrefix(s, "/*") {
			return s
		}
		end := strings.Index(s, "*/")
		if end < 0 {
			return s
		}
		s = s[end+2:]
	}
}

// readIdent reads one identifier: quoted with backticks or double quotes
// (the quote doubled inside), or bare up to a space, "(" or ".". It returns
// the unquoted name and what follows it; ok is false for an empty or
// unterminated name.
func readIdent(s string) (name, rest string, ok bool) {
	if s == "" {
		return "", "", false
	}
	if q := s[0]; q == '`' || q == '"' {
		var b strings.Builder
		for i := 1; i < len(s); i++ {
			if s[i] != q {
				b.WriteByte(s[i])
				continue
			}
			if i+1 < len(s) && s[i+1] == q {
				b.WriteByte(q)
				i++
				continue
			}
			return b.String(), s[i+1:], b.Len() > 0
		}
		return "", "", false
	}
	end := strings.IndexAny(s, " \t(.;")
	if end < 0 {
		end = len(s)
	}
	return s[:end], s[end:], end > 0
}

// dumpKey is a table's (schema, table) as the dump's file names spell them.
// A pair, not "db.table": with a dot in either half the joined string is
// ambiguous.
type dumpKey struct{ db, table string }

// dumpRealNames reads metadata's real_table_name per file key. A dump without
// a metadata file, or a mydumper that does not write the key, gives an empty
// map: the CREATE TABLE is then the only source, and it is always there.
//
// The value is written raw: measured with mydumper 1.0.3-1, a leading space,
// "=", "#", ";" and backslashes come back as they are (no key-file escapes);
// only a backtick is doubled, as in a quoted name.
func dumpRealNames(inputDir string) map[dumpKey]string {
	f, err := os.Open(filepath.Join(inputDir, "metadata"))
	if err != nil {
		// Only a missing file is expected (an older mydumper). The names
		// then come from each CREATE TABLE, which is always there.
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("cannot read the dump's metadata file; table names are read from each CREATE TABLE instead", "dir", inputDir, "error", err)
		}
		return nil
	}
	defer f.Close()
	out := make(map[dumpKey]string)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var section *dumpKey
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = nil
			db, rest, ok := readIdent(line[1 : len(line)-1])
			if ok && strings.HasPrefix(rest, ".") {
				if table, tail, ok := readIdent(rest[1:]); ok && tail == "" {
					section = &dumpKey{db, table}
				}
			}
			continue
		}
		if v, ok := strings.CutPrefix(line, "real_table_name="); ok && section != nil {
			out[*section] = strings.ReplaceAll(v, "``", "`")
		}
	}
	if err := sc.Err(); err != nil {
		slog.Warn("could not read the whole metadata file; table names it did not give are read from each CREATE TABLE", "dir", inputDir, "error", err)
	}
	return out
}

// LeftOutTable is a table the dump holds that the snapshot leaves out
// (#2006), because its real name cannot be read back or cannot be stored.
// Table is as much of the real name as is known, never a made-up one.
type LeftOutTable struct {
	Table  string
	Reason string
	// Schema and Name are the real schema and table as the source spells
	// them, each "" when it could not be read: what a comparison with the
	// source's own table list matches on. Table is for reading.
	Schema, Name string
}

// quoteName writes a name the way the messages below show it.
func quoteName(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// realDumpNames replaces the file-derived names DiscoverDump found with the
// real ones the dump records. A table whose real name cannot be read back or
// cannot be stored is left out, with why (never published under a made-up or
// wrong name); every other table is kept. Views get their real name too; they
// are only reported, never stored.
func realDumpNames(inputDir string, tables []TableFiles, views []SkippedView) ([]TableFiles, []SkippedView, []LeftOutTable) {
	meta := dumpRealNames(inputDir)

	// Schemas: <file db>-schema-create.sql, when mydumper wrote it.
	type dbName struct {
		name string
		err  error
	}
	dbs := make(map[string]dbName)
	realDB := func(fileDB string) dbName {
		if d, ok := dbs[fileDB]; ok {
			return d
		}
		// A schema name mydumper kept is the real one (what every earlier
		// release used). One it made up is read back from the CREATE
		// DATABASE it wrote beside the dump, when it wrote one.
		var d dbName
		createFile := filepath.Join(inputDir, fileDB+"-schema-create.sql")
		if !generatedName.MatchString(fileDB) {
			d.name = fileDB
		} else if name, err := createdName(createFile, "DATABASE"); err == nil {
			d.name = name
		} else if errors.Is(err, os.ErrNotExist) {
			d.err = fmt.Errorf("mydumper wrote its schema as %s, a made-up name, and wrote no CREATE DATABASE to read the real one from", quoteName(fileDB))
		} else {
			d.err = fmt.Errorf("mydumper wrote its schema as %s, a made-up name, and the real one cannot be read from %s: %w",
				quoteName(fileDB), filepath.Base(createFile), err)
		}
		if d.err == nil {
			switch {
			case strings.Contains(d.name, "."):
				d.err = fmt.Errorf("its schema name %s holds a dot, which a snapshot cannot store yet", quoteName(d.name))
			default:
				if err := storableName(d.name); err != nil {
					d.err = fmt.Errorf("its schema name %s cannot be stored: %w", quoteName(d.name), err)
				}
			}
		}
		dbs[fileDB] = d
		return d
	}

	var left []LeftOutTable
	type kept struct {
		tf   TableFiles
		name string // schema.table, the display name
		file string
	}
	var keep []kept
	count := make(map[dumpKey]int, len(tables))
	for _, tf := range tables {
		file := filepath.Base(tf.SchemaFile)
		d := realDB(tf.Database)
		// Which name is the table's: metadata's real_table_name when the
		// dump has one (checked against the CREATE TABLE); else the CREATE
		// TABLE when the file name is one mydumper made up or holds a dot;
		// else the file name, which is what mydumper writes whenever it can,
		// and what every earlier release used.
		created, cerr := createdName(tf.SchemaFile, "TABLE")
		recorded := meta[dumpKey{tf.Database, tf.Table}]
		unknown := "unknown name (dump file " + file + ")"
		var table, problem string
		switch {
		case (generatedName.MatchString(tf.Table) || strings.Contains(tf.Table, ".")) && cerr != nil:
			// A made-up or dotted file name needs the CREATE TABLE to
			// confirm the name: a metadata value alone can be cut short (a
			// name with a line break splits the metadata line), and a
			// dotted one may split at the wrong dot.
			problem = fmt.Sprintf("its real name cannot be read from the dump (%v)", cerr)
		case recorded != "" && (cerr != nil || created != recorded):
			problem = fmt.Sprintf("the dump names it two ways: %s in its CREATE TABLE and %s in its metadata", quoteName(created), quoteName(recorded))
		case recorded != "":
			table = recorded
		case generatedName.MatchString(tf.Table):
			table = created
		case strings.Contains(tf.Table, ".") && created != tf.Table:
			// A mydumper that writes a dotted schema name into the file
			// name: "my.db.t" splits as schema "my", table "db.t". The
			// CREATE TABLE says "t", so the split is wrong.
			table = created
			problem = fmt.Sprintf("the dump file name reads as %s.%s but the table is %s, so its schema name holds a dot, which a snapshot cannot store yet",
				quoteName(tf.Database), quoteName(tf.Table), quoteName(created))
		default:
			table = tf.Table
		}
		name := unknown
		if table != "" {
			name = table
			if d.name != "" {
				name = d.name + "." + table
			}
		}
		if problem == "" && d.err != nil {
			problem = d.err.Error()
		}
		if problem == "" {
			if err := storableName(table); err != nil {
				problem = err.Error()
			}
		}
		if problem != "" {
			// What the source calls it, as far as the dump says: the table
			// name metadata recorded when the CREATE TABLE could not give one.
			match := table
			if match == "" {
				match = recorded
			}
			left = append(left, LeftOutTable{Table: name, Reason: problem + " (dump file " + file + ")", Schema: d.name, Name: match})
			continue
		}
		tf.Database, tf.Table = d.name, table
		count[dumpKey{d.name, table}]++
		keep = append(keep, kept{tf: tf, name: name, file: file})
	}
	out := make([]TableFiles, 0, len(keep))
	for _, k := range keep {
		if count[dumpKey{k.tf.Database, k.tf.Table}] > 1 {
			// Two files claim one real name: neither is known to be it.
			left = append(left, LeftOutTable{Table: k.name,
				Reason: "two dump files hold a table of this name, so which one is the table is not known (dump file " + k.file + ")",
				Schema: k.tf.Database, Name: k.tf.Table})
			continue
		}
		out = append(out, k.tf)
	}
	for _, l := range left {
		slog.Warn("snapshot: a table is left out because its real name cannot be read back or stored", "table", l.Table, "reason", l.Reason)
	}

	for i, v := range views {
		if d := realDB(v.Database); d.err == nil {
			views[i].Database = d.name
		}
		if name := meta[dumpKey{v.Database, v.Name}]; name != "" {
			views[i].Name = name
		} else if !generatedName.MatchString(v.Name) {
			// A name mydumper kept is the real one.
		} else if name, err := createdViewName(v.File); err == nil {
			views[i].Name = name
		}
	}
	return out, views, left
}

// createdViewName reads the view's name from the CREATE ... VIEW statement
// of a mydumper <db>.<view>-schema-view.sql file.
func createdViewName(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		up := strings.ToUpper(line)
		if !strings.HasPrefix(up, "CREATE ") {
			continue
		}
		// CREATE [ALGORITHM=...] [DEFINER=...] [SQL SECURITY ...] VIEW `name`:
		// the first " VIEW " outside a quoted definer.
		i := viewKeyword(line)
		if i < 0 {
			return "", fmt.Errorf("%s: its first CREATE is not a CREATE VIEW", filepath.Base(path))
		}
		name, _, ok := readIdent(skipSpaceAndComments(line[i+len(" VIEW "):]))
		if !ok {
			return "", fmt.Errorf("%s: cannot read the view's name", filepath.Base(path))
		}
		return name, nil
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%s holds no CREATE VIEW", filepath.Base(path))
}

// viewKeyword finds " VIEW " (any case) outside backticks and quotes.
func viewKeyword(line string) int {
	var q byte
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case q != 0:
			if c == q {
				q = 0
			}
		case c == '`' || c == '\'' || c == '"':
			q = c
		case c == ' ' && i+6 <= len(line) && strings.EqualFold(line[i:i+6], " VIEW "):
			return i
		}
	}
	return -1
}
