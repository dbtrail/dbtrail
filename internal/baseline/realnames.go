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
		return fmt.Errorf(`the name holds a "\", which a snapshot cannot store as a file name`)
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

// dumpRealNames reads metadata's real_table_name per file key, keyed by
// "<file db>.<file table>". A dump without a metadata file, or a mydumper that
// does not write the key, gives an empty map: the CREATE TABLE is then the
// only source, and it is always there.
func dumpRealNames(inputDir string) map[string]string {
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
	out := make(map[string]string)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	section := ""
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = ""
			db, rest, ok := readIdent(line[1 : len(line)-1])
			if ok && strings.HasPrefix(rest, ".") {
				if table, tail, ok := readIdent(rest[1:]); ok && tail == "" {
					section = db + "." + table
				}
			}
			continue
		}
		if v, ok := strings.CutPrefix(line, "real_table_name="); ok && section != "" {
			// The value doubles a backtick, as a quoted name does.
			out[section] = strings.ReplaceAll(v, "``", "`")
		}
	}
	if err := sc.Err(); err != nil {
		slog.Warn("could not read the whole metadata file; table names it did not give are read from each CREATE TABLE", "dir", inputDir, "error", err)
	}
	return out
}

// quoteName writes a name the way the messages below show it.
func quoteName(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// realDumpNames replaces the file-derived names DiscoverDump found with the
// real ones the dump records, and refuses (naming every table it cannot
// store) rather than publish a made-up or wrong name. Views get their real
// name when metadata has it; they are only reported, never stored.
func realDumpNames(inputDir string, tables []TableFiles, views []SkippedView) ([]TableFiles, []SkippedView, error) {
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
			d.err = fmt.Errorf("the dump calls its schema %s, a name mydumper makes up for a schema whose name it will not put in a file name, "+
				"and holds no CREATE DATABASE to read the real name from", quoteName(fileDB))
		} else {
			d.err = fmt.Errorf("the dump calls its schema %s, a name mydumper makes up, and its real name cannot be read from %s: %w",
				quoteName(fileDB), filepath.Base(createFile), err)
		}
		if d.err == nil {
			switch {
			case strings.Contains(d.name, "."):
				d.err = fmt.Errorf("the schema name %s holds a dot, which a snapshot cannot store yet", quoteName(d.name))
			default:
				if err := storableName(d.name); err != nil {
					d.err = fmt.Errorf("schema %s: %w", quoteName(d.name), err)
				}
			}
		}
		dbs[fileDB] = d
		return d
	}

	var problems []string
	seen := make(map[string]string, len(tables))
	out := make([]TableFiles, 0, len(tables))
	for _, tf := range tables {
		file := filepath.Base(tf.SchemaFile)
		d := realDB(tf.Database)
		// Which name is the table's: metadata's real_table_name when the
		// dump has one (checked against the CREATE TABLE); else the CREATE
		// TABLE when the file name is one mydumper made up; else the file
		// name, which is what mydumper writes whenever it can, and what
		// every earlier release used.
		created, cerr := createdName(tf.SchemaFile, "TABLE")
		recorded := meta[tf.Database+"."+tf.Table]
		var table string
		switch {
		case generatedName.MatchString(tf.Table) && cerr != nil:
			// A made-up file name needs the CREATE TABLE to confirm any
			// name: a metadata value alone can be cut short (a name with
			// a line break splits the metadata line).
			problems = append(problems, fmt.Sprintf("the table in %s: mydumper wrote it under a made-up name and its real name cannot be read (%v)", file, cerr))
			continue
		case recorded != "" && (cerr != nil || created != recorded):
			problems = append(problems, fmt.Sprintf("the table in %s: its CREATE TABLE names it %s but the dump's metadata names it %s",
				file, quoteName(created), quoteName(recorded)))
			continue
		case recorded != "":
			table = recorded
		case generatedName.MatchString(tf.Table):
			table = created
		case strings.Contains(tf.Table, ".") && cerr == nil && created != tf.Table:
			// A mydumper that writes a dotted schema name into the file
			// name: "my.db.t" splits as schema "my", table "db.t". The
			// CREATE TABLE says "t", so the split is wrong.
			problems = append(problems, fmt.Sprintf("the table in %s: the file name reads as %s.%s but its CREATE TABLE names it %s; a schema name with a dot cannot be stored in a snapshot yet",
				file, quoteName(tf.Database), quoteName(tf.Table), quoteName(created)))
			continue
		default:
			table = tf.Table
		}
		// The schema's real name when it was read, even if it is refused.
		label := quoteName(d.name) + "." + quoteName(table)
		if d.name == "" {
			label = quoteName(tf.Database) + "." + quoteName(table)
		}
		if d.err != nil {
			problems = append(problems, fmt.Sprintf("%s (dump file %s): %v", label, file, d.err))
			continue
		}
		if err := storableName(table); err != nil {
			problems = append(problems, fmt.Sprintf("%s (dump file %s): %v", label, file, err))
			continue
		}
		key := d.name + "." + table
		if other, dup := seen[key]; dup {
			problems = append(problems, fmt.Sprintf("%s: both %s and %s hold it", label, other, file))
			continue
		}
		seen[key] = file
		tf.Database, tf.Table = d.name, table
		out = append(out, tf)
	}
	if len(problems) > 0 {
		return nil, nil, fmt.Errorf("%d table(s) in the dump cannot be stored under their real names, so none was converted: %s",
			len(problems), strings.Join(problems, "; "))
	}

	for i, v := range views {
		if d := realDB(v.Database); d.err == nil {
			views[i].Database = d.name
		}
		if name := meta[v.Database+"."+v.Name]; name != "" {
			views[i].Name = name
		}
	}
	return out, views, nil
}
