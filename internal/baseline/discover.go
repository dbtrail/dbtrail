package baseline

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// TableFiles groups all mydumper files for a single table.
type TableFiles struct {
	Database   string
	Table      string
	SchemaFile string   // <db>.<table>-schema.sql
	DataFiles  []string // absolute paths, sorted
	Format     string   // "sql" or "tab"
}

// SkippedView is a view found in the dump. A view holds no rows of its own, so
// there is nothing to convert: it is left out of the baseline and reported.
type SkippedView struct {
	Database string
	Name     string
	// File is what identified it: mydumper's <db>.<view>-schema-view.sql, or
	// the <db>.<view>-schema.sql of a layout that writes CREATE VIEW there.
	File string
}

// viewSchemaSuffix ends the file mydumper writes the real CREATE VIEW into.
// The view's <db>.<view>-schema.sql beside it is a placeholder CREATE TABLE
// (ENGINE=MEMORY, every column int) that myloader creates first so that views
// depending on each other can be loaded in any order.
const viewSchemaSuffix = "-schema-view.sql"

// DiscoverTables scans the mydumper output directory and groups files by table.
// It returns one TableFiles entry per table that has a schema file. Tables with
// no data files (empty at dump time) are included with an empty DataFiles slice
// so that downstream consumers can produce 0-row baselines. Views are skipped;
// DiscoverDump also says which ones.
func DiscoverTables(inputDir string) ([]TableFiles, error) {
	tables, _, err := DiscoverDump(inputDir)
	return tables, err
}

// DiscoverDump is DiscoverTables plus the views it left out, sorted by
// database and name.
//
// A view is recognized by its <db>.<view>-schema-view.sql file (#1687), and
// only when all of this holds:
//
//   - the file's first CREATE statement is a CREATE VIEW. The name alone is
//     not enough: mydumper 0.10 writes the rows of a table called
//     `x-schema-view` to a file with that exact name.
//   - no data file exists for the object. A view has none, so an object with
//     rows is converted whatever else sits beside it.
//   - its <db>.<view>-schema.sql, when there is one, is mydumper's
//     placeholder (isViewPlaceholder). A real table that is empty has no data
//     file either, and a view file can outlive the dump that wrote it.
//
// Anything else stays a table. Skipping a real table loses it without a word,
// while converting a view fails the run where someone can see it, so every
// doubt resolves to "table". The one case with no safe answer, a
// -schema-view.sql that is empty or cannot be read, is an error that names
// the file.
func DiscoverDump(inputDir string) ([]TableFiles, []SkippedView, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, nil, fmt.Errorf("read input directory: %w", err)
	}

	type tableKey struct{ db, table string }
	schemas := make(map[tableKey]string) // key → schema file path
	data := make(map[tableKey][]string)  // key → data file paths
	formats := make(map[tableKey]string) // key → "sql" or "tab"
	// View files by the object they would describe. The same path is also
	// filed under data below (as the mydumper 0.10 data file of a table named
	// `<view>-schema-view`); its content picks one of the two after the scan.
	viewFiles := make(map[tableKey]string)

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		path := filepath.Join(inputDir, name)

		// Compressed mydumper output (--compress GZIP/ZSTD) writes
		// <db>.<table>.<chunk>.sql.gz / .sql.zst (and matching -schema.sql.gz).
		// The baseline readers parse plain SQL/TSV only, so such a file would
		// fall through the classifier below and be silently skipped — the whole
		// dump then surfaces as an unhelpful "no tables found". Fail loud with
		// actionable guidance instead.
		if isCompressedDump(name) {
			return nil, nil, fmt.Errorf("compressed mydumper dump detected (%s): compressed dumps are not supported — "+
				"re-run mydumper without --compress, or decompress the dump first (e.g. gunzip *.gz / unzstd *.zst)", name)
		}

		// Schema file: <db>.<table>-schema.sql
		if strings.HasSuffix(name, "-schema.sql") {
			base := strings.TrimSuffix(name, "-schema.sql")
			db, table, ok := splitDBTable(base)
			if !ok {
				continue
			}
			k := tableKey{db, table}
			schemas[k] = path
			continue
		}

		if strings.HasSuffix(name, viewSchemaSuffix) {
			if db, view, ok := splitDBTable(strings.TrimSuffix(name, viewSchemaSuffix)); ok {
				viewFiles[tableKey{db, view}] = path
			}
			// No continue: see viewFiles.
		}

		// Data file: <db>.<table>.<chunk>.sql, <db>.<table>.<chunk>.dat,
		// or <db>.<table>.sql / <db>.<table>.dat (mydumper 0.10.0 — no
		// chunk number; the table has a single data file). Both shapes
		// must be recognized so bintrail baseline works with the
		// apt-installed mydumper on Ubuntu 24.04 (#221).
		var ext string
		switch {
		case strings.HasSuffix(name, ".sql"):
			ext = "sql"
			name = strings.TrimSuffix(name, ".sql")
		case strings.HasSuffix(name, ".dat"):
			ext = "tab"
			name = strings.TrimSuffix(name, ".dat")
		default:
			continue
		}

		// Try the chunked format first: <db>.<table>.<chunk>
		// If the last dot-separated segment is numeric, split it off.
		// Otherwise fall through to the unchunked format: <db>.<table>.
		var db, table string
		var ok bool
		lastDot := strings.LastIndex(name, ".")
		if lastDot >= 0 {
			chunk := name[lastDot+1:]
			if isNumericChunk(chunk) {
				db, table, ok = splitDBTable(name[:lastDot])
			}
		}
		if !ok {
			// Unchunked format (mydumper 0.10.0): name is just <db>.<table>.
			db, table, ok = splitDBTable(name)
			if !ok {
				continue
			}
		}

		k := tableKey{db, table}
		data[k] = append(data[k], path)
		if formats[k] == "" {
			formats[k] = ext
		}
	}

	var views []SkippedView
	for k, viewPath := range viewFiles {
		holdsView, err := holdsCreateView(viewPath)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot tell whether %s.%s is a view: %w", k.db, k.table, err)
		}
		if !holdsView {
			continue // table data under a name that looks like a view file
		}
		// It is a view definition, so it is not rows of `<view>-schema-view`.
		asData := tableKey{k.db, k.table + strings.TrimSuffix(viewSchemaSuffix, ".sql")}
		if rest := slices.DeleteFunc(data[asData], func(p string) bool { return p == viewPath }); len(rest) > 0 {
			data[asData] = rest
		} else {
			delete(data, asData)
			delete(formats, asData)
		}
		if len(data[k]) > 0 {
			slog.Warn("the dump holds a view definition and rows under the same name; converting the rows as a table",
				"db", k.db, "table", k.table, "view_file", viewPath, "data_files", len(data[k]))
			continue
		}
		if schemaPath, ok := schemas[k]; ok && !isViewPlaceholder(schemaPath) {
			slog.Warn("the dump holds a view definition beside a table definition of the same name; converting the table",
				"db", k.db, "table", k.table, "view_file", viewPath, "schema_file", schemaPath)
			continue
		}
		views = append(views, SkippedView{Database: k.db, Name: k.table, File: viewPath})
		delete(schemas, k) // the placeholder, when the dump has one
	}

	var result []TableFiles
	for k, schemaPath := range schemas {
		files, ok := data[k]
		if !ok {
			if isView(schemaPath) {
				// genuine view — no data to convert
				views = append(views, SkippedView{Database: k.db, Name: k.table, File: schemaPath})
				continue
			}
			// Empty table: schema exists but mydumper produced no data file
			// because the table had zero rows at dump time. Emit a 0-row
			// Parquet so that reconstruct can find a baseline for every table.
			result = append(result, TableFiles{
				Database:   k.db,
				Table:      k.table,
				SchemaFile: schemaPath,
				Format:     "sql",
			})
			continue
		}
		sort.Strings(files)
		result = append(result, TableFiles{
			Database:   k.db,
			Table:      k.table,
			SchemaFile: schemaPath,
			DataFiles:  files,
			Format:     formats[k],
		})
	}
	// The names so far are the file names; the snapshot takes the real
	// ones (#2006).
	result, views, err = realDumpNames(inputDir, result, views)
	if err != nil {
		return nil, nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Database != result[j].Database {
			return result[i].Database < result[j].Database
		}
		return result[i].Table < result[j].Table
	})
	sort.Slice(views, func(i, j int) bool {
		if views[i].Database != views[j].Database {
			return views[i].Database < views[j].Database
		}
		return views[i].Name < views[j].Name
	})
	return result, views, nil
}

// holdsCreateView reports whether the first CREATE statement of a
// <db>.<view>-schema-view.sql candidate is a CREATE VIEW. A file that reaches
// an INSERT or a CREATE TABLE first is table data or a table definition, and
// the answer is false. An empty or unreadable file is an error: its name says
// view, nothing in it confirms that, and guessing either way is wrong.
//
// Only the start of each line is looked at. A data file's INSERT line can be
// megabytes long, far past what a bufio.Scanner accepts.
func holdsCreateView(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64<<10)
	sawContent := false
	for {
		head, more, err := r.ReadLine()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false, fmt.Errorf("read %s: %w", path, err)
		}
		// Copied before reading on: head points into the reader's buffer.
		line := strings.ToUpper(strings.TrimSpace(string(head)))
		for more { // drop the rest of a line longer than the buffer
			_, more, err = r.ReadLine()
			if errors.Is(err, io.EOF) {
				break // the line filled the buffer and ended the file
			}
			if err != nil {
				return false, fmt.Errorf("read %s: %w", path, err)
			}
		}
		if line == "" {
			continue
		}
		sawContent = true
		fields := strings.Fields(line)
		// A statement inside a version comment: /*!50001 CREATE ... VIEW ... */
		if strings.HasPrefix(fields[0], "/*!") && len(fields) > 1 {
			fields = fields[1:]
		}
		switch fields[0] {
		case "INSERT", "REPLACE", "LOAD":
			return false, nil
		case "CREATE":
			// "CREATE [OR REPLACE] [ALGORITHM=...] [DEFINER=...]
			// [SQL SECURITY ...] VIEW `name`": VIEW is a word of its own
			// before the name. CREATE TABLE `my view` has TABLE there.
			return len(fields) > 1 && fields[1] != "TABLE" && strings.Contains(line, " VIEW "), nil
		}
	}
	if !sawContent {
		return false, fmt.Errorf("%s is empty", path)
	}
	return false, nil
}

// isViewPlaceholder reports whether a <db>.<name>-schema.sql is the table
// mydumper writes in place of a view, and not the definition of a real table.
//
// The two differ in how they are produced. A real table's file is the
// server's SHOW CREATE TABLE output: "CREATE TABLE `t` (", every line of the
// body indented. The placeholder is written by mydumper itself:
// "CREATE TABLE IF NOT EXISTS `v`(", then one column per line starting at
// column 0 and nothing else. The engine does not tell them apart, since an
// empty MEMORY table is a real table.
//
// All of it has to match. Anything else, a file that cannot be read included,
// is answered "not a placeholder", which keeps the object a table.
func isViewPlaceholder(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	inBody, columns := false, 0
	for scanner.Scan() {
		line := scanner.Text()
		if !inBody {
			if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "CREATE") {
				continue
			}
			if !strings.HasPrefix(strings.ToUpper(line), "CREATE TABLE IF NOT EXISTS ") {
				return false
			}
			inBody = true
			continue
		}
		if strings.HasPrefix(line, ")") {
			return columns > 0
		}
		if !strings.HasPrefix(line, "`") {
			return false // indented, or a key or constraint: a real table
		}
		columns++
	}
	return false
}

// splitDBTable splits a "<db>.<table>" string. Returns false if it doesn't
// contain exactly one dot separator.
func splitDBTable(s string) (db, table string, ok bool) {
	dot := strings.Index(s, ".")
	if dot < 0 || dot == len(s)-1 {
		return "", "", false
	}
	return s[:dot], s[dot+1:], true
}

// isView reads a mydumper schema SQL file and returns true if it contains a
// CREATE VIEW statement rather than CREATE TABLE. Views have no data to
// convert, while empty tables should still produce a 0-row Parquet file.
// Returns false (assume table) on errors — a harmless 0-row Parquet is
// better than silently dropping a real table.
func isView(schemaPath string) bool {
	f, err := os.Open(schemaPath)
	if err != nil {
		return false // can't read → assume table (safe: 0-row Parquet is harmless)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.ToUpper(strings.TrimSpace(scanner.Text()))
		if strings.HasPrefix(line, "CREATE") {
			// mydumper emits either "CREATE TABLE" or
			// "CREATE [ALGORITHM=...] [DEFINER=...] [SQL SECURITY ...] VIEW".
			return strings.Contains(line, " VIEW ")
		}
	}
	if err := scanner.Err(); err != nil {
		slog.Debug("I/O error reading schema file for view detection; assuming table",
			"path", schemaPath, "error", err)
	}
	// No CREATE statement found or I/O error — assume table rather than
	// silently skipping, which is the exact failure mode of issue #226.
	return false
}

// isCompressedDump reports whether name is a mydumper data or schema file
// written with --compress: GZIP → ".gz", ZSTD → ".zst". Matching the full
// ".sql.gz"/".dat.zst" shape (not a bare ".gz") avoids false-positives on
// unrelated files that happen to sit in the dump directory.
func isCompressedDump(name string) bool {
	lower := strings.ToLower(name)
	for _, suf := range []string{".sql.gz", ".sql.zst", ".dat.gz", ".dat.zst"} {
		if strings.HasSuffix(lower, suf) {
			return true
		}
	}
	return false
}

// isNumericChunk returns true if s consists only of decimal digits (e.g. "00000").
func isNumericChunk(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
