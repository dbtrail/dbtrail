package reconstruct

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// An output folder of a full-table reconstruct is routinely reused (see
// markRunIncomplete), and a table's files keep fixed names from one run to the
// next. A run that writes a table rewrites its schema file and its data files
// from number 00000 up, and nothing else of it:
//
//   - a table the run refuses or fails on keeps the files an earlier run wrote
//     for it, under the refusal that says nothing was written for it;
//   - a table written again with fewer data files than before keeps the
//     earlier run's higher-numbered ones, under _SUCCESS.
//
// A loader reads every file of the folder and looks at no marker, so either
// way it would load rows from another moment as part of the table. So before
// any table is written, the run removes the files of the tables it names, and
// only those: the names this package writes for them (isMydumperTableFile,
// pgFileName), compared exactly, on regular files.

// tableLeftovers is what removeTableLeftovers did and saw in an output folder.
type tableLeftovers struct {
	// Removed are the files of the run's tables that an earlier run left.
	Removed []string
	// Others are the schema files (mydumper output) of tables this run does
	// not name. They are left in place: the run was not asked about those
	// tables, and a folder can hold them on purpose. A loader still reads
	// them with this run's tables, so the run says so.
	Others []string
}

// removeTableLeftovers removes from dir the files that format writes for each
// of tables ("schema.table" entries; one that does not parse is skipped, the
// run refuses it later). It returns what it removed and, for mydumper output,
// the schema files of other tables. A file that cannot be removed is an
// error: the run must not go on with an earlier run's rows in the folder.
func removeTableLeftovers(dir, format string, tables []string) (tableLeftovers, error) {
	var out tableLeftovers
	if format != OutputFormatMydumper && format != OutputFormatPsql {
		return out, nil
	}
	type name struct{ schema, table string }
	var names []name
	for _, entry := range tables {
		if schema, table, ok := splitSchemaTable(entry); ok {
			names = append(names, name{schema, table})
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return out, fmt.Errorf("read output directory %s: %w", dir, err)
	}
	owned := func(file string) bool {
		for _, n := range names {
			if format == OutputFormatPsql {
				if file == pgFileName(n.schema, n.table) {
					return true
				}
			} else if isMydumperTableFile(file, n.schema, n.table) {
				return true
			}
		}
		return false
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		file := e.Name()
		if !owned(file) {
			if format == OutputFormatMydumper && strings.HasSuffix(file, "-schema.sql") {
				out.Others = append(out.Others, file)
			}
			continue
		}
		if err := os.Remove(filepath.Join(dir, file)); err != nil && !os.IsNotExist(err) {
			return out, fmt.Errorf("could not remove %s, which an earlier run left in %s for a table this run writes "+
				"(a loader would read it with this run's files): %w", file, dir, err)
		}
		out.Removed = append(out.Removed, file)
	}
	slices.Sort(out.Removed)
	slices.Sort(out.Others)
	return out, nil
}

// leftoversRemovedMsg and leftoversOtherTablesMsg are the log lines of
// logTableLeftovers.
const (
	leftoversRemovedMsg = "removed the files an earlier run left in the output folder for the tables this run writes; " +
		"a table this run does not finish has no files there"
	leftoversOtherTablesMsg = "the output folder holds schema files of tables this run does not write, left there earlier; " +
		"a loader reads them with this run's tables, from the moment they were written"
)

// logTableLeftovers reports what removeTableLeftovers removed and saw.
func logTableLeftovers(left tableLeftovers) {
	if len(left.Removed) > 0 {
		slog.Info(leftoversRemovedMsg, "files", strings.Join(left.Removed, ","))
	}
	if len(left.Others) > 0 {
		slog.Warn(leftoversOtherTablesMsg, "schema_files", strings.Join(left.Others, ","))
	}
}
