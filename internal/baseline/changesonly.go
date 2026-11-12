package baseline

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// A full-table reconstruct can write a table from its recorded changes alone
// (`bintrail reconstruct --allow-changes-only`): only the rows those changes
// touched, every other row missing. Its files look like any table's. Converted
// into a snapshot, the table would be trusted as whole by everything that
// reads snapshots, with nothing left to say it is not. So the run that writes
// such a table lists it in the dump folder, and Run refuses to convert a
// table that is listed.

// ChangesOnlyMarkerFile lists, in a mydumper-format folder written by
// reconstruct, the tables written from recorded changes alone: one JSON
// object per line, {"schema":"..","table":".."}.
const ChangesOnlyMarkerFile = "_CHANGES_ONLY"

// ChangesOnlySchemaPrefix is how the schema file of such a table opens. It is
// the second check, for a folder whose marker file was lost on the way (a
// copy of the *.sql files alone).
const ChangesOnlySchemaPrefix = "-- bintrail: CHANGES ONLY."

// ErrChangesOnlyTable marks a conversion refused because the dump holds a
// table written from recorded changes alone.
var ErrChangesOnlyTable = errors.New("the dump holds a table written from recorded changes alone")

// ErrUnfinishedDump marks a conversion refused because the run that wrote
// the dump folder did not finish it.
var ErrUnfinishedDump = errors.New("the dump was not finished")

// changesOnlyMu serializes appends: tables of one run are written
// concurrently.
var changesOnlyMu sync.Mutex

type changesOnlyEntry struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// AppendChangesOnlyTable lists schema.table in dir's marker file, creating
// it. The caller writes the table's files only after this returned nil.
func AppendChangesOnlyTable(dir, schema, table string) error {
	line, err := json.Marshal(changesOnlyEntry{Schema: schema, Table: table})
	if err != nil {
		return fmt.Errorf("list %s.%s in %s: %w", schema, table, ChangesOnlyMarkerFile, err)
	}
	changesOnlyMu.Lock()
	defer changesOnlyMu.Unlock()
	path := filepath.Join(dir, ChangesOnlyMarkerFile)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("list %s.%s in %s: %w", schema, table, path, err)
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		f.Close()
		return fmt.Errorf("list %s.%s in %s: %w", schema, table, path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("list %s.%s in %s: %w", schema, table, path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("list %s.%s in %s: %w", schema, table, path, err)
	}
	return nil
}

// RemoveChangesOnlyMarker removes dir's marker file, left by an earlier run
// into the same folder. A folder without one is not an error.
func RemoveChangesOnlyMarker(dir string) error {
	path := filepath.Join(dir, ChangesOnlyMarkerFile)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("could not remove %s left by an earlier run: %w", path, err)
	}
	return nil
}

// ReadChangesOnlyTables returns the tables dir's marker file lists, as
// "schema.table" keys. No file is no table. A file that cannot be read, or a
// line in it that cannot, is an error: the caller must not convert a folder
// whose list it could not read.
func ReadChangesOnlyTables(dir string) (map[string]bool, error) {
	path := filepath.Join(dir, ChangesOnlyMarkerFile)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer f.Close()
	listed := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e changesOnlyEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil || e.Schema == "" || e.Table == "" {
			return nil, fmt.Errorf("%s line %d does not name a table", path, n)
		}
		listed[e.Schema+"."+e.Table] = true
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return listed, nil
}

// schemaFileSaysChangesOnly reports whether a table's schema file opens with
// ChangesOnlySchemaPrefix. A file that cannot be read says nothing here: the
// conversion reads it next and fails on it by itself.
func schemaFileSaysChangesOnly(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	head := make([]byte, len(ChangesOnlySchemaPrefix))
	n, _ := f.Read(head)
	return string(head[:n]) == ChangesOnlySchemaPrefix
}

// refuseUnconvertibleDump is Run's check of the dump folder before anything
// is converted. tables are the ones this run would convert (after --tables).
//
// A folder marked _INCOMPLETE was left by a reconstruct that failed, was
// cancelled or was killed: tables may be missing or cut short, and a snapshot
// made from it would be marked complete. mydumper never writes that marker,
// so a dump of its own is not affected.
//
// A table written from recorded changes alone is refused by name. The others
// are not converted either: a snapshot is published whole or not at all, and
// naming the others with --tables is how to convert them.
func refuseUnconvertibleDump(inputDir string, tables []TableFiles) error {
	if _, err := os.Stat(filepath.Join(inputDir, IncompleteMarker)); err == nil {
		return fmt.Errorf("%w: %s is marked %s, so the run that wrote it failed or was stopped and its tables may be missing or cut short. "+
			"Nothing was converted. Run that reconstruct again to the end, into an empty folder, and convert that",
			ErrUnfinishedDump, inputDir, IncompleteMarker)
	}
	listed, err := ReadChangesOnlyTables(inputDir)
	if err != nil {
		return fmt.Errorf("%w: %s lists the tables of this folder that were written from recorded changes alone, and it could not be read (%v). "+
			"Nothing was converted, because any table here may be one of them",
			ErrChangesOnlyTable, ChangesOnlyMarkerFile, err)
	}
	var refused []string
	for _, t := range tables {
		name := t.Database + "." + t.Table
		if listed[name] || schemaFileSaysChangesOnly(t.SchemaFile) {
			refused = append(refused, name)
		}
	}
	if len(refused) == 0 {
		return nil
	}
	sort.Strings(refused)
	return fmt.Errorf("%w: %s in %s. `bintrail reconstruct --allow-changes-only` wrote %s with only the rows the recorded changes touched, "+
		"so a snapshot made from %s would be missing every other row, and nothing reading that snapshot could tell. "+
		"Nothing was converted. Take a snapshot of %s from the source instead (`bintrail dump`, then `bintrail baseline`); "+
		"to convert the other tables of this folder, name them with --tables",
		ErrChangesOnlyTable, strings.Join(refused, ", "), inputDir,
		itOrThem(len(refused)), itOrThem(len(refused)), itOrThem(len(refused)))
}

func itOrThem(n int) string {
	if n == 1 {
		return "it"
	}
	return "them"
}
