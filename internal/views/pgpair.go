package views

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// pgPairStateBody is the body of a PostgreSQL table's state view when its
// file has a table delta of PostgreSQL's own beside it (#2757): the file's
// rows as the pair leaves them (pgdelta.StateSQL), and the table's tail over
// that when it has one. ok is false for every other table, whose body
// writeStateBody goes on to write as before.
//
// A snapshot update can publish a table that changed as its previous file,
// linked, and one pair of files beside it holding every change since the
// file was written. The file alone is then the table as it was at an
// earlier update, and a view that read it alone would answer with that.
//
// It answers only for a view pinned to one snapshot whose file is named by
// an absolute local path, which is what every statement on the copy reads
// through (the SQL page, the ports, the AI connection): only there can the
// folder be looked at when the view is written. A view that follows later
// snapshots, or names its files in S3 or relative to a folder it will be
// moved with, is written as before.
//
// A table whose definition is known is a MySQL one and is never looked at:
// no file of its folder is asked for. For the others, two names are asked
// for beside the file, and the folder is listed only when one is there.
//
// A pair that is not whole (one file of the two, a gap in the sequence, a
// folder that cannot be listed) gives a view that refuses when it is read:
// the table cannot be said, and the file alone would say it wrong.
func pgPairStateBody(in Input, t BaselineTable) (body string, ok bool) {
	if t.SchemaKnown || t.Delta || in.Follow.follows() ||
		strings.Contains(t.Path, "://") || !filepath.IsAbs(t.Path) || !strings.HasSuffix(t.Path, ".parquet") {
		return "", false
	}
	dead, rows, err := pgdelta.Paths(t.Path, 0)
	if err != nil {
		return "", false
	}
	if pgPairFileMissing(dead) && pgPairFileMissing(rows) {
		return "", false
	}
	dir := filepath.Dir(t.Path)
	chain, err := pgPairChain(dir, strings.TrimSuffix(filepath.Base(t.Path), ".parquet"))
	if err == nil && len(chain.Pairs) == 0 {
		err = errors.New("a file of its table delta is there and could not be read")
	}
	if err != nil {
		return pgPairRefusal(t, err), true
	}
	// Through fileGlob like every file a view names (#2235): read_parquet
	// takes a path as a glob.
	deadFiles := make([]string, len(chain.Pairs))
	newFiles := make([]string, len(chain.Pairs))
	for i, p := range chain.Pairs {
		deadFiles[i] = fileGlob(filepath.Join(dir, p.Dead))
		newFiles[i] = fileGlob(filepath.Join(dir, p.New))
	}
	state := pgdelta.StateSQL(fileGlob(t.Path), deadFiles, newFiles)
	return ordered(t, func(replace string) string {
		if t.Tail != nil {
			segs := make([]string, len(t.Tail.Paths))
			for i, p := range t.Tail.Paths {
				segs[i] = sqlString(fileGlob(p))
			}
			return baseline.TableTailChainStateSQL(state, segs, t.Tail.PK, replace)
		}
		if replace == "" {
			return state
		}
		return "SELECT * REPLACE (" + replace + ") FROM (" + state + ")"
	}), true
}

// pgPairFileMissing reports that path is known not to be there. A file that
// cannot be asked about is not known to be missing.
func pgPairFileMissing(path string) bool {
	_, err := os.Lstat(path)
	return errors.Is(err, os.ErrNotExist)
}

// pgPairChain is the chain of table among the files of dir.
func pgPairChain(dir, table string) (pgdelta.Chain, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return pgdelta.Chain{}, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		// Another table's file, whose name is its table's: not this chain's,
		// and not a name ChainOf takes.
		if strings.ContainsAny(e.Name(), `/\`) {
			continue
		}
		names = append(names, e.Name())
	}
	return pgdelta.ChainOf(table, names)
}

// pgPairRefusal is a body that refuses when the view is read, and not when
// it is created: one table's broken delta must not keep the other tables'
// views from being made. The count of a glob is not known before the read,
// so the error is not raised while the view is bound.
func pgPairRefusal(t BaselineTable, cause error) string {
	msg := fmt.Sprintf("bintrail views: %s.%s has a table delta beside its file that cannot be read whole (%v), "+
		"and its file alone would show the table as it was when it was last written in full. "+
		"A new full snapshot of this server replaces it", t.Schema, t.Table, cause)
	file := sqlString(fileGlob(t.Path))
	return fmt.Sprintf("SELECT * FROM read_parquet(%s) WHERE CASE WHEN (SELECT count(*) FROM glob(%s)) >= 0 THEN error(%s) ELSE true END",
		file, file, sqlString(msg))
}
