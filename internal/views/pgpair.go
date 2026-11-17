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
// It answers only for a view pinned to one snapshot whose file is on this
// machine, which is what every statement on the copy reads through (the SQL
// page, the ports, the AI connection): only there can the folder be looked
// at when the view is written. A path that is not absolute is looked for
// from where this process runs, as the engine will read it (a snapshot
// folder given as a relative path). A view that follows later snapshots, or
// names its files in S3, is written as before; so is one whose relative
// paths are for a folder the file will be moved with, where nothing is found
// from here.
//
// A table whose definition is known is a MySQL one and is never looked at:
// no file of its folder is asked for. For the others, two names are asked
// for beside the file, and the folder is listed only when one is there.
//
// A pair that is not whole (one file of the two, a gap in the sequence, a
// folder that cannot be listed) gives a view that refuses when it is read:
// the table cannot be said, and the file alone would say it wrong.
//
// Where the folder cannot be looked at from here (a view that follows later
// snapshots, a file in S3, a relative path for a folder that is not this
// one), a table known to be a PostgreSQL one reads its file alone behind a
// guard: when the view is read, the engine is asked whether a delta sits
// beside the file, and the view refuses if one does (pgPairGuardedBody).
func pgPairStateBody(in Input, t BaselineTable) (body string, ok bool) {
	if t.SchemaKnown || t.Delta || !strings.HasSuffix(t.Path, ".parquet") {
		return "", false
	}
	knownPG := len(t.PGCasts) > 0 || t.PGRelation != nil
	if in.Follow.follows() || strings.Contains(t.Path, "://") {
		if knownPG {
			return pgPairGuardedBody(in, t), true
		}
		return "", false
	}
	// Where the file is, for looking beside it. The view names it as t.Path
	// spells it, like every other body.
	at, err := filepath.Abs(t.Path)
	if err != nil {
		return "", false
	}
	dead, rows, err := pgdelta.Paths(at, 0)
	if err != nil {
		return "", false
	}
	if pgPairFileMissing(dead) && pgPairFileMissing(rows) {
		// Looked, and none is there. Unless the file itself is not here:
		// then nothing was looked at.
		if knownPG && pgPairFileMissing(at) {
			return pgPairGuardedBody(in, t), true
		}
		return "", false
	}
	dir := filepath.Dir(t.Path)
	chain, err := pgPairChain(filepath.Dir(at), strings.TrimSuffix(filepath.Base(t.Path), ".parquet"))
	if err == nil && len(chain.Pairs) == 0 {
		err = errors.New("a file of its table delta is there and could not be read")
	}
	if err != nil {
		return pgPairRefusal(t, err), true
	}
	// The paths as they are: StateSQL names each file so that it matches
	// itself alone, as fileGlob does for every other file a view names
	// (#2235: read_parquet takes a path as a glob).
	deadFiles := make([]string, len(chain.Pairs))
	newFiles := make([]string, len(chain.Pairs))
	for i, p := range chain.Pairs {
		deadFiles[i] = filepath.Join(dir, p.Dead)
		newFiles[i] = filepath.Join(dir, p.New)
	}
	state := pgdelta.StateSQL(t.Path, deadFiles, newFiles)
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
// views from being made.
//
// The refusal does not hang on the file's rows: a filter over a file that
// holds none is never run, and the view would answer with an empty table. A
// count with no GROUP BY is always one row, so the error is raised by any
// read of the view; the count is of a glob, which is not known before the
// read, so it is not raised while the view is bound. The file is joined to
// it only for the view to have the table's columns.
func pgPairRefusal(t BaselineTable, cause error) string {
	msg := fmt.Sprintf("bintrail views: %s.%s has a table delta beside its file that cannot be read whole (%v), "+
		"and its file alone would show the table as it was when it was last written in full. "+
		"A new full snapshot of this server replaces it", t.Schema, t.Table, cause)
	file := sqlString(fileGlob(t.Path))
	return fmt.Sprintf("SELECT bintrail_t.* FROM (SELECT CASE WHEN count(*) >= 0 THEN error(%s) ELSE true END AS bintrail_refused FROM glob(%s)) AS bintrail_r "+
		"LEFT JOIN read_parquet(%s) AS bintrail_t ON bintrail_r.bintrail_refused", sqlString(msg), file, file)
}

// pgPairGuardedBody is a PostgreSQL table's file read alone, as
// writeStateBody would write it for the view's mode, behind a guard that
// refuses when the engine finds a table delta beside the file: the body for
// a view whose folder could not be looked at when it was written.
//
// The guard is a filter over the file's rows, as the one a following view of
// a MySQL table carries (deltaAppearedGuard): over a file that holds no row
// it is never run.
func pgPairGuardedBody(in Input, t BaselineTable) string {
	msg := fmt.Sprintf("bintrail views: %s.%s has a PostgreSQL table delta beside its file, and this view reads the file alone "+
		"(it follows later snapshots, or names its files in S3 or by a relative path), so it would show the table as it was "+
		"when it was last written in full. Read the table through DBTrail (the SQL page, the PostgreSQL port), "+
		"or generate the views pinned to a snapshot in a local folder", t.Schema, t.Table)
	guard := func(pattern string) string {
		return fmt.Sprintf("\n  WHERE CASE WHEN (SELECT count(*) FROM glob(%s)) > 0 THEN error(%s) ELSE true END", pattern, sqlString(msg))
	}
	var read string
	if in.Follow == FollowNewest {
		// The file is named beside the session's variable, as
		// writeNewestStateBody names it.
		newest := in.sessionName(newestVar)
		file := fmt.Sprintf("CASE WHEN getvariable('%s') IS NULL\n    THEN error(%s)\n    ELSE getvariable('%s') || %s END",
			newest, sqlString(newestVarUnsetMsg), newest, sqlString(fileGlob(t.Rel)))
		read = "read_parquet(" + file + ")" + guard(fmt.Sprintf("getvariable('%s') || %s", newest, sqlString(pgdelta.AnyNewGlob(t.Rel))))
	} else {
		read = "read_parquet(" + sqlString(fileGlob(t.Path)) + ")" + guard(sqlString(pgdelta.AnyNewGlob(t.Path)))
	}
	if list := selectList(t); list != "" {
		return "SELECT " + list + "\n  FROM " + read
	}
	if replace := replaceClause(t); replace != "" {
		return "SELECT * REPLACE (" + replace + ")\n  FROM " + read
	}
	return "SELECT * FROM " + read
}
