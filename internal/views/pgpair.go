package views

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
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
// It reads the pair only for a view pinned to one snapshot whose file is on
// this machine, which is what every statement on the copy reads through (the
// SQL page, the ports, the AI connection): only there can the folder be
// looked at when the view is written. A path that is not absolute is looked
// for from where this process runs, as the engine will read it (a snapshot
// folder given as a relative path).
//
// A table whose definition is known is a MySQL one and is never looked at:
// no file of its folder is asked for. For the others, two names are asked
// for beside the file, and the folder is listed only when one is there.
//
// A pair that is not whole (one file of the two, a gap in the sequence, a
// folder that cannot be listed) gives a view that refuses when it is read:
// the table cannot be said, and the file alone would say it wrong.
//
// Where the folder cannot be looked at from here, a table known to be a
// PostgreSQL one reads its file alone behind a guard: when the view is read,
// the engine is asked whether a delta sits beside the file, and the view
// refuses if one does (pgPairGuardedBody). That is for a view that follows
// later snapshots, which already asks the store for the files beside the
// table's at every read, and for one with relative paths whose file is not
// here (the views of a folder that is moved).
//
// A view pinned to a snapshot in S3 names its files by their exact keys and
// lists nothing, and a guard would make every read list the store, which a
// reader allowed only to get objects cannot do. So what is beside the file
// has to be known when the view is written: whoever writes it from the
// snapshot's own folder says so first (Input.MarkPGPairs), and the view then
// names the delta's files by their exact keys too. (A delta that is not
// whole is the one case that asks the store for a name: the refusal's own
// glob, of the table file's exact key.) Unmarked, such a view is written as
// before and reads the file alone: the views the console and the CLI write
// for a snapshot in S3, which have no folder to look at.
func pgPairStateBody(in Input, t BaselineTable) (body string, ok bool) {
	if t.SchemaKnown || t.Delta || !strings.HasSuffix(t.Path, ".parquet") {
		return "", false
	}
	knownPG := len(t.PGCasts) > 0 || t.PGRelation != nil
	if in.Follow.follows() {
		if knownPG {
			return pgPairGuardedBody(in, t), true
		}
		return "", false
	}
	mark := t.PGPair
	if mark == nil {
		if strings.Contains(t.Path, "://") {
			return "", false
		}
		if mark = pgPairLook(t.Path); mark == nil {
			// Nothing was looked at. A relative path whose file is not
			// here is for another folder.
			if knownPG && !filepath.IsAbs(t.Path) {
				return pgPairGuardedBody(in, t), true
			}
			return "", false
		}
	}
	if mark.Unasked != nil {
		// Whether it has a delta could not be asked. A table known to be a
		// PostgreSQL one refuses, since its file alone may not be the
		// table; any other is written as before.
		if knownPG {
			return pgPairRefusal(t, fmt.Sprintf("whether %s.%s has a table delta beside its file could not be asked when these views were written (%v), "+
				"and its file alone may show the table as it was when it was last written in full. Generate the views again",
				t.Schema, t.Table, mark.Unasked)), true
		}
		return "", false
	}
	if mark.Err != nil {
		return pgPairRefusal(t, fmt.Sprintf("%s.%s has a table delta beside its file that cannot be read whole (%v), "+
			"and its file alone would show the table as it was when it was last written in full. "+
			"A new full snapshot of this server replaces it", t.Schema, t.Table, mark.Err)), true
	}
	if len(mark.Pairs) == 0 {
		return "", false
	}
	// The delta's files are named beside the table's, as t.Path spells it:
	// StateSQL names each file so that it matches itself alone, as fileGlob
	// does for every other file a view names (#2235: read_parquet takes a
	// path as a glob).
	deadFiles := make([]string, len(mark.Pairs))
	newFiles := make([]string, len(mark.Pairs))
	for i, p := range mark.Pairs {
		deadFiles[i] = pgPairBeside(t.Path, p.Dead)
		newFiles[i] = pgPairBeside(t.Path, p.New)
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

// PGPairMark is what was found beside a PostgreSQL table's file when the
// folder was looked at: the pairs of its table delta, none when it has no
// delta, or why the delta there cannot be read whole.
//
// Unasked is set instead when the folder could not be listed at all: nothing
// is known about any table in it.
type PGPairMark struct {
	Pairs   []pgdelta.Pair
	Err     error
	Unasked error
}

// MarkPGPairs looks beside the file of every table that can have a
// PostgreSQL table delta and records what is there (BaselineTable.PGPair).
// For whoever writes a views file from a snapshot's own folder and then
// names its files somewhere else (RespellBaselines): after that the folder
// cannot be looked at, and before it this is the one moment it can.
//
// A table whose definition is known, or that has a MySQL delta, is a MySQL
// one and is not looked at. Nor is a path that is not a local file, or one
// whose file is not there: those stay unmarked, and are written as before.
func (in *Input) MarkPGPairs() {
	for i := range in.Baselines {
		t := &in.Baselines[i]
		if !pgPairMarkable(*t) || strings.Contains(t.Path, "://") {
			continue
		}
		t.PGPair = pgPairLook(t.Path)
		pgPairWarn(*t)
	}
}

// MarkPGPairsListed is MarkPGPairs for tables whose files may be in S3: a
// local file is looked beside as there, and the files of a snapshot in S3
// are listed once, by one glob over the snapshot. Call it while Path is the
// real path and after MarkTableDeltas, whose mark tells a MySQL table apart.
//
// It returns nothing, on purpose: it is called for every source family, and
// a listing that fails must not fail the views of a MySQL server. What was
// not known is recorded (PGPairMark.Unasked) and refuses only for a table
// that turns out to be a PostgreSQL one.
func MarkPGPairsListed(ctx context.Context, tables []BaselineTable) {
	type listing struct {
		byDir map[string][]string
		err   error
	}
	listed := map[string]*listing{}
	for i := range tables {
		t := &tables[i]
		if !pgPairMarkable(*t) {
			continue
		}
		if !strings.Contains(t.Path, "://") {
			t.PGPair = pgPairLook(t.Path)
			pgPairWarn(*t)
			continue
		}
		if !strings.HasPrefix(t.Path, "s3://") {
			continue
		}
		snap := snapshotDirOf(t.Path)
		l := listed[snap]
		if l == nil {
			l = &listing{}
			if l.byDir, l.err = pgPairListS3(ctx, snap); l.err != nil {
				slog.Warn("views: the PostgreSQL table deltas of a snapshot could not be listed; "+
					"the view of a PostgreSQL table in it is written to refuse",
					"snapshot", snap, "error", l.err)
			}
			listed[snap] = l
		}
		if l.err != nil {
			t.PGPair = &PGPairMark{Unasked: l.err}
			continue
		}
		cut := strings.LastIndexByte(t.Path, '/')
		chain, err := pgdelta.ChainOf(strings.TrimSuffix(t.Path[cut+1:], ".parquet"), l.byDir[t.Path[:cut]])
		if err != nil {
			t.PGPair = &PGPairMark{Err: err}
		} else {
			t.PGPair = &PGPairMark{Pairs: chain.Pairs}
		}
		pgPairWarn(*t)
	}
}

// pgPairMarkable reports whether t can have a PostgreSQL table delta: not a
// table whose definition is known or that has a MySQL delta, which is a
// MySQL one.
func pgPairMarkable(t BaselineTable) bool {
	return !t.SchemaKnown && !t.Delta && strings.HasSuffix(t.Path, ".parquet")
}

// pgPairWarn says, when the views are written, that a table's view will
// refuse: that is read much later and maybe somewhere else.
func pgPairWarn(t BaselineTable) {
	if t.PGPair == nil || t.PGPair.Err == nil {
		return
	}
	slog.Warn("views: a table's delta cannot be read whole, so its view is written to refuse; "+
		"a new full snapshot of this server replaces it",
		"schema", t.Schema, "table", t.Table, "file", t.Path, "error", t.PGPair.Err)
}

// pgPairListS3 lists the delta files of the snapshot at an s3:// URL, by the
// folder they are in: one glob, over every schema's folder. A variable, for
// a test to stand in for the store.
var pgPairListS3 = func(ctx context.Context, snapshotURL string) (map[string][]string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()
	if err := duckdbutil.LoadHTTPFS(ctx, db); err != nil {
		return nil, fmt.Errorf("load httpfs extension: %w", err)
	}
	if err := duckdbutil.EnableS3CredentialChainFor(ctx, db, "", snapshotURL); err != nil {
		return nil, err
	}
	pattern := pgdelta.GlobLiteral(strings.TrimRight(snapshotURL, "/")) + "/*/*.pg*"
	rows, err := db.QueryContext(ctx, "SELECT file FROM glob("+sqlString(pattern)+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		cut := strings.LastIndexByte(f, '/')
		if cut < 0 {
			continue
		}
		// Only a delta's own names: ChainOf takes any ".pgdead"/".pgnew"
		// name for a half of something.
		if _, ok := pgdelta.ParseName(f[cut+1:]); ok {
			out[f[:cut]] = append(out[f[:cut]], f[cut+1:])
		}
	}
	return out, rows.Err()
}

// pgPairLook looks beside the table file at path, a local one. nil when
// nothing could be looked at: the file is not there and no delta is.
func pgPairLook(path string) *PGPairMark {
	at, err := filepath.Abs(path)
	if err != nil {
		return nil
	}
	dead, rows, err := pgdelta.Paths(at, 0)
	if err != nil {
		return nil
	}
	if pgPairFileMissing(dead) && pgPairFileMissing(rows) {
		if pgPairFileMissing(at) {
			return nil
		}
		return &PGPairMark{}
	}
	// A file that cannot be asked about is not known to be missing, and
	// that is the cause to give: the listing below may not show it.
	for _, f := range []string{dead, rows} {
		if _, lerr := os.Lstat(f); lerr != nil && !errors.Is(lerr, os.ErrNotExist) {
			return &PGPairMark{Err: fmt.Errorf("whether it has a table delta cannot be asked: %w", lerr)}
		}
	}
	chain, err := pgPairChain(filepath.Dir(at), strings.TrimSuffix(filepath.Base(at), ".parquet"))
	if err == nil && len(chain.Pairs) == 0 {
		err = errors.New("a file of its table delta is there and could not be read")
	}
	if err != nil {
		return &PGPairMark{Err: err}
	}
	return &PGPairMark{Pairs: chain.Pairs}
}

// pgPairBeside names the file called name beside the table file at path, in
// path's own spelling: a local path, a relative one, or a URL, which
// filepath.Join would not leave as it is.
func pgPairBeside(path, name string) string {
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[:i+1] + name
	}
	return filepath.Join(filepath.Dir(path), name)
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
func pgPairRefusal(t BaselineTable, why string) string {
	msg := "bintrail views: " + why
	file := sqlString(fileGlob(t.Path))
	return fmt.Sprintf("SELECT bintrail_t.* FROM (SELECT CASE WHEN count(*) >= 0 THEN error(%s) ELSE true END AS bintrail_refused FROM glob(%s)) AS bintrail_r "+
		"LEFT JOIN read_parquet(%s) AS bintrail_t ON bintrail_r.bintrail_refused", sqlString(msg), file, file)
}

// pgPairGuardedBody is a PostgreSQL table's file read alone, as
// writeStateBody would write it for the view's mode, behind a guard that
// refuses when the engine finds a table delta beside the file: the body for
// a view whose folder could not be looked at when it was written. A table's
// tail is not laid over it: a tail is given only to a pinned view of a local
// file, which never comes here.
//
// The guard is a filter over the file's rows, as the one a following view of
// a MySQL table carries (deltaAppearedGuard): over a file that holds no row
// it is never run.
func pgPairGuardedBody(in Input, t BaselineTable) string {
	msg := fmt.Sprintf("bintrail views: %s.%s has a PostgreSQL table delta beside its file, and this view reads the file alone "+
		"(it follows later snapshots, or names its files by a relative path), so it would show the table as it was "+
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
