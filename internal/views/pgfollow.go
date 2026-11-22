package views

import (
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/pgdelta"
)

// pgFollowStateBody is the body of a FOLLOWING state view of a table known
// to be a PostgreSQL one: its file and every pair of its table delta beside
// it, read through patterns (pgdelta.FollowStateSQL), so the view answers
// with the table as the snapshot it reads leaves it, whether that snapshot
// holds no pair beside the file, one, or a chain of them.
//
// It is how a following view of a MySQL table reads its chain
// (writeNewestStateBody, deltaStateBody: baseline.TableDeltaFollowStateSQL),
// built the same way for each following mode:
//
//   - Following the `current` pointer, the paths name the pointer and are
//     read as they are (t.Path).
//   - Following the newest snapshot, every path is built against the
//     session's variable from the file's place in the snapshot (t.Rel).
//     When the file lists the snapshot once (listsOnce), the patterns pick
//     their files out of that listing instead of asking the store again; a
//     session with no listing for the snapshot the variable names falls back
//     to the patterns themselves.
//
// The patterns also match the table's own file, which is always there, so no
// read meets a pattern that matches nothing; the name filter drops that
// file's rows from the chain's reads again.
func pgFollowStateBody(in Input, t BaselineTable) string {
	var base, dead, rows, plain string
	if in.Follow == FollowNewest {
		newest := in.sessionName(newestVar)
		path := func(rel string) string {
			return fmt.Sprintf("CASE WHEN getvariable('%s') IS NULL\n    THEN error(%s)\n    ELSE getvariable('%s') || %s END",
				newest, sqlString(newestVarUnsetMsg), newest, sqlString(rel))
		}
		chain := path
		if in.listsOnce() {
			chain = func(glob string) string { return pgFollowListed(in, t, glob) }
		}
		deadGlob, rowsGlob := pgdelta.FollowGlobs(t.Rel)
		base, dead, rows, plain = path(fileGlob(t.Rel)), chain(deadGlob), chain(rowsGlob), t.Rel
	} else {
		deadGlob, rowsGlob := pgdelta.FollowGlobs(t.Path)
		base, dead, rows, plain = sqlString(fileGlob(t.Path)), sqlString(deadGlob), sqlString(rowsGlob), t.Path
	}
	state := pgdelta.FollowStateSQL(base, dead, rows, plain)
	return ordered(t, func(replace string) string {
		if replace == "" {
			return state
		}
		return "SELECT * REPLACE (" + replace + ") FROM (" + state + ")"
	})
}

// pgFollowListed is the pattern glob, relative to the snapshot newestVar
// names, as the files of the session's one listing of that snapshot that
// match it, in the listing's order: what writeNewestStateBody's listed does
// for a MySQL table's chain, written here again for a PostgreSQL table's.
//
// Each listed file is named through globLiteralSQL, since read_parquet takes
// every entry of a list as a glob again (#2235). The GLOB operator lets `*`
// cross a "/", which the store's glob does not, so a file in a folder below
// the table's is left out: nothing after the table's own folder may hold a
// "/". The table's own file is in the listing whenever the snapshot holds the
// table, so the list is never empty while the table is there; a snapshot
// that no longer holds it fails the view, as it fails a MySQL table's.
func pgFollowListed(in Input, t BaselineTable, glob string) string {
	newest := in.sessionName(newestVar)
	fv := filesVarExpr(in)
	pattern := fmt.Sprintf("getvariable('%s') || %s", newest, sqlString(glob))
	dirOf := fmt.Sprintf("getvariable('%s') || %s", newest, sqlString(t.Rel[:strings.LastIndex(t.Rel, "/")+1]))
	return fmt.Sprintf("CASE WHEN getvariable('%s') IS NULL\n    THEN error(%s)\n"+
		"    WHEN struct_extract(%s, 'dir') IS DISTINCT FROM getvariable('%s')\n    THEN [%s]\n"+
		"    ELSE [%s FOR f IN struct_extract(%s, 'files') IF f GLOB (%s) AND NOT contains(substr(f, length(%s) + 1), '/')] END",
		newest, sqlString(newestVarUnsetMsg), fv, newest, pattern, globLiteralSQL("f"), fv, pattern, dirOf)
}
