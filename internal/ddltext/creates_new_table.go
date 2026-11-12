package ddltext

import "strings"

// CreatesNewTable says whether queryStr is a statement after which its table
// is new: CREATE TABLE or MariaDB's CREATE OR REPLACE TABLE, naming a table,
// without IF NOT EXISTS.
//
// IF NOT EXISTS is the case that matters. The server writes the statement to
// the binary log whether or not the table was there, so an application that
// runs it at every start leaves one record per start for a table that may be
// years old. Such a record says nothing about when the table was created.
//
// A statement that is not read in full as one of these shapes (a comment left
// open, CREATE TEMPORARY TABLE, text cut short before the name, prose) is
// false: the caller offers nothing on a statement it could not read.
func CreatesNewTable(queryStr string) bool {
	if !blockCommentsClosed(queryStr) {
		return false
	}
	m := VerbRe.FindStringSubmatch(Normalize(queryStr))
	if m == nil || !strings.HasPrefix(strings.ToUpper(m[1]), "CREATE") {
		return false
	}
	sc := Scanner{S: m[2]}
	if sc.Keyword("IF NOT EXISTS") {
		return false
	}
	_, ok := sc.QualifiedName("")
	return ok
}
