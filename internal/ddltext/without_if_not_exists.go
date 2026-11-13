package ddltext

import (
	"regexp"
	"strings"
)

// WithoutIfNotExists returns a CREATE TABLE IF NOT EXISTS statement as the
// plain CREATE TABLE it would be without the clause, and true. A statement
// that is not one of these returns "" and false, and so does one whose clause
// is not right after CREATE TABLE in its text (a comment between the words):
// the caller then has no statement to use, rather than one that still carries
// the clause.
//
// The clause is what a loader must not see in a table's schema file. On a
// target where the table already exists, CREATE TABLE IF NOT EXISTS succeeds
// without creating anything, and the rows that follow are loaded into the
// table that was there, whatever its definition and its rows. A plain CREATE
// TABLE stops there with "table exists", which is what a mydumper schema file
// does.
//
// Only the clause at the head is touched: an IF NOT EXISTS later in the text
// (a column comment, a default) is part of the definition.
func WithoutIfNotExists(stmt string) (string, bool) {
	if !blockCommentsClosed(stmt) || !HasIfNotExists(stmt) {
		return "", false
	}
	loc := ifNotExistsHeadRe.FindStringSubmatchIndex(stmt)
	if loc == nil {
		return "", false
	}
	out := stmt[:loc[3]] + "CREATE TABLE " + stmt[loc[1]:]
	if !CreatesNewTable(out) {
		return "", false
	}
	return out, true
}

// ifNotExistsHeadRe matches CREATE TABLE IF NOT EXISTS at the start of a
// statement, past any leading comments, with the clause written out or inside
// an executable comment (/*!32312 IF NOT EXISTS*/, MariaDB's /*M!...*/).
// Group 1 is the leading comments. The match ends after the clause and the
// space that follows it.
var ifNotExistsHeadRe = regexp.MustCompile(`(?is)^((?:\s+|/\*.*?\*/|--[^\n]*(?:\n|$)|#[^\n]*(?:\n|$))*)` +
	`CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\b|/\*M?!\d*\s*IF\s+NOT\s+EXISTS\s*\*/)\s*`)

// HasIfNotExists says whether stmt reads as CREATE TABLE IF NOT EXISTS,
// wherever comments sit between its words. A caller that cannot take the
// clause out (WithoutIfNotExists returned false) uses it to tell such a
// statement from one that never had the clause.
func HasIfNotExists(stmt string) bool {
	m := VerbRe.FindStringSubmatch(Normalize(stmt))
	if m == nil || !strings.EqualFold(m[1], "CREATE TABLE") {
		return false
	}
	sc := Scanner{S: m[2]}
	return sc.Keyword("IF NOT EXISTS")
}
