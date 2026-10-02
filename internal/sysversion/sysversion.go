// Package sysversion holds the facts about MariaDB system-versioned tables
// (`WITH SYSTEM VERSIONING`) that the readers of captured changes share
// (#2007).
//
// A versioned table keeps every old version of a row inside the table itself,
// and that changes what its binlog says. Measured on MariaDB 11.4 and 11.8:
//
//   - MariaDB adds the ROW END period column to the PRIMARY KEY
//     (`PRIMARY KEY (id, row_end)`), so every captured pk_values carries it.
//   - A row that is still current has ROW END = the largest TIMESTAMP the
//     server can store (the "current" marker below). Any other value is the
//     moment that version stopped being current.
//   - An UPDATE logs the update of the current row AND an INSERT of the old
//     version, whose ROW END is the moment of the update (a history row).
//   - A DELETE logs no delete: it is an UPDATE that moves ROW END from the
//     current marker to the moment of the delete.
//   - DELETE HISTORY (and partition rotation) logs real deletes, but only of
//     history rows.
//
// So "is this row current?" is a question about one value, and every reader
// that wants the table as the application sees it must ask it. RowEnd answers
// it, and refuses a value it cannot place instead of guessing.
package sysversion

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// State is where a row version stands, read from its ROW END value.
type State int

const (
	// Current is the live version of the row: what a plain SELECT returns.
	Current State = iota + 1
	// History is a version that ended: kept by the server, invisible to a
	// plain SELECT.
	History
)

func (s State) String() string {
	switch s {
	case Current:
		return "current"
	case History:
		return "history"
	}
	return "unknown"
}

// The ROW END a current row carries: the largest TIMESTAMP(6) the server can
// store, rendered in UTC the way the parser stores TIMESTAMP values (#757).
// MariaDB 11.5 widened TIMESTAMP on 64-bit builds, so the marker depends on
// the server version; both are real and both are in the field (CI runs 10.11,
// 11.4, 11.8 and 12.3).
var currentMarkers = map[string]bool{
	"2038-01-19 03:14:07.999999": true, // up to 11.4
	"2106-02-07 06:28:15.999999": true, // 11.5 and later, 64-bit
}

// CurrentMarkers returns every ROW END value that marks a current row, in a
// fixed order, for lookups that must spell a current row's stored key.
func CurrentMarkers() []string {
	return []string{"2038-01-19 03:14:07.999999", "2106-02-07 06:28:15.999999"}
}

// IsCurrentMarker reports whether v is one of the current-row markers.
func IsCurrentMarker(v any) bool {
	s, ok := v.(string)
	return ok && currentMarkers[s]
}

// historySlack is how far past the moment of its own event a history row's
// ROW END may sit and still be read as "ended". MariaDB stamps ROW END with
// the statement's time and the binlog event with the same clock, so the two
// agree to within the statement; the slack only absorbs a long statement.
// Anything further in the future is no known marker and no plausible past:
// it is refused rather than guessed.
const historySlack = 24 * time.Hour

// RowEnd places one ROW END value. eventAt is the time of the event that
// carried it.
//
// It errors on a value it cannot place: missing, not a TIMESTAMP (a
// transaction-precise table stores a BIGINT transaction id there, which this
// package does not read), or a future time that is not a current marker
// (a server whose marker this build does not know). Callers must refuse on
// that error: reading such a row as either current or ended would silently
// drop or resurrect it.
func RowEnd(v any, eventAt time.Time) (State, error) {
	if v == nil {
		return 0, fmt.Errorf("the row has no value in its system-versioning end column")
	}
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("the system-versioning end value %v (%T) is not a TIMESTAMP; "+
			"transaction-precise versioning (BIGINT UNSIGNED period columns) is not supported", v, v)
	}
	if currentMarkers[s] {
		return Current, nil
	}
	ts, err := time.ParseInLocation("2006-01-02 15:04:05.999999", s, time.UTC)
	if err != nil {
		return 0, fmt.Errorf("the system-versioning end value %q is not a TIMESTAMP", s)
	}
	if ts.After(eventAt.Add(historySlack)) {
		return 0, fmt.Errorf("the system-versioning end value %q is neither the marker of a current row "+
			"nor a moment before its event (%s); this build does not know how this server marks current rows",
			s, eventAt.UTC().Format(time.RFC3339))
	}
	return History, nil
}

// Period names a versioned table's two period columns.
type Period struct {
	Start, End string
}

// The names MariaDB gives the hidden period columns of a table versioned
// without declaring them (`CREATE TABLE t (...) WITH SYSTEM VERSIONING`).
// The binlog's optional metadata names them exactly this (#1272).
const (
	ImplicitStart = "row_start"
	ImplicitEnd   = "row_end"
)

var (
	withVersioningRe = regexp.MustCompile(`(?i)\bWITH\s+SYSTEM\s+VERSIONING\b`)
	periodRe         = regexp.MustCompile("(?i)\\bPERIOD\\s+FOR\\s+SYSTEM_TIME\\s*\\(\\s*(`(?:[^`]|``)+`|[A-Za-z0-9_$]+)\\s*,\\s*(`(?:[^`]|``)+`|[A-Za-z0-9_$]+)\\s*\\)")
)

// FromCreateTable reports whether a CREATE TABLE statement, as SHOW CREATE
// TABLE or mydumper writes it, declares a system-versioned table, and names
// its period columns: those of `PERIOD FOR SYSTEM_TIME (start, end)` when the
// columns are declared, else the hidden row_start/row_end.
//
// Only the table options count (the text after the column list closes): a
// column's own `WITHOUT SYSTEM VERSIONING`, or the words inside a quoted
// COMMENT, do not make a table versioned.
func FromCreateTable(createSQL string) (Period, bool) {
	masked := maskStrings(createSQL)
	open := strings.IndexByte(masked, '(')
	if open < 0 {
		return Period{}, false
	}
	close := matchingParen(masked, open)
	if close < 0 {
		return Period{}, false
	}
	if !withVersioningRe.MatchString(masked[close+1:]) {
		return Period{}, false
	}
	body := masked[open+1 : close]
	if m := periodRe.FindStringSubmatch(body); m != nil {
		return Period{Start: unquoteIdent(m[1]), End: unquoteIdent(m[2])}, true
	}
	return Period{Start: ImplicitStart, End: ImplicitEnd}, true
}

// maskStrings blanks the inside of '...' and "..." string literals (same
// length, so offsets still line up) and leaves `identifiers` alone, so a
// COMMENT can neither open a parenthesis nor spell a table option.
func maskStrings(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		q := b[i]
		switch q {
		case '\'', '"':
			for j := i + 1; j < len(b); j++ {
				if b[j] == '\\' && j+1 < len(b) {
					b[j], b[j+1] = ' ', ' '
					j++
					continue
				}
				if b[j] == q {
					if j+1 < len(b) && b[j+1] == q { // doubled quote
						b[j], b[j+1] = ' ', ' '
						j++
						continue
					}
					i = j
					break
				}
				b[j] = ' '
				i = j
			}
		case '`':
			for j := i + 1; j < len(b); j++ {
				if b[j] == '`' {
					if j+1 < len(b) && b[j+1] == '`' {
						j++
						continue
					}
					i = j
					break
				}
				i = j
			}
		}
	}
	return string(b)
}

// matchingParen returns the index of the parenthesis closing the one at open,
// skipping `identifiers` (string literals are already masked), or -1.
func matchingParen(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '`':
			for i++; i < len(s); i++ {
				if s[i] == '`' {
					if i+1 < len(s) && s[i+1] == '`' {
						i++
						continue
					}
					break
				}
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func unquoteIdent(s string) string {
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return strings.ReplaceAll(s[1:len(s)-1], "``", "`")
	}
	return s
}

// FromSnapshot names the period of a table whose schema snapshot has the
// system-versioning shape, for readers that hold the snapshot but neither a
// CREATE TABLE statement nor a known source flavor (recovery over an index
// whose flavor is not recorded): exactly one generated column in the primary
// key (ROW END, which MariaDB appends to the key) and exactly one other
// generated column outside it (ROW START), both TIMESTAMP or both BIGINT.
//
// BIGINT is the transaction-precise form, accepted here so it is DETECTED:
// RowEnd then refuses its values instead of the table passing as an
// ordinary one.
//
// MariaDB refuses a generated column in a primary key ("Primary key cannot
// be defined upon a generated column") except ROW END. MySQL has no system
// versioning; a MySQL table would need both a stored generated column in its
// key and a second one of the same type beside it to match.
func FromSnapshot(cols []metadata.ColumnMeta) (Period, bool) {
	var p Period
	var startType, endType string
	for _, c := range cols {
		if !c.IsGenerated {
			continue
		}
		typ := strings.ToLower(strings.TrimSpace(c.DataType))
		if typ != "timestamp" && typ != "bigint" {
			continue
		}
		slot, slotType := &p.Start, &startType
		if c.IsPK {
			slot, slotType = &p.End, &endType
		}
		if *slot != "" {
			return Period{}, false
		}
		*slot, *slotType = c.Name, typ
	}
	if p.Start == "" || p.End == "" || startType != endType {
		return Period{}, false
	}
	return p, true
}
