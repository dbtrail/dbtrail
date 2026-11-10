package ddltext

import (
	"strings"

	"github.com/dbtrail/dbtrail/internal/event"
)

// RowsChangedWithoutEvents reads an ALTER TABLE for a clause that removes,
// swaps or brings in rows without writing them to the binlog as row events
// (#2632):
//
//	DROP PARTITION, TRUNCATE PARTITION
//	EXCHANGE PARTITION p WITH TABLE t2       (both tables change)
//	CONVERT PARTITION p TO TABLE t2          (MariaDB)
//	CONVERT TABLE t2 TO PARTITION p          (MariaDB)
//	DISCARD [PARTITION ...] TABLESPACE, IMPORT [PARTITION ...] TABLESPACE
//
// op names the first such clause, "" when there is none. tables is the
// altered table first, then the table the clause names, if any; a name the
// statement does not qualify has an empty schema, for the caller to resolve.
//
// Reading it wrong one way costs a full read of the source; the other way
// leaves rows in a copy that the source no longer has, with nothing to say
// so. So it leans to finding: text inside quotes is skipped, but when the
// statement holds a backslash, whose meaning inside a quote depends on the
// session's sql_mode, it is read a second time as if quotes were not there,
// and a quote left open hides nothing.
func RowsChangedWithoutEvents(queryStr string) (op string, tables []event.DDLTable) {
	altered, rest, ok := alterRest(normalize(queryStr, len(queryStr)+1, true))
	if !ok {
		return "", nil
	}
	op, other := rowsClause(rest, true)
	if strings.ContainsRune(queryStr, '\\') {
		// Read again with the backslash a plain character (sql_mode
		// NO_BACKSLASH_ESCAPES), where the quotes, and so the comments
		// removed, fall elsewhere; and with quotes read as any other byte.
		// What either reading finds counts.
		if a2, rest2, ok := alterRest(normalize(queryStr, len(queryStr)+1, false)); ok {
			op2, other2 := rowsClause(rest2, false)
			if op == "" {
				op, altered = op2, a2
			}
			if other.Table == "" {
				other = other2
			}
		}
	}
	if op == "" {
		return "", nil
	}
	tables = []event.DDLTable{altered}
	if other.Table != "" {
		tables = append(tables, other)
	}
	return op, tables
}

// alterRest reads a normalized ALTER TABLE's table and returns what follows
// it. ok is false for any other statement.
func alterRest(norm string) (altered event.DDLTable, rest string, ok bool) {
	m := VerbRe.FindStringSubmatch(norm)
	if m == nil || !strings.HasPrefix(strings.ToUpper(m[1]), "ALTER") {
		return event.DDLTable{}, "", false
	}
	sc := Scanner{S: m[2]}
	// MariaDB's ALTER TABLE IF EXISTS. IF is reserved, so a table called if
	// is quoted and does not match.
	if sc.Keyword("IF") {
		sc.Keyword("EXISTS")
	}
	altered, ok = sc.QualifiedName("")
	if !ok {
		return event.DDLTable{}, "", false
	}
	return altered, sc.S[sc.I:], true
}

// rowsClause finds the first clause RowsChangedWithoutEvents looks for in s,
// what follows an ALTER TABLE's name. With quoted false, ' and " are read as
// any other byte. A quote left open ends the quote, not the reading.
func rowsClause(s string, quoted bool) (op string, other event.DDLTable) {
	sc := Scanner{S: s}
	for sc.I < len(sc.S) {
		c := sc.S[sc.I]
		switch {
		case c == '`' || quoted && (c == '\'' || c == '"'):
			if end := skipQuoted(sc.S, sc.I, c != '`'); end > 0 {
				sc.I = end
			} else {
				sc.I++
			}
		case IsNameRune(rune(c)) || c >= 0x80:
			start := sc.I
			for sc.I < len(sc.S) && (IsNameRune(rune(sc.S[sc.I])) || sc.S[sc.I] >= 0x80) {
				sc.I++
			}
			switch strings.ToUpper(sc.S[start:sc.I]) {
			case "DROP":
				if sc.Keyword("PARTITION") {
					return "DROP PARTITION", event.DDLTable{}
				}
			case "TRUNCATE":
				if sc.Keyword("PARTITION") {
					return "TRUNCATE PARTITION", event.DDLTable{}
				}
			case "EXCHANGE":
				if sc.Keyword("PARTITION") {
					if _, ok := sc.Ident(); ok && sc.Keyword("WITH") && sc.Keyword("TABLE") {
						other, _ = sc.QualifiedName("")
					}
					return "EXCHANGE PARTITION", other
				}
			case "CONVERT":
				if sc.Keyword("PARTITION") {
					if _, ok := sc.Ident(); ok && sc.Keyword("TO") && sc.Keyword("TABLE") {
						other, _ = sc.QualifiedName("")
					}
					return "CONVERT PARTITION", other
				}
				if sc.Keyword("TABLE") {
					other, _ = sc.QualifiedName("")
					return "CONVERT TABLE", other
				}
			case "DISCARD", "IMPORT":
				// Only before TABLESPACE or PARTITION: both words are
				// unreserved, so a column may be called either.
				verb := strings.ToUpper(sc.S[start:sc.I])
				if sc.Keyword("TABLESPACE") || sc.Keyword("PARTITION") {
					return verb + " TABLESPACE", event.DDLTable{}
				}
			}
		default:
			sc.I++
		}
	}
	return "", event.DDLTable{}
}

// skipQuoted returns the index just past the quote that closes the one at
// s[i], a doubled quote standing for itself and, when backslash is set, a
// backslash escaping the byte after it; 0 when it is left open.
func skipQuoted(s string, i int, backslash bool) int {
	q := s[i]
	for j := i + 1; j < len(s); j++ {
		switch {
		case backslash && s[j] == '\\':
			j++
		case s[j] != q:
		case j+1 < len(s) && s[j+1] == q:
			j++
		default:
			return j + 1
		}
	}
	return 0
}
