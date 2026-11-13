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
// and MariaDB's ALTER IGNORE TABLE (#2640), which keeps the first row of
// each duplicate of a unique key and deletes the others, with no row events.
// It counts when the statement may make a duplicate: it adds a unique key
// (UNIQUE, PRIMARY KEY, a column's bare KEY, SERIAL), or changes values a
// unique key that already exists compares (MODIFY, CHANGE, CONVERT TO, a
// character set or collation; the text does not say whether the column is in
// one). It counts too when the stored text was cut short (capDDLQuery in the
// indexer), since the clause may be in the part cut off. Without IGNORE a
// duplicate fails the statement, and MySQL has had no ALTER IGNORE since 5.7.
// A DROP PRIMARY KEY, a plain index or a new column under IGNORE deletes
// nothing and does not count.
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
	altered, rest, ignore, ok := alterRest(normalize(queryStr, len(queryStr)+1, true))
	if !ok {
		return "", nil
	}
	op, other := rowsClause(rest, true, ignore)
	if strings.ContainsRune(queryStr, '\\') {
		// Read again with the backslash a plain character (sql_mode
		// NO_BACKSLASH_ESCAPES), where the quotes, and so the comments
		// removed, fall elsewhere; and with quotes read as any other byte.
		// What either reading finds counts.
		if a2, rest2, ignore2, ok := alterRest(normalize(queryStr, len(queryStr)+1, false)); ok {
			op2, other2 := rowsClause(rest2, false, ignore2)
			if op == "" {
				op, altered = op2, a2
			}
			if other.Table == "" {
				other = other2
			}
		}
	}
	if op == "" && ignore && strings.Contains(queryStr, strings.TrimSpace(event.QueryTextTruncationMarker)) {
		op = "IGNORE (statement text cut short)"
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
// it, and whether the verb was ALTER [ONLINE] IGNORE TABLE. ok is false for
// any other statement.
func alterRest(norm string) (altered event.DDLTable, rest string, ignore, ok bool) {
	m := VerbRe.FindStringSubmatch(norm)
	if m == nil {
		return event.DDLTable{}, "", false, false
	}
	verb := strings.ToUpper(m[1])
	if !strings.HasPrefix(verb, "ALTER") {
		return event.DDLTable{}, "", false, false
	}
	// VerbRe's group 1 is the verb alone, its words one space apart, so a
	// table called ignore (quoted, IGNORE is reserved) is never in it.
	ignore = strings.Contains(verb, " IGNORE ")
	sc := Scanner{S: m[2]}
	// MariaDB's ALTER TABLE IF EXISTS. IF is reserved, so a table called if
	// is quoted and does not match.
	if sc.Keyword("IF") {
		sc.Keyword("EXISTS")
	}
	altered, ok = sc.QualifiedName("")
	if !ok {
		return event.DDLTable{}, "", false, false
	}
	return altered, sc.S[sc.I:], ignore, true
}

// rowsClause finds the first clause RowsChangedWithoutEvents looks for in s,
// what follows an ALTER TABLE's name. With quoted false, ' and " are read as
// any other byte. A quote left open ends the quote, not the reading. ignore
// says the verb was ALTER IGNORE TABLE, where the words that may make a
// duplicate count (RowsChangedWithoutEvents).
func rowsClause(s string, quoted, ignore bool) (op string, other event.DDLTable) {
	sc := Scanner{S: s}
	prev := "" // the word before this one, outside quotes
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
			word := strings.ToUpper(sc.S[start:sc.I])
			before := prev
			prev = word
			switch word {
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
				if ignore && sc.Keyword("TO") {
					return "IGNORE ... CONVERT TO", event.DDLTable{}
				}
			case "DISCARD", "IMPORT":
				// Only before TABLESPACE or PARTITION: both words are
				// unreserved, so a column may be called either.
				if sc.Keyword("TABLESPACE") || sc.Keyword("PARTITION") {
					return word + " TABLESPACE", event.DDLTable{}
				}
			case "UNIQUE":
				// Reserved: a column or key called unique is quoted, and
				// unique_code is another word.
				if ignore {
					return "IGNORE ... UNIQUE", event.DDLTable{}
				}
			case "PRIMARY":
				if ignore && before != "DROP" {
					return "IGNORE ... PRIMARY KEY", event.DDLTable{}
				}
			case "KEY":
				// In a column definition a bare KEY is PRIMARY KEY. After
				// the words in keyNotPrimary it names an index, a foreign key,
				// a partitioning, or the PRIMARY KEY a DROP removes.
				if ignore && !keyNotPrimary[before] {
					return "IGNORE ... PRIMARY KEY", event.DDLTable{}
				}
			case "SERIAL", "MODIFY", "CHANGE", "COLLATE":
				if ignore {
					return "IGNORE ... " + word, event.DDLTable{}
				}
			case "CHARACTER", "CHARSET":
				if ignore {
					return "IGNORE ... CHARACTER SET", event.DDLTable{}
				}
			}
		default:
			sc.I++
		}
	}
	return "", event.DDLTable{}
}

// keyNotPrimary holds the words after which KEY does not mean PRIMARY KEY.
var keyNotPrimary = map[string]bool{
	"ADD": true, "DROP": true, "RENAME": true, "FOREIGN": true, "FULLTEXT": true,
	"SPATIAL": true, "VECTOR": true, "BY": true, "LINEAR": true, "PRIMARY": true,
	"UNIQUE": true,
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
