package baseline

import "strings"

// TextKind is how a text column's values compare on MySQL against the copy's
// SQL session, which folds case and nothing else (#2533). Read routing lets
// the copy answer a statement that names such a column only where the two
// agree, so each kind says what the values must be for that.
type TextKind string

const (
	// TextFolded is a case-insensitive collation with no rules of its own
	// language (folding collations): measured against MySQL 8.0 on every
	// pair of 389 texts, it compares printable ASCII the way the copy does
	// (an equal pair is equal on both sides), letters, digits and inner
	// spaces in the same order too, and nothing else is promised: an accent,
	// 'ß' or a full-width letter is equal to its plain letter on MySQL and
	// not on the copy, a sign (_ [ { ...) sorts elsewhere, and under a PAD
	// SPACE collation (all of these but the 0900 ones; MariaDB's uca1400 ones
	// pad too) a trailing space is not there for MySQL.
	TextFolded TextKind = "folded"
	// TextBytes is a _bin collation of a UTF-8 character set: MySQL compares
	// code points, which is the order of the bytes the copy compares under
	// its COLLATE C (views.BaselineTable.BinaryText). What differs is the
	// padding: utf8mb4_bin is PAD SPACE, so 'a ' equals 'a' on MySQL and a
	// control character sorts below the space it is padded with.
	TextBytes TextKind = "bytes"
	// TextBytesASCII is a _bin collation of any other character set
	// (latin1_bin): MySQL's byte order is its own encoding's, which is the
	// copy's for printable ASCII and not past it ('€' is 0x80 in latin1 and
	// U+20AC on the copy).
	TextBytesASCII TextKind = "bytes_ascii"
	// TextUnlike is a collation the copy compares another way whatever the
	// text: one that tells case apart (_cs), one with a language's rules
	// (Turkish 'I' and 'i' are not a pair, Danish 'aa' is a letter of its
	// own), a character set whose default is neither, a collation not
	// measured, and a column whose collation is not known at all.
	TextUnlike TextKind = "unlike"
)

// TextColumn is one text column of a table and its TextKind. Positional says
// it is an ENUM or a SET, which MySQL's ORDER BY sorts by its position in
// the declared list, where the copy sorts the text. Padded says its collation
// is, or may be, PAD SPACE: MySQL does not see a trailing space, so a value
// cut short to end at one of its inner spaces compares as without it. Only
// the 0900 collations and MariaDB's _nopad_ ones are known not to pad; a
// character set's default is taken to pad, since which one it is depends on
// the server.
type TextColumn struct {
	Name       string
	Kind       TextKind
	Positional bool
	Padded     bool
}

// foldingCollations are the collations TextFolded names: case-insensitive,
// no language's rules. Measured on MySQL 8.0.46 (#2533), with two kinds of
// exception: the utf8 and utf8mb3 spellings of a measured utf8mb4 collation
// (the same algorithm over a smaller character set), and uca1400_ai_ci,
// MariaDB's default since 11.5, which the MariaDB integration job compares.
var foldingCollations = map[string]bool{
	"utf8mb4_0900_ai_ci": true, "utf8mb4_0900_as_ci": true,
	"utf8mb4_general_ci": true, "utf8mb4_unicode_ci": true, "utf8mb4_unicode_520_ci": true,
	"utf8mb4_uca1400_ai_ci": true,
	"utf8mb3_general_ci":    true, "utf8mb3_unicode_ci": true, "utf8mb3_unicode_520_ci": true,
	"utf8mb3_uca1400_ai_ci": true,
	"utf8_general_ci":       true, "utf8_unicode_ci": true, "utf8_unicode_520_ci": true,
	"latin1_swedish_ci": true, "latin1_general_ci": true, "ascii_general_ci": true,
}

// foldingCharsets are the character sets whose default collation is a
// folding one on every server version: utf8mb4's is utf8mb4_0900_ai_ci on
// MySQL 8, utf8mb4_general_ci before it and on older MariaDB, and
// utf8mb4_uca1400_ai_ci on MariaDB 11.5 and later.
var foldingCharsets = map[string]bool{"utf8mb4": true, "utf8mb3": true, "utf8": true, "latin1": true, "ascii": true}

// TextColumns names every text column of a parsed schema (CHAR, VARCHAR, the
// TEXT types, ENUM and SET) with how MySQL compares it against the copy
// (#2533). createSQL is the CREATE TABLE cols was parsed from. The collation
// is resolved as MySQL resolves it: the column's own COLLATE, else its own
// character set's default, else the table's COLLATE, else the table's
// character set's default. Where none is written at all the server's default
// character set applies: SHOW CREATE TABLE names the table's character set
// whenever the server has one, and the defaults (utf8mb4 on MySQL 8 and
// MariaDB, latin1 on MySQL 5.7) are folding ones, so only a CREATE TABLE no
// server printed (a hand-written one) gets here. A value kept verbatim as text (RawText, a
// PostgreSQL source) has no MySQL collation, and is TextUnlike whatever its
// type.
func TextColumns(createSQL string, cols []Column) []TextColumn {
	tableCollation, tableCharset := tableDefaultCollation(createSQL), tableDefaultCharset(createSQL)
	var out []TextColumn
	for _, c := range cols {
		if c.RawText {
			out = append(out, TextColumn{Name: c.Name, Kind: TextUnlike, Padded: true})
			continue
		}
		positional := false
		switch c.MySQLType {
		case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		case "enum", "set":
			positional = true
		default:
			continue
		}
		kind, padded := TextFolded, true
		switch {
		case c.Collation != "":
			kind, padded = collationKind(c.Collation), pads(c.Collation)
		case c.Charset != "":
			kind = charsetKind(c.Charset)
		case tableCollation != "":
			kind, padded = collationKind(tableCollation), pads(tableCollation)
		case tableCharset != "":
			kind = charsetKind(tableCharset)
		}
		out = append(out, TextColumn{Name: c.Name, Kind: kind, Positional: positional, Padded: padded})
	}
	return out
}

// collationKind is the TextKind of a collation named in full, lower case.
func collationKind(collation string) TextKind {
	switch {
	case foldingCollations[collation]:
		return TextFolded
	case strings.HasSuffix(collation, "_bin"):
		switch charset, _, _ := strings.Cut(collation, "_"); charset {
		case "utf8mb4", "utf8mb3", "utf8":
			return TextBytes
		}
		return TextBytesASCII
	}
	return TextUnlike
}

// pads says a collation named in full is PAD SPACE: all but the 0900 ones
// and MariaDB's _nopad_ ones.
func pads(collation string) bool {
	return !strings.Contains(collation, "_0900_") && !strings.Contains(collation, "_nopad_")
}

// charsetKind is the TextKind of a character set's default collation.
func charsetKind(charset string) TextKind {
	if foldingCharsets[charset] {
		return TextFolded
	}
	return TextUnlike
}

// declaredCharset reads a column's CHARACTER SET (or CHARSET) attribute from
// the text after its declared type, lower case, or "" when it names none.
// Only top-level words count, as in declaredCollation.
func declaredCharset(attrs string) string {
	return charsetIn(topLevelWords(attrs))
}

// tableDefaultCharset reads the table's own character set option from an
// embedded CREATE TABLE, lower case, or "" when the options name none. Like
// tableDefaultCollation it reads the line that closes the column list.
func tableDefaultCharset(createSQL string) string {
	for _, line := range strings.Split(createSQL, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, ")") {
			return charsetIn(topLevelWords(trimmed))
		}
	}
	return ""
}

// charsetIn finds CHARSET x or CHARACTER SET x among upper-cased words.
func charsetIn(words []string) string {
	for i := 0; i+1 < len(words); i++ {
		switch {
		case words[i] == "CHARSET" && words[i+1] != "":
			return strings.ToLower(words[i+1])
		case words[i] == "CHARACTER" && words[i+1] == "SET" && i+2 < len(words) && words[i+2] != "":
			return strings.ToLower(words[i+2])
		}
	}
	return ""
}
