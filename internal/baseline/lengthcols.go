package baseline

// lengthCharsets are the character sets whose bytes for a text are the UTF-8
// bytes the copy holds it as: utf8mb4, utf8mb3 (utf8 too, which
// ComparableCharset spells so) and ascii. latin1 is not one: MySQL holds 'é'
// in one byte, the copy in two.
var lengthCharsets = map[string]bool{"utf8mb4": true, "utf8mb3": true, "ascii": true}

// LengthColumns splits a parsed schema into the columns whose LENGTH,
// OCTET_LENGTH, BIT_LENGTH, CHAR_LENGTH and CHARACTER_LENGTH the copy counts
// as MySQL does and all the others (#2918). createSQL is the CREATE TABLE
// cols was parsed from.
//
// Safe are CHAR, VARCHAR and the TEXT types whose character set is one of
// lengthCharsets: MySQL counts the bytes of the text in that character set
// (LENGTH, OCTET_LENGTH; BIT_LENGTH eight times as many) and its characters
// (CHAR_LENGTH, CHARACTER_LENGTH), and the copy, given strlen for the first
// two, counts the bytes and the code points of the same UTF-8 (measured on
// MySQL 8.0.46, MariaDB 10.11.19 and DuckDB 1.5.6, a combining accent and a
// four-byte character included). A CHAR comes back from MySQL without its
// trailing spaces, which is how the copy holds it; PAD_CHAR_TO_FULL_LENGTH,
// which pads it again, keeps a routed session on MySQL.
//
// The character set is ColumnCharsets': the column's own, else the one its
// own collation belongs to, else the table's. A definition that names none (a
// hand-written one) leaves it not known, and the column among the others. Not
// safe either: a character set with other bytes (latin1, utf16, gbk, ...),
// binary, JSON (its own printed form on MySQL), ENUM and SET, numbers and
// dates, which MySQL turns into text its own way, and a value kept verbatim
// as text (RawText).
func LengthColumns(createSQL string, cols []Column) (safe, other []string) {
	charsets := ColumnCharsets(createSQL, cols)
	for _, c := range cols {
		ok := false
		switch c.MySQLType {
		case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
			ok = lengthCharsets[charsets[c.Name]]
		}
		if ok {
			safe = append(safe, c.Name)
		} else {
			other = append(other, c.Name)
		}
	}
	return safe, other
}
