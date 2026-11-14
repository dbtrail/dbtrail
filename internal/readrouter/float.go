package readrouter

import (
	"strings"
	"unicode/utf8"
)

// floatDiffers is what a FLOAT does on each side, for the reasons below.
const floatDiffers = "MySQL compares and computes a FLOAT as a double and prints it with 6 significant digits; " +
	"the copy compares and computes it as a single-precision float"

// FloatVeto says why the copy must not answer a statement because it reads a
// FLOAT column, or "" when it can (#2827). shape is Shape of the statement as
// the client sent it (of the template, for a prepared statement); floats are
// the FLOAT columns of the tables the statement reads, all tables together,
// by name; star says the statement holds a star that stands for columns,
// when the caller knows from a parse of it.
//
// MySQL compares and computes a FLOAT as a double: 3.14 stored in one is
// 3.140000104904175, so WHERE f = 3.14 finds no row there, and f * 2 is a
// double. The copy compares it as a float (the literal is cast to one, and
// the row matches), computes in single precision (f * 2 overflows to
// -Infinity near the type's limit), and prints the widened double. Measured
// on MySQL 8.0.46. So a statement that names such a column, anywhere, is kept
// on MySQL, and so is one with a star, which reaches the column without its
// name. Names are looked for as ColumnVeto looks for a TIME or a YEAR: whole
// words, quoted or not, qualified or not, up to case as sameName folds it;
// another table's column, an alias or a function called the same costs a
// statement the copy could have answered, never a wrong answer.
//
// Three statements are refused without being searched, because the search
// could miss: one whose shape was not handed over, one that is not valid
// UTF-8, and any over a FLOAT column whose own name is not made of word
// characters alone.
func FloatVeto(shape string, floats []string, star bool) string {
	if len(floats) == 0 {
		return ""
	}
	kinds := make(map[string]byte, len(floats))
	for _, name := range floats {
		if !utf8.ValidString(name) || !wordName(name) {
			return "the column " + name + " is a FLOAT, and that name cannot be looked for in a statement"
		}
		kinds[sameName(name)] = 1
	}
	if shape == "" {
		return "the statement could not be searched for its FLOAT columns"
	}
	if !utf8.ValidString(shape) {
		return "the statement is not valid UTF-8, so it cannot be searched for FLOAT columns"
	}
	text := namesKept(shape, kinds)
	for i := 0; i < len(text); {
		if !wordByte(text[i]) {
			i++
			continue
		}
		j := i
		for j < len(text) && wordByte(text[j]) {
			j++
		}
		word := sameName(text[i:j])
		if kinds[word] == 0 {
			// A quoted name is written with a $ before it (namesKept).
			word = strings.TrimPrefix(word, "$")
		}
		if kinds[word] != 0 {
			return "the statement names " + word + ", a FLOAT column (" + floatDiffers + ")"
		}
		i = j
	}
	if star || starItem.MatchString(text) {
		return "the statement has a star over a table with a FLOAT column (" + floats[0] + "; " + floatDiffers + ")"
	}
	return ""
}

// FloatVeto is FloatVeto over this shape.
func (s StatementShape) FloatVeto(floats []string, star bool) string {
	return FloatVeto(string(s), floats, star)
}
