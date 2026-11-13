// Package latin1 converts between MySQL's latin1 character set and UTF-8
// (#2634).
//
// MySQL's latin1 is Windows-1252, except for the five bytes Windows-1252
// leaves undefined (0x81, 0x8D, 0x8F, 0x90, 0x9D): MySQL maps those to the C1
// control characters of the same number (measured on 8.0.46: CONVERT(_latin1
// 0x81 USING utf8mb4) is C2 81). Every byte therefore has exactly one
// character, so Decode never fails and Encode(Decode(b)) == b for every b.
//
// The copy (DuckDB over Parquet) reads text as UTF-8 only, while a mydumper
// dump and a binlog TEXT value carry latin1 columns as their stored bytes.
// Decode is applied where those bytes enter the copy; Encode where text goes
// back to MySQL under `SET NAMES binary` (a reconstructed dump) or is compared
// with stored bytes (verify).
package latin1

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
)

// toRune is the character of every byte; fromRune is its inverse.
var (
	toRune   [256]rune
	fromRune = make(map[rune]byte, 256)
)

func init() {
	for b := 0; b < 256; b++ {
		r := charmap.Windows1252.DecodeByte(byte(b))
		if r == utf8.RuneError {
			r = rune(b) // one of the five undefined bytes: MySQL's C1 control
		}
		toRune[b] = r
		fromRune[r] = byte(b)
	}
}

// IsLatin1 reports whether a column's character set, as information_schema or
// a CREATE TABLE names it, is latin1.
func IsLatin1(charset string) bool {
	return strings.EqualFold(strings.TrimSpace(charset), "latin1")
}

// Decode turns latin1 bytes into UTF-8 text. Total: every byte has a character.
func Decode(b string) string {
	ascii := true
	for i := 0; i < len(b); i++ {
		if b[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if ascii {
		return b
	}
	var sb strings.Builder
	sb.Grow(len(b) + len(b)/2)
	for i := 0; i < len(b); i++ {
		sb.WriteRune(toRune[b[i]])
	}
	return sb.String()
}

// Holds reports whether latin1 has a byte for r, which is whether MySQL keeps
// r when it converts text to latin1 rather than writing '?' in its place
// (#2735).
func Holds(r rune) bool {
	_, ok := fromRune[r]
	return ok
}

// Encode turns UTF-8 text back into latin1 bytes. It fails on text that is not
// valid UTF-8 or holds a character latin1 has no byte for: writing either
// would store something other than the text.
func Encode(s string) (string, error) {
	out := make([]byte, 0, len(s))
	for i, r := range s {
		if r == utf8.RuneError {
			if _, size := utf8.DecodeRuneInString(s[i:]); size <= 1 {
				return "", fmt.Errorf("text is not valid UTF-8 at byte %d", i)
			}
		}
		b, ok := fromRune[r]
		if !ok {
			return "", fmt.Errorf("character %q has no latin1 byte", r)
		}
		out = append(out, b)
	}
	return string(out), nil
}

// ToStored turns a latin1 column's value, as the copy and the index hold it,
// into the bytes MySQL stores. Valid UTF-8 is encoded. Text that is not valid
// UTF-8 is a TEXT change captured before #2634, which kept the stored bytes,
// and is returned as it is.
//
// Accepted residual: such a change whose bytes also happen to read as UTF-8
// (latin1 "Ã©", bytes C3 A9) is encoded to E9. Only the bytes are left to tell
// the two apart, and the next full read replaces the change.
func ToStored(s string) (string, error) {
	if !utf8.ValidString(s) {
		return s, nil
	}
	return Encode(s)
}
