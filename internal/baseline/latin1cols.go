package baseline

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/dbtrail/dbtrail/internal/latin1"
)

// Latin1Columns says, per column of a parsed schema, whether it is a text
// column (CHAR, VARCHAR, the TEXT types, ENUM, SET) whose character set is
// latin1 (#2634). The character set resolves as MySQL resolves it: the
// column's own COLLATE or CHARACTER SET, else the table's COLLATE or
// character set. A CREATE TABLE that names none at all is not assumed to be
// latin1: SHOW CREATE TABLE always names the table's.
//
// A dump holds such a column's stored latin1 bytes, which the copy cannot read
// (DuckDB refuses a Parquet string that is not UTF-8, and refuses the whole
// file with it), so the conversion decodes them into UTF-8.
func Latin1Columns(createSQL string, cols []Column) []bool {
	tableCharset := tableDefaultCharset(createSQL)
	if c := tableDefaultCollation(createSQL); c != "" {
		tableCharset, _, _ = strings.Cut(c, "_")
	}
	out := make([]bool, len(cols))
	for i, c := range cols {
		if c.RawText {
			continue
		}
		switch c.MySQLType {
		case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
		default:
			continue
		}
		charset := tableCharset
		switch {
		case c.Collation != "":
			charset, _, _ = strings.Cut(c.Collation, "_")
		case c.Charset != "":
			charset = c.Charset
		}
		out[i] = latin1.IsLatin1(charset)
	}
	return out
}

// setNamesRe finds the SET NAMES a dump file loads under.
var setNamesRe = regexp.MustCompile(`(?i)\bSET\s+NAMES\s+'?([a-z0-9_]+)'?`)

// headerSaysBinary reports whether a data file's header loads it under SET
// NAMES binary, mydumper's default, which is what a dump of stored bytes
// carries. A file that names none is taken as mydumper's default. Only the
// head of the file is read.
//
// The header is a claim, not proof: see fileHoldsStoredLatin1.
func headerSaysBinary(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return true // the read that follows fails on it loudly
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 64*1024)
	for n := 0; n < 20 && sc.Scan(); n++ {
		line := sc.Text()
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "INSERT") {
			break
		}
		if m := setNamesRe.FindStringSubmatch(line); m != nil {
			// Under SET NAMES latin1 the server sends a latin1 column's stored
			// bytes as they are, the same as binary.
			return strings.EqualFold(m[1], "binary") || latin1.IsLatin1(m[1])
		}
	}
	return true
}

// errStoredLatin1Found ends fileHoldsStoredLatin1's scan at the first proof.
var errStoredLatin1Found = errors.New("stored latin1 bytes found")

// fileHoldsStoredLatin1 reports whether one data file holds its latin1 columns
// as the stored bytes (to be decoded into UTF-8) rather than already converted
// to UTF-8. It is decided per file, because mydumper decides it per thread.
//
// mydumper 1.0.3 with its stock /etc/mydumper.cnf, against MySQL 8 and
// MariaDB 11 and 12, writes SET NAMES utf8mb4 at the top of every data file
// but sets character_set_results = NULL on every connection, and only a
// connection that ran a schema job sets it back to utf8mb4. A thread that
// dumped rows before any schema job writes the stored latin1 bytes under that
// utf8mb4 header; the others write UTF-8. Which thread takes which table is a
// race, so the same table comes out one way or the other from run to run.
//
// So a header other than binary is checked against the rows. A server
// converting latin1 to utf8mb4 writes valid UTF-8 and only characters latin1
// has, so a latin1 value that is not valid UTF-8, or that holds a character
// latin1 has no byte for (stored UTF-8 bytes such as 'Å‘', which read as
// 'ő'), proves the whole file holds stored bytes. A file whose latin1 values
// pass both is taken as converted, as its header says. What cannot be told
// apart is stored text whose every byte above 0x7f forms UTF-8 of characters
// latin1 has (such as 'Ã©', stored as the bytes of 'é'): unless another value
// of the same file settles it, such a file is read as converted.
//
// A tab-separated dump (.dat) has no header and is taken as stored bytes, as
// is a file whose header says binary or latin1: neither is scanned.
func fileHoldsStoredLatin1(ctx context.Context, path, format string, isLatin1 []bool) (bool, error) {
	if format != "sql" || headerSaysBinary(path) {
		return true, nil
	}
	err := ReadSQLFile(path, func(values []string, nulls []bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i, v := range values {
			if i < len(isLatin1) && isLatin1[i] && (i >= len(nulls) || !nulls[i]) && !convertedLatin1(v) {
				return errStoredLatin1Found
			}
		}
		return nil
	})
	switch {
	case errors.Is(err, errStoredLatin1Found):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("scan for its latin1 encoding: %w", err)
	}
	return false, nil
}

// convertedLatin1 reports whether v can be a latin1 value a server converted
// to UTF-8: valid UTF-8 made only of characters latin1 has.
func convertedLatin1(v string) bool {
	if !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if !latin1.Holds(r) {
			return false
		}
	}
	return true
}

// decodeLatin1Row returns values with every latin1 column decoded to UTF-8, or
// values itself when no column needs it. values is never written to: the
// content digest is taken over the stored bytes (#633), as the live checksum
// reads them.
func decodeLatin1Row(values []string, nulls []bool, isLatin1 []bool) []string {
	var out []string
	for i, v := range values {
		if i >= len(isLatin1) || !isLatin1[i] || (i < len(nulls) && nulls[i]) {
			continue
		}
		d := latin1.Decode(v)
		if d == v {
			continue
		}
		if out == nil {
			out = append([]string(nil), values...)
		}
		out[i] = d
	}
	if out == nil {
		return values
	}
	return out
}

// storedLatin1Row returns values with every latin1 column turned back into
// the bytes the server stores, or values itself when no column needs it. It
// is the content digest's view of a file that came already converted to
// UTF-8: the digest is taken over the stored bytes (#633), as the live
// checksum reads them, whichever way the dump wrote them. A value latin1
// cannot hold (which a converted latin1 column does not carry) is kept as it
// came.
func storedLatin1Row(values []string, nulls []bool, isLatin1 []bool) []string {
	var out []string
	for i, v := range values {
		if i >= len(isLatin1) || !isLatin1[i] || (i < len(nulls) && nulls[i]) {
			continue
		}
		s, err := latin1.ToStored(v)
		if err != nil || s == v {
			continue
		}
		if out == nil {
			out = append([]string(nil), values...)
		}
		out[i] = s
	}
	if out == nil {
		return values
	}
	return out
}
