package baseline

import (
	"bufio"
	"os"
	"regexp"
	"strings"

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

// dumpIsBinary reports whether a table's dump holds its text as stored bytes,
// which is what a latin1 column needs decoded. mydumper writes SET NAMES
// binary at the top of every data file and dumps the bytes as stored; a dump
// made with another SET NAMES (mydumper --set-names utf8mb4) already holds
// UTF-8, which decoding again would garble. A file that names none is taken as
// mydumper's default. Only the head of the first file is read.
func dumpIsBinary(tf TableFiles) bool {
	// A tab-separated dump (.dat) has no header to read: its first lines are
	// rows, which may well hold the words SET NAMES. mydumper writes it as
	// stored bytes too.
	if tf.Format != "sql" || len(tf.DataFiles) == 0 {
		return true
	}
	f, err := os.Open(tf.DataFiles[0])
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
			return strings.EqualFold(m[1], "binary")
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
