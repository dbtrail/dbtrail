package baseline

import (
	"log/slog"
	"unicode/utf8"
)

// textCheck warns, once per column of a table, when a value about to be
// written to a Parquet string column is not valid UTF-8. DuckDB refuses such a
// file whole ("Invalid string encoding found in Parquet file"), so the copy
// and every reconstruct of the table would fail on it long after the baseline
// was taken. latin1 is decoded on the way in (#2634); a column in another
// single-byte character set (latin2, cp1251, ...) dumped as stored bytes still
// reaches here as it came, and this says so while the conversion runs. The
// value itself is never logged.
type textCheck struct {
	db, table string
	file      string // the data file being read, for the warning
	cols      []Column
	charsets  map[string]string
	isText    []bool
	warned    []bool
}

func newTextCheck(tf TableFiles, createSQL string, cols []Column) *textCheck {
	c := &textCheck{db: tf.Database, table: tf.Table, cols: cols, isText: make([]bool, len(cols)), warned: make([]bool, len(cols))}
	found := false
	for i, col := range cols {
		if col.ParquetType == nil {
			continue
		}
		if lt := col.ParquetType.Type().LogicalType(); lt != nil && lt.UTF8 != nil {
			c.isText[i] = true
			found = true
		}
	}
	if !found {
		return nil
	}
	c.charsets = ColumnCharsets(createSQL, cols)
	return c
}

// setFile names the data file the rows that follow come from.
func (c *textCheck) setFile(path string) {
	if c != nil {
		c.file = path
	}
}

// row checks one row's values, as they will be written.
func (c *textCheck) row(values []string, nulls []bool) {
	if c == nil {
		return
	}
	for i, v := range values {
		if i >= len(c.isText) || !c.isText[i] || c.warned[i] || (i < len(nulls) && nulls[i]) || utf8.ValidString(v) {
			continue
		}
		c.warned[i] = true
		slog.Warn("baseline: a text value is not valid UTF-8, so this table's Parquet file cannot be read back "+
			"(the copy and reconstruct will refuse it); the conversion decodes only latin1, and this column's "+
			"character set reached the dump as stored bytes",
			"db", c.db, "table", c.table, "column", c.cols[i].Name, "charset", c.charsets[c.cols[i].Name], "file", c.file)
	}
}
