package baseline

import "log/slog"

// collectPGTyped records a PostgreSQL-source file's MetaKeyPGTypedColumns
// value, for applyPGTyped.
func (st *footerScan) collectPGTyped(file, value string) {
	if st.pgTyped == nil {
		st.pgTyped = map[string]string{}
	}
	st.pgTyped[file] = value
}

// applyPGTyped records, for each PostgreSQL-source file that stores columns
// typed, the columns it stores and as what (TableFooter.PGStored).
// A value that does not parse (a newer version wrote it), or that does not
// agree with the file's own table shape, is recorded as why
// (TableFooter.PGTypedUnusable): the pinned views then refuse the table when
// it is read, rather than read a stored column through a cast written for
// its text or read it as it is.
func (st *footerScan) applyPGTyped() {
	for file, value := range st.pgTyped {
		f, ok := st.footers[file]
		if !ok || f.PG == nil || f.PGRelation == nil {
			continue
		}
		typed, err := ParsePGTypedColumns(value)
		if err == nil {
			err = CheckPGTypedColumns(*f.PGRelation, typed)
		}
		if err != nil {
			f.PGTypedUnusable = err.Error()
			st.footers[file] = f
			slog.Warn("baseline: the list of columns a PostgreSQL snapshot file stores as their type cannot be used; "+
				"this table's state view refuses to be read until the snapshot is taken again by this version",
				"path", file, "error", err)
			continue
		}
		if len(typed) > 0 {
			f.PGStored = typed
			st.footers[file] = f
		}
	}
}
