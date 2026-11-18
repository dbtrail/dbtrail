package reconstruct

import (
	"slices"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// pgShapeFromFile is a PostgreSQL table's shape as its snapshot file records
// it (the footer's relation: baseline.PGRelation), in the form the index's
// resolver gives one the stream recorded (metadata.WritePGSnapshot): the
// columns in the file's order, numbered from 1, the primary key's marked.
// nil when the file records no column or no primary key, which the caller
// refuses as it refuses a table with no shape.
//
// For a table the stream never described: it describes a table when it first
// carries a change of it, so a table with no write since capture began has no
// shape in the index. Only a run whose window holds no change of the table
// may use this: a change was captured under the shape the source had then,
// which the file does not say.
//
// A column the source computes (GENERATED ... STORED) is not in the file and
// not in rel.Columns, as it is not in a stream's description either.
func pgShapeFromFile(schema, table string, rel baseline.PGRelation) *metadata.TableMeta {
	if len(rel.Columns) == 0 || len(rel.PrimaryKey) == 0 {
		return nil
	}
	tm := &metadata.TableMeta{Schema: schema, Table: table}
	for i, c := range rel.Columns {
		pk := slices.Contains(rel.PrimaryKey, c.Name)
		tm.Columns = append(tm.Columns, metadata.ColumnMeta{
			Name:             c.Name,
			OrdinalPosition:  i + 1,
			IsPK:             pk,
			IsIdentityAlways: c.Identity == "a",
		})
		if pk {
			tm.PKColumns = append(tm.PKColumns, c.Name)
		}
	}
	// Every column of the key must be one of the file's.
	if len(tm.PKColumns) != len(rel.PrimaryKey) {
		return nil
	}
	return tm
}
