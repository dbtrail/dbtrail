package reconstruct

import (
	"context"
	"database/sql"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// This file is the visibility seam for the Iceberg export (#1466). Every
// function here is a thin exported name over logic that already exists in
// this package; none of them changes what the recovery path does. The export
// lives in its own package because the Iceberg library must never be linked
// by anything `recover` traverses (see cliapp/icebergfree_test.go), and that
// package needs the same per-event guards and decoders the full-table fold
// runs, so that the exported table equals `reconstruct` at the same cut.

// PKChangedInEvent reports whether ev is an UPDATE whose primary key changed
// (#782). The Iceberg export folds events by the before-image PK exactly like
// the full-table merge, so it refuses the same events for the same reason.
func PKChangedInEvent(ev *query.ResultRow, pkCols []metadata.ColumnMeta) (before, after string, changed bool) {
	return pkChangedInEvent(ev, pkCols)
}

// PKChangingUpdateError is the refusal message for a PK-changing UPDATE, the
// same words the full-table reconstruct uses.
func PKChangingUpdateError(schema, table, before, after string) error {
	return pkChangingUpdateErr(schema, table, before, after)
}

// CheckBaselineSchemaCurrent refuses with ErrSchemaChanged when the table's
// current schema snapshot has columns the baseline's embedded CREATE TABLE
// lacks, or the reverse. Column names only: unlike the fold, the export does
// not publish the carried CREATE TABLE, and it checks the types it exports
// itself (sameTableTypes).
func CheckBaselineSchemaCurrent(createSQL string, tm *metadata.TableMeta, schema, table string) error {
	return checkBaselineSchema(createSQL, tm, schema, table, false)
}

// MaterializeBaselineLocal returns a local path for a baseline Parquet file:
// the file itself (after the #636 integrity check) for a local baseline, or a
// temporary download for an s3:// one. The returned cleanup must be called.
func MaterializeBaselineLocal(ctx context.Context, path string, tuning duckdbutil.Tuning) (string, func(), error) {
	return materializeBaselineLocal(ctx, path, tuning)
}

// IsBase64StoredType reports whether a column of this DATA_TYPE is stored
// base64-encoded in the event row images (BLOB and TEXT families, JSON, the
// spatial types, VECTOR), i.e. one whose exported value depends on the epoch
// decoder having resolved. One list, the decoder's own.
func IsBase64StoredType(dataType string) bool {
	_, ok := base64StoredKind(dataType)
	return ok
}

// EventDecoder is the per-table, epoch-aware decoder the full-table fold
// builds once and reuses across pages: ENUM/SET ordinals become labels and
// base64-stored values become real values. BLOB/TEXT are decoded only against
// the schema snapshot in effect at each event's own timestamp; ENUM/SET fall
// back to the LATEST snapshot when the epochs cannot be loaded, and Typed
// reports the first case only. Not safe for concurrent use.
type EventDecoder struct{ d *eventDecoder }

// NewEventDecoder builds the decoder for one table run. latest may be nil.
func NewEventDecoder(db *sql.DB, schema, table string, latest *metadata.Resolver) *EventDecoder {
	return &EventDecoder{d: newEventDecoder(db, schema, table, latest)}
}

// DecodePage decodes one fetched page in place. Call it exactly once per
// page: the base64 pass is not idempotent.
func (e *EventDecoder) DecodePage(page []query.ResultRow) {
	e.d.mapEnums(page)
	e.d.decodeBinaries(page)
}

// Typed reports whether every event decoded so far resolved to a schema
// epoch. False means at least one event could not be typed against the
// snapshot in effect at its timestamp: a base64-stored value may still be
// the stored text, and an ENUM/SET ordinal was mapped by the latest
// definition or not at all.
func (e *EventDecoder) Typed() bool { return e.d.typed }
