package verify

import (
	"encoding/json"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestIsDeferredType pins the case list, including the #672 decision: TEXT
// family types are decoded by DecodeEventBinaries (same as BLOB) but are NOT
// deferred — once decoded, TEXT is directly comparable to the baseline/source
// text, unlike ENUM/JSON/binary, so deferring it would mask genuine
// divergences instead of guarding against an unresolved representation gap.
func TestIsDeferredType(t *testing.T) {
	deferred := []string{
		"enum", "set", "json",
		"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bit",
		// #793/#1136: spatial family + VECTOR — binary (SRID+WKB / packed
		// floats) in the event image, decoded by DecodeEventBinaries like
		// BLOB. Spatial defers for when the epoch-typed decode degrades;
		// VECTOR stays permanently unresolved (the baseline side does not
		// store its raw bytes — see deferredValueUnresolved).
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection", // MySQL 8.0.11+ reports the latter
		"vector",
		// MariaDB UUID/INET4/INET6: permanently unresolved (see
		// deferredValueUnresolved's uuid case).
		"uuid", "inet4", "inet6", "UUID",
		"BLOB", "Enum", "GEOMETRY", // case-insensitive
	}
	for _, dt := range deferred {
		if !isDeferredType(dt) {
			t.Errorf("isDeferredType(%q) = false, want true", dt)
		}
	}
	notDeferred := []string{
		"int", "varchar", "char", "datetime", "double", "decimal",
		"text", "tinytext", "mediumtext", "longtext", "TEXT",
	}
	for _, dt := range notDeferred {
		if isDeferredType(dt) {
			t.Errorf("isDeferredType(%q) = true, want false", dt)
		}
	}
}

// TestDeferredReprUnresolved pins the #769/#791 gate: the deferred downgrade
// stays on ONLY for a value the event-side normalization passes provably could
// not resolve (unmapped ENUM ordinal, width-less BIT, numbered/uncanonical
// JSON, untyped binary/spatial, width-less fixed BINARY) — never merely because the table contains a
// deferred column, and never for a value already normalized to the
// baseline/source form (mapped label, width-known BIT, number-free JSON,
// decoded binary).
func TestDeferredReprUnresolved(t *testing.T) {
	intCol := metadata.ColumnMeta{Name: "n", DataType: "int"}
	strCol := metadata.ColumnMeta{Name: "s", DataType: "varchar"}
	enumCol := metadata.ColumnMeta{Name: "kind", DataType: "enum", ColumnType: "enum('active','inactive')"}
	bitCol := metadata.ColumnMeta{Name: "flags", DataType: "bit", ColumnType: "bit(8)"}
	bitNoWidth := metadata.ColumnMeta{Name: "flags", DataType: "bit"} // pre-#212 snapshot
	jsonCol := metadata.ColumnMeta{Name: "meta", DataType: "json"}
	blobCol := metadata.ColumnMeta{Name: "payload", DataType: "blob"}
	geoCol := metadata.ColumnMeta{Name: "loc", DataType: "geometry"}
	pointCol := metadata.ColumnMeta{Name: "loc", DataType: "point", ColumnType: "point"}
	vectorCol := metadata.ColumnMeta{Name: "emb", DataType: "vector", ColumnType: "vector(4)"}
	uuidCol := metadata.ColumnMeta{Name: "u", DataType: "uuid", ColumnType: "uuid"}
	inet4Col := metadata.ColumnMeta{Name: "ip", DataType: "inet4", ColumnType: "inet4"}
	binCol := metadata.ColumnMeta{Name: "v", DataType: "binary", ColumnType: "binary(16)"}
	binNoWidth := metadata.ColumnMeta{Name: "v", DataType: "binary"} // pre-#212 snapshot
	varbinCol := metadata.ColumnMeta{Name: "v", DataType: "varbinary"}

	ch := func(rows ...*query.ResultRow) map[string]*query.ResultRow {
		m := map[string]*query.ResultRow{}
		for i, r := range rows {
			m[string(rune('a'+i))] = r
		}
		return m
	}
	upd := func(after map[string]any) *query.ResultRow {
		return &query.ResultRow{EventType: event.EventUpdate, RowAfter: after}
	}

	cases := []struct {
		name     string
		cols     []metadata.ColumnMeta
		changes  map[string]*query.ResultRow
		binTyped bool
		want     bool
	}{
		{"no deferred column at all", []metadata.ColumnMeta{intCol, strCol},
			ch(upd(map[string]any{"n": json.Number("1"), "s": "x"})), true, false},
		{"no changes", []metadata.ColumnMeta{enumCol}, ch(), true, false},
		{"delete only never gates", []metadata.ColumnMeta{enumCol},
			ch(&query.ResultRow{EventType: event.EventDelete,
				RowBefore: map[string]any{"kind": json.Number("1")}}), true, false},

		// ENUM/SET: mapped label = resolved; leftover ordinal = unresolved.
		// The #769 keystone: a FULL image carrying an ENUM the update did not
		// touch is fine once the label pass mapped it — no ChangedColumns check.
		{"enum mapped to label", []metadata.ColumnMeta{enumCol},
			ch(upd(map[string]any{"kind": "active"})), true, false},
		{"enum ordinal the mapper could not label", []metadata.ColumnMeta{enumCol},
			ch(upd(map[string]any{"kind": json.Number("9")})), true, true},
		{"enum NULL", []metadata.ColumnMeta{enumCol},
			ch(upd(map[string]any{"kind": nil})), true, false},
		{"enum absent from the image", []metadata.ColumnMeta{enumCol},
			ch(upd(map[string]any{"s": "x"})), true, false},
		{"non-ASCII label is charset-ambiguous", []metadata.ColumnMeta{enumCol},
			ch(upd(map[string]any{"kind": "café"})), true, true},

		// BIT: numeric with a declared width renders to exact source bytes.
		{"bit with declared width", []metadata.ColumnMeta{bitCol},
			ch(upd(map[string]any{"flags": json.Number("5")})), true, false},
		{"bit without ColumnType width", []metadata.ColumnMeta{bitNoWidth},
			ch(upd(map[string]any{"flags": json.Number("5")})), true, true},

		// JSON: number-free canonicalizable docs are conclusive; a number
		// literal is ambiguous (a JSONB double 1.0 captures as "1").
		{"json number-free container", []metadata.ColumnMeta{jsonCol},
			ch(upd(map[string]any{"meta": map[string]any{"tags": []any{"a"}}})), true, false},
		{"json with a number literal", []metadata.ColumnMeta{jsonCol},
			ch(upd(map[string]any{"meta": map[string]any{"price": json.Number("1")}})), true, true},
		// JSON is a base64StoredKind text-decode target too (like BLOB): an
		// untyped epoch may leave the value as the raw base64 string it was
		// stored as. "true" is within the base64 alphabet and happens to also
		// be valid bare JSON, so without an explicit binariesTyped guard it
		// would slip through jsonRenderConclusive as "resolved".
		{"json with typing unavailable", []metadata.ColumnMeta{jsonCol},
			ch(upd(map[string]any{"meta": "true"})), false, true},

		// Binary family: resolved only when the decode pass had epoch typing.
		{"blob decoded (typed)", []metadata.ColumnMeta{blobCol},
			ch(upd(map[string]any{"payload": "raw-bytes"})), true, false},
		{"blob with typing unavailable", []metadata.ColumnMeta{blobCol},
			ch(upd(map[string]any{"payload": "cmF3"})), false, true},

		// Fixed BINARY(n): the event image strips trailing 0x00 padding, which
		// renderCell reverses by padding to the declared width (#1135) — so a
		// decoded value resolves ONLY when the width is parseable. A pre-#212
		// snapshot's empty ColumnType leaves the pad width unknown → honest
		// Inconclusive instead of a false MISMATCH.
		{"binary decoded with declared width", []metadata.ColumnMeta{binCol},
			ch(upd(map[string]any{"v": []byte{0xab, 0xcd}})), true, false},
		{"binary without ColumnType width", []metadata.ColumnMeta{binNoWidth},
			ch(upd(map[string]any{"v": []byte{0xab, 0xcd}})), true, true},
		{"binary with typing unavailable", []metadata.ColumnMeta{binCol},
			ch(upd(map[string]any{"v": "q80="})), false, true},
		// VARBINARY has no padding, hence no width requirement — like BLOB.
		{"varbinary decoded (typed)", []metadata.ColumnMeta{varbinCol},
			ch(upd(map[string]any{"v": []byte{0xab}})), true, false},

		// Spatial: decoded by DecodeEventBinaries like BLOB since #1136 — a
		// decoded value resolves; an untyped epoch stays unresolved.
		{"geometry decoded (typed)", []metadata.ColumnMeta{geoCol},
			ch(upd(map[string]any{"loc": []byte{0, 0, 0, 0, 1}})), true, false},
		{"point decoded (typed)", []metadata.ColumnMeta{pointCol},
			ch(upd(map[string]any{"loc": "AAAA"})), true, false},
		{"geometry with typing unavailable", []metadata.ColumnMeta{geoCol},
			ch(upd(map[string]any{"loc": "AAAA"})), false, true},

		// VECTOR: unresolved even when decoded — the baseline side stores the
		// literal dump token, not the raw packed-float bytes, so a decoded
		// event value can never be byte-faithful to the baseline rendering.
		{"vector decoded stays unresolved", []metadata.ColumnMeta{vectorCol},
			ch(upd(map[string]any{"emb": []byte{0, 0, 128, 63}})), true, true},
		{"vector with typing unavailable", []metadata.ColumnMeta{vectorCol},
			ch(upd(map[string]any{"emb": "AACAPw=="})), false, true},

		// MariaDB UUID/INET: unresolved whatever the shape. The source and the
		// baseline render the text form ('12345678-...', '10.0.0.0'), the event
		// holds base64 of the binary form, and events captured before the
		// []byte fix hold damaged raw text.
		{"uuid stored base64", []metadata.ColumnMeta{uuidCol},
			ch(upd(map[string]any{"u": "EjRWeJq8He+AEjRWeJq83g=="})), true, true},
		{"uuid decoded bytes", []metadata.ColumnMeta{uuidCol},
			ch(upd(map[string]any{"u": []byte("0123456789abcdef")})), true, true},
		{"inet4 pre-fix raw text", []metadata.ColumnMeta{inet4Col},
			ch(upd(map[string]any{"ip": "\n"})), true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col, got := deferredReprUnresolved(tc.cols, tc.changes, tc.binTyped)
			if got != tc.want {
				t.Errorf("deferredReprUnresolved = %v, want %v", got, tc.want)
			}
			// The reported column must be the unresolved one (#1136): with a
			// single deferred column per case, unresolved=true must name it.
			if got && len(tc.cols) > 0 {
				want := ""
				for _, c := range tc.cols {
					if isDeferredType(c.DataType) {
						want = c.Name
						break
					}
				}
				if col.Name != want {
					t.Errorf("unresolved column = %q, want %q", col.Name, want)
				}
			}
		})
	}
}
