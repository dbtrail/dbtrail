package verify

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// The source side of a verify digest reads a latin1 column's stored bytes
// (character_set_results = binary); the reconstructed side holds the text as
// UTF-8 since #2634. renderCell renders the stored bytes, so the two meet.
func TestRenderCell_latin1Text(t *testing.T) {
	l1 := metadata.ColumnMeta{Name: "v", DataType: "varchar", CharacterSet: "latin1"}
	for _, c := range []struct {
		col  metadata.ColumnMeta
		v    any
		want string
	}{
		{l1, "café", "caf\xe9"},
		{l1, []byte("naïve €"), "na\xefve \x80"},
		{l1, "caf\xe9", "caf\xe9"}, // captured before #2634: already stored bytes
		{metadata.ColumnMeta{DataType: "TEXT", CharacterSet: "LATIN1"}, "crème", "cr\xe8me"},
		{metadata.ColumnMeta{DataType: "enum", CharacterSet: "latin1"}, "sí", "s\xed"},
		{metadata.ColumnMeta{DataType: "varchar", CharacterSet: "utf8mb4"}, "café", "café"},
		{metadata.ColumnMeta{DataType: "blob", CharacterSet: "latin1"}, []byte("café"), "café"},
		{l1, "日本", "日本"}, // no latin1 bytes: left, and cannot match
	} {
		if got := string(renderCell(c.v, c.col)); got != c.want {
			t.Errorf("renderCell(%q, %s/%s) = %q, want %q", c.v, c.col.DataType, c.col.CharacterSet, got, c.want)
		}
	}
	if renderCell(nil, l1) != nil {
		t.Error("NULL must render nil")
	}
}
