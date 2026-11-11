package metadata

import (
	"bytes"
	"testing"
)

// A latin1 TEXT value arrives from go-mysql as its stored bytes ([]byte) and
// was indexed as those bytes, base64, so a reader got latin1 back where every
// other text is UTF-8 and the copy refused the table (#2634). It is now
// decoded to UTF-8 at capture and still stored as []byte (base64), so every
// reader that decodes TEXT from base64 is unchanged.
func TestCoerceTextEncoding_latin1Text(t *testing.T) {
	for _, dt := range []string{"tinytext", "text", "mediumtext", "longtext", "TEXT"} {
		got, err := coerceTextEncoding([]byte("cr\xe8me \x80 \x81"), ColumnMeta{DataType: dt, CharacterSet: "latin1"})
		if err != nil {
			t.Fatalf("%s: %v", dt, err)
		}
		b, ok := got.([]byte)
		if !ok || !bytes.Equal(b, []byte("crème € \u0081")) {
			t.Fatalf("%s: got %T %q, want []byte UTF-8", dt, got, got)
		}
	}
	// Not latin1, or not text: untouched.
	for _, c := range []ColumnMeta{
		{DataType: "text", CharacterSet: "utf8mb4"},
		{DataType: "text", CharacterSet: ""},
		{DataType: "text", CharacterSet: "latin2"},
		{DataType: "blob", CharacterSet: "latin1"},
	} {
		in := []byte("cr\xe8me")
		got, err := coerceTextEncoding(in, c)
		if err != nil || !bytes.Equal(got.([]byte), in) {
			t.Fatalf("%+v: got %q, %v; want the bytes untouched", c, got, err)
		}
	}
	// NULL stays NULL.
	if got, err := coerceTextEncoding(nil, ColumnMeta{DataType: "text", CharacterSet: "latin1"}); got != nil || err != nil {
		t.Fatalf("nil: got %v, %v", got, err)
	}
}
