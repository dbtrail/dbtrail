package shim

import (
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// capturePW is an in-memory packetWriter that records the payload of every
// WritePacket call (stripping the 4 reserved header bytes the streamWriter
// passes) so the framing can be asserted without a socket. failAt (1-based)
// makes the Nth WritePacket fail, exercising the mid-stream error path.
type capturePW struct {
	packets [][]byte
	failAt  int
	n       int
}

func (c *capturePW) WritePacket(data []byte) error {
	c.n++
	if c.failAt > 0 && c.n == c.failAt {
		return fmt.Errorf("wire write %d failed", c.n)
	}
	c.packets = append(c.packets, append([]byte(nil), data[4:]...))
	return nil
}

// headerPackets is the number of packets a streamWriter header occupies: the
// column-count packet, one column-definition packet per field, and the
// intermediate EOF that terminates the column-definition block (go-mysql never
// negotiates CLIENT_DEPRECATE_EOF, so this EOF is always present).
func headerPackets(cols []string) int { return 1 + len(cols) + 1 }

// assertHeader checks the header packets: the column-count packet (a
// length-encoded int == len(cols), single byte for the small counts here), one
// column-definition packet per field (each carrying the lenenc "def" catalog
// prefix), and the terminating intermediate EOF (0xfe).
func assertHeader(t *testing.T, pw *capturePW, cols []string) {
	t.Helper()
	if len(pw.packets) < headerPackets(cols) {
		t.Fatalf("only %d packets; need at least header (%d)", len(pw.packets), headerPackets(cols))
	}
	if got := int(pw.packets[0][0]); got != len(cols) {
		t.Errorf("column-count packet = %d, want %d", got, len(cols))
	}
	for i := 1; i <= len(cols); i++ {
		p := pw.packets[i]
		if len(p) < 4 || p[0] != 0x03 || string(p[1:4]) != "def" {
			t.Errorf("packet %d is not a column definition (missing def catalog): %v", i, p)
		}
	}
	if eof := pw.packets[1+len(cols)]; len(eof) == 0 || eof[0] != 0xfe {
		t.Errorf("packet %d is not the intermediate EOF (0xfe): %v", 1+len(cols), eof)
	}
}

func TestStreamWriter_HeaderAndRows(t *testing.T) {
	cols := []string{"id", "name", "note"}
	pw := &capturePW{}
	sw := newStreamWriter(pw, cols)

	rows := [][]any{
		{[]byte("1"), []byte("alice"), nil},
		{[]byte("2"), []byte("bob"), []byte("hi")},
	}
	for i, r := range rows {
		if err := sw.writeRow(r); err != nil {
			t.Fatalf("writeRow %d: %v", i, err)
		}
	}
	res, err := sw.finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}

	// finish returns a StreamingDone StreamingSelect result so go-mysql writes
	// ONLY the trailing EOF — the header and rows already went out here.
	if res.Resultset == nil || res.Streaming != mysql.StreamingSelect || !res.StreamingDone {
		t.Fatalf("finish result is not a StreamingDone StreamingSelect: %+v", res)
	}
	if len(res.Fields) != len(cols) {
		t.Errorf("result carries %d fields, want %d", len(res.Fields), len(cols))
	}

	// header (colcount + fields + EOF) + one packet per row.
	wantPackets := headerPackets(cols) + len(rows)
	if len(pw.packets) != wantPackets {
		t.Fatalf("packet count = %d, want %d", len(pw.packets), wantPackets)
	}
	assertHeader(t, pw, cols)

	// Row packets round-trip through the library's own text-protocol parser
	// against the streamer's fields — proving the bytes are real MySQL rows,
	// not just plausible framing.
	for ri, want := range rows {
		payload := pw.packets[headerPackets(cols)+ri]
		vals, err := mysql.RowData(payload).ParseText(sw.fields, nil)
		if err != nil {
			t.Fatalf("row %d ParseText: %v", ri, err)
		}
		if len(vals) != len(cols) {
			t.Fatalf("row %d parsed %d values, want %d", ri, len(vals), len(cols))
		}
		for ci, cell := range want {
			if cell == nil {
				if vals[ci].Type != mysql.FieldValueTypeNull {
					t.Errorf("row %d col %d: want NULL, got type %v", ri, ci, vals[ci].Type)
				}
				continue
			}
			if got := string(vals[ci].AsString()); got != string(cell.([]byte)) {
				t.Errorf("row %d col %d: got %q, want %q", ri, ci, got, cell)
			}
		}
	}
}

// TestStreamWriter_LazyHeader pins that the header is NOT written until the
// first row — the property that lets a merge failing before any row surface as
// a clean first-packet ERR rather than an ERR after a dangling header.
func TestStreamWriter_LazyHeader(t *testing.T) {
	cols := []string{"a", "b"}
	pw := &capturePW{}
	sw := newStreamWriter(pw, cols)

	if sw.wroteHeader || len(pw.packets) != 0 {
		t.Fatalf("header written before any row: wrote=%v packets=%d", sw.wroteHeader, len(pw.packets))
	}
	if err := sw.writeRow([]any{[]byte("1"), []byte("2")}); err != nil {
		t.Fatalf("writeRow: %v", err)
	}
	if !sw.wroteHeader {
		t.Error("wroteHeader still false after first row")
	}
	// header (colcount + fields + EOF) + 1 row
	if got, want := len(pw.packets), headerPackets(cols)+1; got != want {
		t.Errorf("packet count = %d, want %d", got, want)
	}
}

// TestStreamWriter_EmptyStillWritesHeader pins that a zero-row resultset still
// emits its column definitions before the terminating EOF — an EOF with no
// preceding header would be a malformed response.
func TestStreamWriter_EmptyStillWritesHeader(t *testing.T) {
	cols := []string{"x", "y", "z"}
	pw := &capturePW{}
	sw := newStreamWriter(pw, cols)

	res, err := sw.finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !res.StreamingDone || res.Streaming != mysql.StreamingSelect {
		t.Fatalf("finish result not StreamingDone StreamingSelect: %+v", res)
	}
	if got, want := len(pw.packets), headerPackets(cols); got != want {
		t.Fatalf("empty resultset wrote %d packets, want header-only %d", got, want)
	}
	assertHeader(t, pw, cols)
}

// TestStreamWriter_WriteError propagates a wire-write failure so the caller can
// return it (which go-mysql renders as an ERR packet — mid-resultset if rows
// already went out). Two positions: the colcount packet (header, wroteHeader
// stays false) and a row packet (after the header).
func TestStreamWriter_WriteError(t *testing.T) {
	t.Run("header_fails", func(t *testing.T) {
		pw := &capturePW{failAt: 1} // colcount packet
		sw := newStreamWriter(pw, []string{"a"})
		if err := sw.writeRow([]any{[]byte("1")}); err == nil {
			t.Fatal("expected error from writeRow when the header packet fails")
		}
		if sw.wroteHeader {
			t.Error("wroteHeader must stay false when the header packet failed")
		}
	})
	t.Run("row_fails_after_header", func(t *testing.T) {
		cols := []string{"a"}
		pw := &capturePW{failAt: headerPackets(cols) + 1} // first row packet, after header
		sw := newStreamWriter(pw, cols)
		if err := sw.writeRow([]any{[]byte("1")}); err == nil {
			t.Fatal("expected error from writeRow when the row packet fails")
		}
		if !sw.wroteHeader {
			t.Error("header should have been written before the failing row packet")
		}
	})
}

// TestStreamWriter_NullEncoding pins the NULL wire encoding (0xfb) and the
// length-encoded-string encoding for a non-NULL cell.
func TestStreamWriter_NullEncoding(t *testing.T) {
	cols := []string{"a", "b"}
	pw := &capturePW{}
	sw := newStreamWriter(pw, cols)
	if err := sw.writeRow([]any{nil, []byte("hi")}); err != nil {
		t.Fatalf("writeRow: %v", err)
	}
	row := pw.packets[headerPackets(cols)] // after header
	want := append([]byte{0xfb}, mysql.PutLengthEncodedString([]byte("hi"))...)
	if string(row) != string(want) {
		t.Errorf("row payload = %v, want %v (0xfb NULL + lenenc 'hi')", row, want)
	}
}

// An empty string is an empty string on the wire (lenenc 0x00), whatever Go
// type carries it; only nil is NULL. go-mysql's FormatTextValue renders a Go
// "" to a nil slice, which read as NULL here.
func TestStreamWriter_EmptyStringIsNotNull(t *testing.T) {
	cols := []string{"a", "b", "c"}
	pw := &capturePW{}
	sw := newStreamWriter(pw, cols)
	if err := sw.writeRow([]any{"", []byte{}, nil}); err != nil {
		t.Fatalf("writeRow: %v", err)
	}
	row := pw.packets[headerPackets(cols)]
	if want := []byte{0x00, 0x00, 0xfb}; string(row) != string(want) {
		t.Errorf("row payload = % x, want % x (two empty strings, then NULL)", row, want)
	}
}
