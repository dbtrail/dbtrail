package shim

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/event"
)

// packetWriter is the subset of *server.Conn the streaming full-table path
// needs: writing raw MySQL protocol packets. Declared as an interface (not the
// concrete *server.Conn) so the shim package doesn't take a go-mysql/server
// import here, streamWriter is unit-testable with an in-memory capture, and a
// nil conn on the Handler is an unambiguous "buffer instead of stream" signal.
//
// WritePacket's contract (go-mysql packet.Conn): data must have 4 reserved
// leading bytes; WritePacket fills them with the length+sequence header in
// place and splits payloads larger than the max packet size itself.
type packetWriter interface {
	WritePacket([]byte) error
}

// streamWriter incrementally emits a MySQL text-protocol resultset over a
// connection (#998): the column-count packet, one column-definition packet per
// field, and the intermediate EOF that closes the column-definition block, then
// one packet per row. This mirrors go-mysql/server's own writeFieldList
// byte-for-byte — the intermediate EOF is REQUIRED, because the server never
// advertises CLIENT_DEPRECATE_EOF (server_conf.go), so writeFieldList always
// sends it (see writeHeader). The caller finishes the resultset by returning a
// mysql.Result whose
// Resultset carries Streaming=StreamingSelect and StreamingDone=true, so
// go-mysql writes the trailing EOF and the packet sequence stays continuous.
//
// Header emission is LAZY — writeRow triggers it on the first row, and
// finish() emits it for an empty resultset — so a merge that fails BEFORE
// producing any row leaves nothing on the wire and the error surfaces as a
// clean first-packet ERR. Once ≥1 row (or the header) is out, a later failure
// surfaces as an ERR packet mid-resultset (no terminating EOF), which the
// client reads as an unambiguous query failure rather than a clean short
// result. wroteHeader records which side of that line we are on.
type streamWriter struct {
	conn        packetWriter
	fields      []*mysql.Field
	wroteHeader bool
	rows        int
	buf         []byte // reused per-row packet buffer (4-byte header + payload)
}

// newStreamWriter builds a text-protocol streamer for the given column names.
// Every full-table cell is rendered to text bytes before emission
// (fullTableTextCell / resultsetValue), so VAR_STRING with the utf8 collation
// (33) is the accurate, uniform column type — byte-identical to what
// BuildSimpleTextResultset emits for a []byte/string column
// (fieldType→MYSQL_TYPE_VAR_STRING, formatField→Charset 33). Declaring it
// upfront sidesteps the buffered path's per-first-row type inference, which a
// streamer can't do without a full scan; the trade-off (numeric columns render
// as string-typed rather than typed) is documented on the streaming path.
func newStreamWriter(conn packetWriter, cols []string) *streamWriter {
	fields := make([]*mysql.Field, len(cols))
	for i, c := range cols {
		fields[i] = &mysql.Field{
			Name:    []byte(c),
			Charset: 33,
			Type:    mysql.MYSQL_TYPE_VAR_STRING,
		}
	}
	return &streamWriter{conn: conn, fields: fields, buf: make([]byte, 4, 256)}
}

// newStreamWriterFields is newStreamWriter for a resultset whose column
// metadata is already known (a forwarded MySQL resultset, #2038): the fields
// go out as the source declared them.
func newStreamWriterFields(conn packetWriter, fields []*mysql.Field) *streamWriter {
	return &streamWriter{conn: conn, fields: fields, buf: make([]byte, 4, 256)}
}

// Header and Row make streamWriter a readrouter.RowSink.
func (w *streamWriter) Header(fields []*mysql.Field) error {
	w.fields = fields
	return w.writeHeader()
}

func (w *streamWriter) Row(values []any) error { return w.writeRow(values) }

// writeHeader emits the column-count packet, one packet per column definition,
// and the intermediate EOF that terminates the column-definition block. That
// trailing EOF is REQUIRED: go-mysql/server never advertises
// CLIENT_DEPRECATE_EOF, so its own writeFieldList always sends it, and a client
// (go-sql-driver, the mysql CLI) parses the first row packet as another column
// definition without it. Idempotent via wroteHeader so writeRow and finish can
// both call it safely.
func (w *streamWriter) writeHeader() error {
	if w.wroteHeader {
		return nil
	}
	data := make([]byte, 4, 64)
	data = append(data, mysql.PutLengthEncodedInt(uint64(len(w.fields)))...)
	if err := w.conn.WritePacket(data); err != nil {
		return fmt.Errorf("stream column count: %w", err)
	}
	for _, f := range w.fields {
		data = make([]byte, 4, 128)
		data = append(data, f.Dump()...)
		if err := w.conn.WritePacket(data); err != nil {
			return fmt.Errorf("stream column definition: %w", err)
		}
	}
	// Intermediate EOF (EOF_HEADER + warnings=0 + status). go-sql-driver's
	// readColumns loops until it sees this; the status is not acted on for a
	// single resultset, so AUTOCOMMIT (the shim's steady state) is correct. The
	// TRAILING EOF is written by go-mysql for the StreamingDone result finish()
	// returns, using the connection's real status/warnings.
	eof := make([]byte, 4, 9)
	eof = append(eof, mysql.EOF_HEADER, 0x00, 0x00,
		byte(mysql.SERVER_STATUS_AUTOCOMMIT), byte(mysql.SERVER_STATUS_AUTOCOMMIT>>8))
	if err := w.conn.WritePacket(eof); err != nil {
		return fmt.Errorf("stream column-list EOF: %w", err)
	}
	w.wroteHeader = true
	return nil
}

// writeRow encodes one already-coerced row (each cell a []byte text value or
// nil for NULL) as a text-protocol row packet and writes it, emitting the
// header first if this is the first row. The encoding mirrors
// BuildSimpleTextResultset: a length-encoded string per non-NULL cell, 0xfb
// for NULL — except that an empty Go string is an empty string here, where
// the builder would write NULL.
func (w *streamWriter) writeRow(cells []any) error {
	if err := w.writeHeader(); err != nil {
		return err
	}
	w.buf = w.buf[:4]
	for _, v := range cells {
		b, err := mysql.FormatTextValue(v)
		if err != nil {
			return fmt.Errorf("stream row %d: %w", w.rows, err)
		}
		if s, isString := v.(string); b == nil && isString && s == "" {
			// FormatTextValue renders a Go "" to a nil slice; it is an
			// empty string, not NULL.
			w.buf = append(w.buf, 0x00)
		} else if b == nil {
			w.buf = append(w.buf, 0xfb)
		} else {
			w.buf = append(w.buf, mysql.PutLengthEncodedString(b)...)
		}
	}
	if err := w.conn.WritePacket(w.buf); err != nil {
		return fmt.Errorf("stream row %d: %w", w.rows, err)
	}
	w.rows++
	return nil
}

// finish completes a resultset with no error: it emits the header if no row
// ever did (an empty table must still send its column definitions before the
// terminating EOF, or the client sees a malformed response) and returns the
// StreamingDone sentinel Result that makes go-mysql write the trailing EOF.
func (w *streamWriter) finish() (*mysql.Result, error) {
	if err := w.writeHeader(); err != nil {
		return nil, err
	}
	rs := mysql.NewResultset(0)
	rs.Fields = w.fields
	rs.Streaming = mysql.StreamingSelect
	rs.StreamingDone = true
	return &mysql.Result{Resultset: rs}, nil
}

// projectCell coerces one raw merged-row value to its text wire form for the
// stream, failing loud on a residual unchanged-TOAST marker (#592) exactly as
// the buffered buildImagesResult does — serving the marker's JSON would be
// silently wrong data on the wire. schema/table/column identify the offending
// cell in the error.
func (h *Handler) projectCell(schema, table, column string, raw any) (any, error) {
	if event.IsUnchangedToastMarker(raw) {
		return nil, event.UnresolvedToastError(schema, table, "", []string{column})
	}
	return h.fullTableTextCell(schema, table, column, raw), nil
}
