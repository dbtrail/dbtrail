package shim

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
	"github.com/go-mysql-org/go-mysql/server"
)

// wireConn is a net.Conn that keeps what is written to it.
type wireConn struct {
	net.Conn
	out    bytes.Buffer
	writes int
}

func (c *wireConn) Write(p []byte) (int, error) { c.writes++; return c.out.Write(p) }

// packets returns n packets as go-mysql takes them (4 reserved bytes, then
// the payload): an empty one, small ones, and one too large for a frame.
func testPackets() [][]byte {
	var out [][]byte
	for _, size := range []int{0, 1, 70, 300, mysql.MaxPayloadLen + 10, 5} {
		p := make([]byte, 4+size)
		for i := range size {
			p[4+i] = byte(i)
		}
		out = append(out, p)
	}
	return out
}

// The coalescer puts on the wire the same bytes go-mysql would, packet by
// packet (each framed with its length and the connection's next sequence
// number, a packet too large for one frame split by go-mysql in order), in
// fewer writes, and leaves the sequence where go-mysql would.
func TestCoalescer_writesWhatGoMySQLWould(t *testing.T) {
	ref := &wireConn{}
	refConn := packet.NewConn(ref)
	refConn.Sequence = 250 // wraps past 255 on the way
	for _, p := range testPackets() {
		if err := refConn.WritePacket(append([]byte(nil), p...)); err != nil {
			t.Fatal(err)
		}
	}

	got := &wireConn{}
	pc := packet.NewConn(got)
	pc.Sequence = 250
	c := newCoalescer(&server.Conn{Conn: pc})
	if c == nil {
		t.Fatal("no coalescer for go-mysql's connection")
	}
	for _, p := range testPackets() {
		if err := c.WritePacket(append([]byte(nil), p...)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.out.Bytes(), ref.out.Bytes()) {
		t.Fatalf("the wire differs: %d bytes, go-mysql's %d", got.out.Len(), ref.out.Len())
	}
	if pc.Sequence != refConn.Sequence {
		t.Errorf("sequence %d after, go-mysql's %d", pc.Sequence, refConn.Sequence)
	}
	if got.writes >= ref.writes {
		t.Errorf("%d writes, go-mysql's %d: nothing was gathered", got.writes, ref.writes)
	}
}

// Many small packets go out in writes of about coalesceBytes, not one each.
func TestCoalescer_gathersSmallPackets(t *testing.T) {
	w := &wireConn{}
	c := newCoalescer(&server.Conn{Conn: packet.NewConn(w)})
	const rows = 100_000
	for range rows {
		if err := c.WritePacket(make([]byte, 4+60)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if want := rows * 64 / coalesceBytes; w.writes > want+1 {
		t.Errorf("%d writes for %d packets, want about %d", w.writes, rows, want)
	}
	if w.out.Len() != rows*64 {
		t.Errorf("%d bytes, want %d", w.out.Len(), rows*64)
	}
}

// Only go-mysql's own uncompressed connection is coalesced: compressed
// frames are go-mysql's to build, and a test's capture keeps its packets.
func TestNewCoalescer_onlyForAPlainConnection(t *testing.T) {
	pc := packet.NewConn(&wireConn{})
	pc.Compression = mysql.MYSQL_COMPRESS_ZLIB
	if newCoalescer(&server.Conn{Conn: pc}) != nil {
		t.Error("a compressed connection is coalesced")
	}
	if newCoalescer(&capturePW{}) != nil {
		t.Error("a capture is coalesced")
	}
}

// A statement whose answer is streamed through the coalescer leaves
// nothing in it when HandleQuery returns: go-mysql writes the EOF that
// closes the answer next, and it must follow every row.
func TestHandleQuery_flushesTheWireBeforeReturning(t *testing.T) {
	base, rows := spoolCase()
	f := &fakeFreeSQL{res: base, spoolRows: rows, updatedAt: time.Now(), spoolDir: t.TempDir()}
	w := &wireConn{}
	pc := packet.NewConn(w)
	h := routingHandler(t, &fakeRouter{toCopy: true, reason: "expensive"}, f, time.Minute)
	h.BindConn(&server.Conn{Conn: pc})
	res, err := h.HandleQuery(spoolStmt)
	if err != nil || res == nil || !res.Resultset.StreamingDone {
		t.Fatalf("(%v, %v)", res, err)
	}
	if len(f.spools) != 1 {
		t.Fatal("the answer did not come from a file")
	}
	if n := len(h.wire.buf); n != 0 {
		t.Fatalf("%d bytes left unsent when the statement returned", n)
	}
	// Header (count, 4 definitions, EOF) and 3 rows, in sequence from 0.
	b := w.out.Bytes()
	for i := range 4 + 2 + 3 {
		if len(b) < 4 {
			t.Fatalf("packet %d missing", i)
		}
		n := int(b[0]) | int(b[1])<<8 | int(b[2])<<16
		if int(b[3]) != i {
			t.Fatalf("packet %d has sequence %d", i, b[3])
		}
		b = b[4+n:]
	}
	if len(b) != 0 {
		t.Errorf("%d bytes after the last row", len(b))
	}
}
