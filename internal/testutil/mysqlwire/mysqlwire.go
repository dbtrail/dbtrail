// Package mysqlwire is a small MySQL-protocol client for tests that must see
// what a server puts on the wire: the status flags of the handshake, of the
// OK that ends authentication and of every OK and EOF after it. A driver
// hides those; some of them act on them (PyMySQL decides from the handshake
// alone whether to send SET AUTOCOMMIT), so a test of that behaviour needs a
// client that shows them and that sends nothing of its own.
//
// It speaks protocol 4.1 without CLIENT_DEPRECATE_EOF and authenticates with
// mysql_native_password only, which is what the port's tests need.
package mysqlwire

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// Status flags (SERVER_STATUS_flags_enum).
const (
	StatusInTrans            uint16 = 0x0001
	StatusAutocommit         uint16 = 0x0002
	StatusMoreResults        uint16 = 0x0008
	StatusNoGoodIndexUsed    uint16 = 0x0010
	StatusNoIndexUsed        uint16 = 0x0020
	StatusCursorExists       uint16 = 0x0040
	StatusLastRowSent        uint16 = 0x0080
	StatusNoBackslashEscapes uint16 = 0x0200
	StatusInTransReadonly    uint16 = 0x2000
	StatusSessionStateChange uint16 = 0x4000
)

const (
	capLongPassword     uint32 = 1 << 0
	capConnectWithDB    uint32 = 1 << 3
	capProtocol41       uint32 = 1 << 9
	capTransactions     uint32 = 1 << 13
	capSecureConnection uint32 = 1 << 15
	capPluginAuth       uint32 = 1 << 19

	comQuit        = 0x01
	comInitDB      = 0x02
	comQuery       = 0x03
	comPing        = 0x0e
	comStmtPrepare = 0x16
	comStmtExecute = 0x17
	comStmtClose   = 0x19
	comStmtReset   = 0x1a
)

// Handshake is what a server says before the client has said anything.
type Handshake struct {
	ServerVersion string
	Status        uint16
	AuthPlugin    string
	salt          []byte
}

// Error is a server ERR packet.
type Error struct {
	Code    uint16
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("ERROR %d: %s", e.Code, e.Message) }

// Reply is the answer to one command.
type Reply struct {
	// Resultset says the answer was a result set (rows may be zero).
	Resultset bool
	// Status is the status of the packet that ended the answer: the OK, or
	// the EOF after the last row.
	Status uint16
	// Warnings is the warning count of that packet.
	Warnings uint16
	// HeaderStatus is the status of the EOF that closes the column
	// definitions of a result set.
	HeaderStatus uint16
	// Rows are the rows of a text result set, each cell nil for NULL. Rows of
	// a binary result set are counted in RowCount and not decoded.
	Rows     [][]*string
	RowCount int
}

// Conn is one connection. Status is the status of the last packet that
// carried one, the handshake's until then: what a driver that tracks it
// believes the session's state to be.
type Conn struct {
	c   net.Conn
	r   *bufio.Reader
	seq byte

	Handshake Handshake
	// AuthStatus is the status of the OK that ended authentication.
	AuthStatus uint16
	Status     uint16
	// Sent are the statements sent with Exec, in order.
	Sent []string
}

// ReadHandshake connects, reads the server's handshake and hangs up. It needs
// no account.
func ReadHandshake(addr string) (Handshake, error) {
	c, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return Handshake{}, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	conn := &Conn{c: c, r: bufio.NewReader(c)}
	return conn.readHandshake()
}

// Dial connects and authenticates. db may be empty.
func Dial(addr, user, password, db string) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(60 * time.Second))
	conn := &Conn{c: c, r: bufio.NewReader(c)}
	hs, err := conn.readHandshake()
	if err != nil {
		c.Close()
		return nil, err
	}
	conn.Handshake, conn.Status = hs, hs.Status

	caps := capLongPassword | capProtocol41 | capTransactions | capSecureConnection | capPluginAuth
	if db != "" {
		caps |= capConnectWithDB
	}
	pkt := binary.LittleEndian.AppendUint32(nil, caps)
	pkt = binary.LittleEndian.AppendUint32(pkt, 1<<24)
	pkt = append(pkt, 45) // utf8mb4_general_ci
	pkt = append(pkt, make([]byte, 23)...)
	pkt = append(pkt, user...)
	pkt = append(pkt, 0)
	scramble := nativeScramble(hs.salt, password)
	pkt = append(pkt, byte(len(scramble)))
	pkt = append(pkt, scramble...)
	if db != "" {
		pkt = append(pkt, db...)
		pkt = append(pkt, 0)
	}
	pkt = append(pkt, "mysql_native_password"...)
	pkt = append(pkt, 0)
	if err := conn.write(pkt); err != nil {
		c.Close()
		return nil, err
	}
	data, err := conn.read()
	if err != nil {
		c.Close()
		return nil, err
	}
	if len(data) > 0 && data[0] == 0xfe {
		// Auth switch request: plugin name, then new salt.
		rest := data[1:]
		name, rest := cstring(rest)
		if name != "mysql_native_password" {
			c.Close()
			return nil, fmt.Errorf("mysqlwire: server asks for auth plugin %q, only mysql_native_password is spoken", name)
		}
		if n := len(rest); n > 0 && rest[n-1] == 0 {
			rest = rest[:n-1]
		}
		if err := conn.write(nativeScramble(rest, password)); err != nil {
			c.Close()
			return nil, err
		}
		if data, err = conn.read(); err != nil {
			c.Close()
			return nil, err
		}
	}
	rep, err := conn.okOrErr(data)
	if err != nil {
		c.Close()
		return nil, err
	}
	conn.AuthStatus = rep.Status
	return conn, nil
}

// Close hangs up.
func (c *Conn) Close() {
	c.seq = 0
	_ = c.write([]byte{comQuit})
	_ = c.c.Close()
}

// Autocommit reports what the client believes from the last status it saw.
func (c *Conn) Autocommit() bool { return c.Status&StatusAutocommit != 0 }

// SetAutocommitLikePyMySQL does what PyMySQL's Connection.autocommit(want)
// does: it compares want with the autocommit bit of the last status the
// server sent (the handshake's on a fresh connection) and sends
// SET AUTOCOMMIT only when they differ. sent says whether it did.
func (c *Conn) SetAutocommitLikePyMySQL(want bool) (sent bool, err error) {
	if c.Autocommit() == want {
		return false, nil
	}
	v := "0"
	if want {
		v = "1"
	}
	_, err = c.Exec("SET AUTOCOMMIT = " + v)
	return true, err
}

// Exec sends one statement as COM_QUERY.
func (c *Conn) Exec(query string) (Reply, error) {
	c.Sent = append(c.Sent, query)
	return c.command(append([]byte{comQuery}, query...), false)
}

// Ping sends COM_PING.
func (c *Conn) Ping() (Reply, error) { return c.command([]byte{comPing}, false) }

// InitDB sends COM_INIT_DB.
func (c *Conn) InitDB(db string) (Reply, error) {
	return c.command(append([]byte{comInitDB}, db...), false)
}

// Prepare sends COM_STMT_PREPARE and returns the statement id and its
// parameter count.
func (c *Conn) Prepare(query string) (id uint32, params int, err error) {
	c.seq = 0
	if err := c.write(append([]byte{comStmtPrepare}, query...)); err != nil {
		return 0, 0, err
	}
	data, err := c.read()
	if err != nil {
		return 0, 0, err
	}
	if data[0] == 0xff {
		return 0, 0, parseErr(data)
	}
	if len(data) < 12 || data[0] != 0 {
		return 0, 0, errors.New("mysqlwire: malformed COM_STMT_PREPARE answer")
	}
	id = binary.LittleEndian.Uint32(data[1:])
	columns := int(binary.LittleEndian.Uint16(data[5:]))
	params = int(binary.LittleEndian.Uint16(data[7:]))
	for _, n := range []int{params, columns} {
		if n == 0 {
			continue
		}
		for range n {
			if _, err := c.read(); err != nil {
				return 0, 0, err
			}
		}
		if eof, err := c.read(); err != nil {
			return 0, 0, err
		} else if !isEOF(eof) {
			return 0, 0, errors.New("mysqlwire: definitions of a prepared statement not closed by EOF")
		}
	}
	return id, params, nil
}

// Execute sends COM_STMT_EXECUTE for a statement without parameters.
func (c *Conn) Execute(id uint32) (Reply, error) {
	pkt := binary.LittleEndian.AppendUint32([]byte{comStmtExecute}, id)
	pkt = append(pkt, 0, 1, 0, 0, 0)
	return c.command(pkt, true)
}

// ResetStmt sends COM_STMT_RESET.
func (c *Conn) ResetStmt(id uint32) (Reply, error) {
	return c.command(binary.LittleEndian.AppendUint32([]byte{comStmtReset}, id), false)
}

// CloseStmt sends COM_STMT_CLOSE, which has no answer.
func (c *Conn) CloseStmt(id uint32) error {
	c.seq = 0
	return c.write(binary.LittleEndian.AppendUint32([]byte{comStmtClose}, id))
}

func (c *Conn) command(pkt []byte, binaryRows bool) (Reply, error) {
	c.seq = 0
	if err := c.write(pkt); err != nil {
		return Reply{}, err
	}
	data, err := c.read()
	if err != nil {
		return Reply{}, err
	}
	if len(data) == 0 {
		return Reply{}, errors.New("mysqlwire: empty packet")
	}
	if data[0] == 0x00 || data[0] == 0xff {
		return c.okOrErr(data)
	}
	// A result set: the column count, the definitions, EOF, the rows, EOF.
	columns, _ := lenEnc(data)
	rep := Reply{Resultset: true}
	for range columns {
		if _, err := c.read(); err != nil {
			return rep, err
		}
	}
	eof, err := c.read()
	if err != nil {
		return rep, err
	}
	if !isEOF(eof) {
		return rep, errors.New("mysqlwire: column definitions not closed by EOF")
	}
	rep.HeaderStatus = binary.LittleEndian.Uint16(eof[3:])
	c.Status = rep.HeaderStatus
	for {
		row, err := c.read()
		if err != nil {
			return rep, err
		}
		if len(row) > 0 && row[0] == 0xff {
			return rep, parseErr(row)
		}
		if isEOF(row) {
			rep.Warnings = binary.LittleEndian.Uint16(row[1:])
			rep.Status = binary.LittleEndian.Uint16(row[3:])
			c.Status = rep.Status
			return rep, nil
		}
		rep.RowCount++
		if binaryRows {
			continue
		}
		cells := make([]*string, 0, columns)
		for len(row) > 0 {
			if row[0] == 0xfb {
				cells = append(cells, nil)
				row = row[1:]
				continue
			}
			n, size := lenEnc(row)
			s := string(row[size : size+int(n)])
			cells = append(cells, &s)
			row = row[size+int(n):]
		}
		rep.Rows = append(rep.Rows, cells)
	}
}

func (c *Conn) okOrErr(data []byte) (Reply, error) {
	if len(data) == 0 {
		return Reply{}, errors.New("mysqlwire: empty packet")
	}
	switch data[0] {
	case 0xff:
		return Reply{}, parseErr(data)
	case 0x00:
		pos := 1
		_, n := lenEnc(data[pos:])
		pos += n
		_, n = lenEnc(data[pos:])
		pos += n
		if len(data) < pos+4 {
			return Reply{}, errors.New("mysqlwire: short OK packet")
		}
		rep := Reply{Status: binary.LittleEndian.Uint16(data[pos:]), Warnings: binary.LittleEndian.Uint16(data[pos+2:])}
		c.Status = rep.Status
		return rep, nil
	}
	return Reply{}, fmt.Errorf("mysqlwire: unexpected packet 0x%02x", data[0])
}

func (c *Conn) readHandshake() (Handshake, error) {
	data, err := c.read()
	if err != nil {
		return Handshake{}, err
	}
	if len(data) > 0 && data[0] == 0xff {
		return Handshake{}, parseErr(data)
	}
	if len(data) < 1 || data[0] != 10 {
		return Handshake{}, errors.New("mysqlwire: not a protocol 10 handshake")
	}
	var hs Handshake
	var rest []byte
	hs.ServerVersion, rest = cstring(data[1:])
	// connection id (4), salt part 1 (8), filler (1), capabilities low (2),
	// charset (1), status (2), capabilities high (2), salt length (1),
	// reserved (10), salt part 2, plugin name.
	if len(rest) < 4+8+1+2+1+2+2+1+10 {
		return Handshake{}, errors.New("mysqlwire: short handshake")
	}
	hs.salt = append(hs.salt, rest[4:12]...)
	hs.Status = binary.LittleEndian.Uint16(rest[16:])
	saltLen := int(rest[20])
	rest = rest[31:]
	part2 := max(13, saltLen-8)
	if len(rest) < part2 {
		return Handshake{}, errors.New("mysqlwire: short handshake salt")
	}
	hs.salt = append(hs.salt, rest[:part2-1]...)
	hs.AuthPlugin, _ = cstring(rest[part2:])
	return hs, nil
}

func (c *Conn) read() ([]byte, error) {
	var out []byte
	for {
		var head [4]byte
		if _, err := io.ReadFull(c.r, head[:]); err != nil {
			return nil, err
		}
		n := int(head[0]) | int(head[1])<<8 | int(head[2])<<16
		c.seq = head[3] + 1
		buf := make([]byte, n)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return nil, err
		}
		out = append(out, buf...)
		if n < 0xffffff {
			return out, nil
		}
	}
}

func (c *Conn) write(payload []byte) error {
	head := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), c.seq}
	c.seq++
	_, err := c.c.Write(append(head, payload...))
	return err
}

func isEOF(data []byte) bool { return len(data) == 5 && data[0] == 0xfe }

func parseErr(data []byte) *Error {
	e := &Error{}
	if len(data) >= 3 {
		e.Code = binary.LittleEndian.Uint16(data[1:])
		msg := data[3:]
		if len(msg) >= 6 && msg[0] == '#' {
			msg = msg[6:]
		}
		e.Message = string(msg)
	}
	return e
}

func cstring(data []byte) (string, []byte) {
	for i, b := range data {
		if b == 0 {
			return string(data[:i]), data[i+1:]
		}
	}
	return string(data), nil
}

func lenEnc(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}
	switch data[0] {
	case 0xfc:
		return uint64(binary.LittleEndian.Uint16(data[1:])), 3
	case 0xfd:
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16, 4
	case 0xfe:
		return binary.LittleEndian.Uint64(data[1:]), 9
	}
	return uint64(data[0]), 1
}

// nativeScramble is mysql_native_password:
// SHA1(password) XOR SHA1(salt + SHA1(SHA1(password))).
func nativeScramble(salt []byte, password string) []byte {
	if password == "" {
		return nil
	}
	h1 := sha1.Sum([]byte(password))
	h2 := sha1.Sum(h1[:])
	h := sha1.New()
	h.Write(salt)
	h.Write(h2[:])
	h3 := h.Sum(nil)
	for i := range h3 {
		h3[i] ^= h1[i]
	}
	return h3
}
