package shim

import (
	"net"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// RefuseTooManyConnections writes the MySQL wire ERR packet a real
// mysqld sends when max_connections is exhausted (1040 "Too many
// connections"). It is sent INSTEAD of the initial handshake packet:
// the protocol allows an ERR as the server's first packet, and client
// libraries (go-sql-driver, libmysql, ProxySQL) surface it as error
// 1040 rather than a bare TCP reset the operator can't attribute.
// Pre-handshake the CLIENT_PROTOCOL_41 capability is not negotiated
// yet, so the packet omits the '#'+SQLSTATE marker, matching mysqld's
// own pre-handshake ERR format. Caller closes the conn.
func RefuseTooManyConnections(c net.Conn) {
	code := uint16(gomysql.ER_CON_COUNT_ERROR) // 1040
	msg := "Too many connections"
	payload := make([]byte, 0, 3+len(msg))
	payload = append(payload, 0xff, byte(code), byte(code>>8))
	payload = append(payload, msg...)
	pkt := make([]byte, 4, 4+len(payload))
	pkt[0] = byte(len(payload))
	pkt[1] = byte(len(payload) >> 8)
	pkt[2] = byte(len(payload) >> 16)
	pkt[3] = 0 // sequence id of the server's first packet
	pkt = append(pkt, payload...)
	// Best-effort with a short deadline: never let a slow/hostile peer
	// wedge the accept loop's goroutine on the refusal write.
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = c.Write(pkt)
}
