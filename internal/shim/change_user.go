package shim

import (
	"bytes"
	"crypto/subtle"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// UserChanger is a handler that can move its connection to another user
// (COM_CHANGE_USER) or start it afresh as the same user
// (COM_RESET_CONNECTION). A proxy in front of the port, ProxySQL first among
// them, keeps a pool of backend connections and hands one opened for one
// application to another by changing its user; without this the port
// answered "command 17 is not supported now" and the application got the
// error. A handler that does not implement it keeps that answer.
//
// The session does the protocol and the password check; the handler says
// what authenticates a user and binds the connection. Both decisions come
// from ONE reading: ChangeUserCredential returns the credential and a ticket
// that records what that credential opens, and ChangeUser binds with the
// ticket, never by looking the user up again (the same rule the handshake
// follows).
type UserChanger interface {
	// ChangeUserCredential is what authenticates user now, read once.
	// found=false refuses the change like a wrong password does.
	ChangeUserCredential(user string) (cred server.Credential, found bool, ticket any)
	// ChangeUser binds the connection to user, whose password was checked
	// against the credential that came with ticket, with schema as its
	// default database ("" for none). An error closes the connection.
	ChangeUser(user, schema string, ticket any) error
	// ResetConnection binds the connection afresh to the user it has. An
	// error closes the connection.
	ResetConnection() error
}

// closeAfter is a dispatch answer written before the session closes the
// connection: a change of user that is refused leaves no connection behind,
// neither on the old user nor half way to the new one.
type closeAfter struct{ err error }

// errChangeUserMalformed is a COM_CHANGE_USER packet that cannot be read.
var errChangeUserMalformed = mysql.NewError(mysql.ER_MALFORMED_PACKET, "malformed COM_CHANGE_USER packet")

// changeUserPacket is what the session reads from a COM_CHANGE_USER packet.
// The auth data in it is ignored: it was scrambled with the handshake's
// salt, which the library keeps to itself; the session sends a fresh one
// (an auth switch, which every client that sends COM_CHANGE_USER answers).
type changeUserPacket struct {
	user, schema string
}

func parseChangeUser(data []byte, capability uint32) (changeUserPacket, error) {
	var p changeUserPacket
	user, rest, ok := bytes.Cut(data, []byte{0})
	if !ok || len(user) == 0 {
		return p, errChangeUserMalformed
	}
	p.user = string(user)
	if capability&mysql.CLIENT_SECURE_CONNECTION != 0 {
		if len(rest) < 1 || len(rest) < 1+int(rest[0]) {
			return p, errChangeUserMalformed
		}
		rest = rest[1+int(rest[0]):]
	} else {
		_, after, ok := bytes.Cut(rest, []byte{0})
		if !ok {
			return p, errChangeUserMalformed
		}
		rest = after
	}
	// The schema is NUL-terminated; a client may end the packet right after
	// the auth data when it names none.
	if len(rest) > 0 {
		schema, _, ok := bytes.Cut(rest, []byte{0})
		if !ok {
			return p, errChangeUserMalformed
		}
		p.schema = string(schema)
	}
	return p, nil
}

// changeUser answers COM_CHANGE_USER: it reads the packet, asks the handler
// what authenticates the user, checks the client's answer to a fresh salt,
// and then has the handler bind the connection. Every refusal is a
// closeAfter.
func (s *Session) changeUser(uc UserChanger, data []byte) any {
	p, err := parseChangeUser(data, s.conn.Capability())
	if err != nil {
		return closeAfter{err}
	}
	cred, found, ticket := uc.ChangeUserCredential(p.user)
	plugin := cred.AuthPluginName
	if plugin == "" {
		plugin = mysql.AUTH_NATIVE_PASSWORD
	}
	if plugin != mysql.AUTH_NATIVE_PASSWORD && plugin != mysql.AUTH_CACHING_SHA2_PASSWORD {
		return closeAfter{mysql.NewError(mysql.ER_NOT_SUPPORTED_AUTH_MODE,
			fmt.Sprintf("changing the user of a connection is not supported under %s; open a new connection", plugin))}
	}
	salt := mysql.RandomBuf(20)
	sw := make([]byte, 4, 4+1+len(plugin)+1+len(salt)+1)
	sw = append(sw, mysql.EOF_HEADER)
	sw = append(sw, plugin...)
	sw = append(sw, 0)
	sw = append(sw, salt...)
	sw = append(sw, 0) // as MySQL writes it; the official client needs it
	if err := s.conn.WritePacket(sw); err != nil {
		return closeAfter{err}
	}
	answer, err := s.conn.ReadPacket()
	if err != nil {
		return closeAfter{err}
	}
	if len(answer) == 1 && answer[0] == 0 {
		answer = nil // an empty password, sent as a lone NUL
	}
	denied := mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR,
		fmt.Sprintf("Access denied for user '%s'@'%s' (using password: %s)", p.user, s.conn.RemoteAddr(), yesNo(len(answer) > 0)))
	if !found || !passwordMatches(plugin, salt, answer, cred.Passwords) {
		return closeAfter{denied}
	}
	if plugin == mysql.AUTH_CACHING_SHA2_PASSWORD {
		// Fast authentication succeeded: the client waits for this before
		// the OK.
		if err := s.conn.WritePacket([]byte{0, 0, 0, 0, mysql.MORE_DATE_HEADER, mysql.CACHE_SHA2_FAST_AUTH}); err != nil {
			return closeAfter{err}
		}
	}
	// What the old user prepared is not the new user's (MySQL drops it too).
	s.dropStatements()
	if err := uc.ChangeUser(p.user, p.schema, ticket); err != nil {
		return closeAfter{err}
	}
	return nil
}

// resetConnection answers COM_RESET_CONNECTION.
func (s *Session) resetConnection(uc UserChanger) any {
	s.dropStatements()
	if err := uc.ResetConnection(); err != nil {
		return closeAfter{err}
	}
	return nil
}

// dropStatements forgets every prepared statement and the long data held
// for them.
func (s *Session) dropStatements() {
	clear(s.stmts)
	s.longBytes = 0
}

// passwordMatches checks a client's answer to salt against each password,
// the way MySQL checks it under plugin. Compared in constant time.
func passwordMatches(plugin string, salt, answer []byte, passwords []string) bool {
	for _, pw := range passwords {
		if pw == "" {
			continue // never a passwordless login
		}
		var want []byte
		switch plugin {
		case mysql.AUTH_NATIVE_PASSWORD:
			want = mysql.CalcNativePassword(salt, []byte(pw))
		case mysql.AUTH_CACHING_SHA2_PASSWORD:
			want = mysql.CalcCachingSha2Password(salt, []byte(pw))
		}
		if len(want) > 0 && subtle.ConstantTimeCompare(want, answer) == 1 {
			return true
		}
	}
	return false
}

func yesNo(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}
