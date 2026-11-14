package shim

import (
	"bytes"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"sync"

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

// CollationChanger is a UserChanger told the collation a COM_CHANGE_USER
// asks for, before ChangeUser: on MySQL the session takes it, as it took the
// handshake's (#2810). A packet that carries none says nothing.
type CollationChanger interface {
	ChangeUserCollation(id uint16)
}

// ErrChangeUserRefused is what Session.HandleCommand returns when it closed
// the connection after refusing a COM_CHANGE_USER or COM_RESET_CONNECTION.
var ErrChangeUserRefused = errors.New("connection closed after a refused change of user")

// closeAfter is a dispatch answer written before the session closes the
// connection: a change of user that is refused leaves no connection behind,
// neither on the old user nor half way to the new one. err is what the client
// reads; why is for the operator's log only (the client reads the same
// "Access denied" for an unknown user as for a wrong password). lost marks a
// connection that broke during the exchange: nothing was refused.
type closeAfter struct {
	err  error
	why  string
	lost bool
}

// saltFallbackWarned: the warning that the handshake's salt could not be
// read is given once per process.
var saltFallbackWarned sync.Once

// errChangeUserMalformed is a COM_CHANGE_USER packet that cannot be read.
var errChangeUserMalformed = mysql.NewError(mysql.ER_MALFORMED_PACKET, "malformed COM_CHANGE_USER packet")

// changeUserPacket is what the session reads from a COM_CHANGE_USER packet.
// The auth data in it is ignored: the session asks for the password again
// with an auth switch, which a client with CLIENT_PLUGIN_AUTH (required in
// changeUser) answers, and checks only that answer.
type changeUserPacket struct {
	user, schema string
	// collation is the character set number after the schema, 0 when the
	// packet ends before it.
	collation uint16
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
		schema, after, ok := bytes.Cut(rest, []byte{0})
		if !ok {
			return p, errChangeUserMalformed
		}
		p.schema = string(schema)
		// Then the character set, two bytes, when the client sends more.
		if capability&mysql.CLIENT_PROTOCOL_41 != 0 && len(after) >= 2 {
			p.collation = uint16(after[0]) | uint16(after[1])<<8
		}
	}
	return p, nil
}

// changeUser answers COM_CHANGE_USER: it reads the packet, asks the handler
// what authenticates the user, checks the client's answer to an auth switch
// that carries the handshake's salt, and then has the handler bind the
// connection. Every refusal is a closeAfter.
func (s *Session) changeUser(uc UserChanger, data []byte) any {
	p, err := parseChangeUser(data, s.conn.Capability())
	if err != nil {
		return closeAfter{err: err, why: "malformed packet"}
	}
	if s.conn.Capability()&mysql.CLIENT_PLUGIN_AUTH == 0 {
		// The password is checked through an auth switch, which such a
		// client would read as a request for the old (pre-4.1) password.
		return closeAfter{err: mysql.NewError(mysql.ER_NOT_SUPPORTED_AUTH_MODE,
			"changing the user of a connection needs a client that supports authentication plugins; open a new connection"), why: "client without CLIENT_PLUGIN_AUTH"}
	}
	cred, found, ticket := uc.ChangeUserCredential(p.user)
	plugin := cred.AuthPluginName
	if plugin == "" {
		plugin = mysql.AUTH_NATIVE_PASSWORD
	}
	if plugin != mysql.AUTH_NATIVE_PASSWORD && plugin != mysql.AUTH_CACHING_SHA2_PASSWORD {
		return closeAfter{err: mysql.NewError(mysql.ER_NOT_SUPPORTED_AUTH_MODE,
			fmt.Sprintf("changing the user of a connection is not supported under %s; open a new connection", plugin)), why: "unsupported auth plugin " + plugin}
	}
	// The handshake's salt, never a new one (MySQL keeps the connection's
	// salt for a change of user too): ProxySQL answers with the scramble of
	// that salt whatever the switch carries (see handshakeSalt).
	salt := handshakeSalt(s.conn)
	if salt == nil {
		// Unreadable only if a go-mysql upgrade moved the field, which
		// TestHandshakeSalt_readsTheLibrarysField exists to catch first. A
		// fresh salt still serves clients that scramble the switch's salt;
		// ProxySQL's changes would fail with 1045.
		salt = mysql.RandomBuf(20)
		saltFallbackWarned.Do(func() {
			slog.Warn("MySQL port: cannot read the connection's handshake salt from go-mysql (the library changed?); a change of user asks with a fresh salt, which ProxySQL answers wrongly, so its changes of user will be refused with 1045")
		})
	}
	sw := make([]byte, 4, 4+1+len(plugin)+1+len(salt)+1)
	sw = append(sw, mysql.EOF_HEADER)
	sw = append(sw, plugin...)
	sw = append(sw, 0)
	sw = append(sw, salt...)
	sw = append(sw, 0) // as MySQL writes it; the official client needs it
	if err := s.conn.WritePacket(sw); err != nil {
		return closeAfter{err: err, lost: true}
	}
	answer, err := s.conn.ReadPacket()
	if err != nil {
		return closeAfter{err: err, lost: true}
	}
	if len(answer) == 1 && answer[0] == 0 {
		answer = nil // an empty password, sent as a lone NUL
	}
	denied := mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR,
		fmt.Sprintf("Access denied for user '%s'@'%s' (using password: %s)", p.user, s.conn.RemoteAddr(), yesNo(len(answer) > 0)))
	switch {
	case !found:
		return closeAfter{err: denied, why: "no password is accepted for this user (unknown user, or the port is off)"}
	case !passwordMatches(plugin, salt, answer, cred.Passwords):
		return closeAfter{err: denied, why: "wrong password"}
	}
	if plugin == mysql.AUTH_CACHING_SHA2_PASSWORD {
		// Fast authentication succeeded: the client waits for this before
		// the OK.
		if err := s.conn.WritePacket([]byte{0, 0, 0, 0, mysql.MORE_DATE_HEADER, mysql.CACHE_SHA2_FAST_AUTH}); err != nil {
			return closeAfter{err: err, lost: true}
		}
	}
	// What the old user prepared is not the new user's (MySQL drops it too).
	s.dropStatements()
	if cc, ok := uc.(CollationChanger); ok && p.collation != 0 {
		cc.ChangeUserCollation(p.collation)
	}
	if err := uc.ChangeUser(p.user, p.schema, ticket); err != nil {
		return closeAfter{err: err, why: "binding the new user failed"}
	}
	return nil
}

// resetConnection answers COM_RESET_CONNECTION.
func (s *Session) resetConnection(uc UserChanger) any {
	s.dropStatements()
	if err := uc.ResetConnection(); err != nil {
		return closeAfter{err: err, why: "reset refused"}
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
