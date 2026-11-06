package shim

import (
	"reflect"
	"unsafe"

	"github.com/go-mysql-org/go-mysql/server"
)

// handshakeSalt is the salt conn's client scrambled its password with when it
// logged in: the one the initial handshake sent, or the one an auth switch
// during the handshake replaced it with (go-mysql keeps the latest in the
// same field). nil when it cannot be read.
//
// A change of user asks for the password again with this same salt; MySQL,
// too, keeps the connection's salt for a change of user rather than drawing a
// new one. ProxySQL 3.1.12 (through its libmariadb) was seen answering that
// question with the scramble it computed against the handshake's salt,
// whatever salt the question carried; asked with a fresh salt, every change
// of user it made was refused with 1045. This assumes the client, like
// go-mysql, keeps the latest salt when its login went through an auth switch.
//
// go-mysql keeps the salt unexported and has no getter, so it is read through
// reflection. TestHandshakeSalt_readsTheLibrarysField fails if a go-mysql
// upgrade renames or reshapes the field.
func handshakeSalt(conn *server.Conn) []byte {
	if conn == nil {
		return nil
	}
	f := reflect.ValueOf(conn).Elem().FieldByName("salt")
	if !f.IsValid() || f.Kind() != reflect.Slice || f.Type().Elem().Kind() != reflect.Uint8 {
		return nil
	}
	salt := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Bytes()
	if len(salt) != 20 {
		return nil
	}
	return append([]byte(nil), salt...)
}
