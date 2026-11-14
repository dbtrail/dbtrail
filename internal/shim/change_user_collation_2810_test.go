package shim

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// #2810: the character set number a COM_CHANGE_USER carries after the
// schema, which MySQL gives the session.
func TestParseChangeUser_collation_2810(t *testing.T) {
	const v41 = mysql.CLIENT_SECURE_CONNECTION | mysql.CLIENT_PROTOCOL_41
	for _, c := range []struct {
		name       string
		data       []byte
		capability uint32
		want       uint16
	}{
		{"utf8mb4_general_ci", []byte("app2\x00\x03abcshop\x00\x2d\x00mysql_native_password\x00"), v41, 45},
		{"latin1, no plugin after it", []byte("app2\x00\x00shop\x00\x08\x00"), v41, 8},
		{"a number past 255", []byte("app2\x00\x00\x00\x16\x01"), v41, 278},
		{"one byte only", []byte("app2\x00\x00shop\x00\x08"), v41, 0},
		{"ends after the schema", []byte("app2\x00\x00shop\x00"), v41, 0},
		{"ends after the auth", []byte("app2\x00\x02ab"), v41, 0},
		{"no CLIENT_PROTOCOL_41", []byte("app2\x00\x00shop\x00\x08\x00"), mysql.CLIENT_SECURE_CONNECTION, 0},
	} {
		p, err := parseChangeUser(c.data, c.capability)
		if err != nil || p.collation != c.want {
			t.Errorf("%s: collation %d, %v; want %d", c.name, p.collation, err, c.want)
		}
	}
}
