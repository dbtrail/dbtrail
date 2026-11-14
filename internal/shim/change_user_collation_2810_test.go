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

func (f *fakeChanger) ChangeUserCollation(id uint16) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.collation = id
}

// #2810: a change of user that the session accepts tells the handler the
// collation the packet carried, before it binds the new user.
func TestChangeUser_tellsTheCollation_2810(t *testing.T) {
	c, h, _ := sessionPair(t)
	c.ResetSequence()
	if err := c.WritePacket(changeUserPkt("app2")); err != nil { // carries 0x21
		t.Fatal(err)
	}
	sw, err := c.ReadPacket()
	if err != nil || sw[0] != mysql.EOF_HEADER {
		t.Fatalf("no auth switch: %x %v", sw, err)
	}
	salt := sw[1+len(mysql.AUTH_NATIVE_PASSWORD)+1 : len(sw)-1]
	if err := c.WritePacket(append([]byte{0, 0, 0, 0}, mysql.CalcNativePassword(salt, []byte("pw2"))...)); err != nil {
		t.Fatal(err)
	}
	resp, err := c.ReadPacket()
	if err != nil || resp[0] != mysql.OK_HEADER {
		t.Fatalf("client read %q %v, want OK", resp, err)
	}
	h.mu.Lock()
	got := h.collation
	h.mu.Unlock()
	if got != 0x21 {
		t.Errorf("ChangeUserCollation was told %d, want 33", got)
	}
}
