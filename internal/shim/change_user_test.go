package shim

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// The COM_CHANGE_USER packet as clients write it: user NUL, auth (length-
// prefixed under CLIENT_SECURE_CONNECTION, else NUL-terminated), schema NUL,
// then charset, plugin and attributes, which the session does not need.
func TestParseChangeUser(t *testing.T) {
	secure := uint32(mysql.CLIENT_SECURE_CONNECTION)
	cases := []struct {
		name         string
		data         []byte
		capability   uint32
		user, schema string
		bad          bool
	}{
		{"full", []byte("app2\x00\x03abcshop\x00\x21\x00mysql_native_password\x00"), secure, "app2", "shop", false},
		{"no schema", []byte("app2\x00\x00\x00"), secure, "app2", "", false},
		{"ends after the auth", []byte("app2\x00\x02ab"), secure, "app2", "", false},
		{"old auth, NUL-terminated", []byte("app2\x00scramble\x00shop\x00"), 0, "app2", "shop", false},
		{"empty", nil, secure, "", "", true},
		{"no user", []byte("\x00\x00"), secure, "", "", true},
		{"no NUL after the user", []byte("app2"), secure, "", "", true},
		{"auth longer than the packet", []byte("app2\x00\xc8x"), secure, "", "", true},
		{"no length byte", []byte("app2\x00"), secure, "", "", true},
		{"schema without its NUL", []byte("app2\x00\x00shop"), secure, "", "", true},
	}
	for _, c := range cases {
		p, err := parseChangeUser(c.data, c.capability)
		if c.bad {
			if err == nil {
				t.Errorf("%s: accepted as %+v", c.name, p)
			}
			continue
		}
		if err != nil || p.user != c.user || p.schema != c.schema {
			t.Errorf("%s: %+v %v, want user %q schema %q", c.name, p, err, c.user, c.schema)
		}
	}
}

func TestPasswordMatches(t *testing.T) {
	salt := []byte("0123456789abcdefghij")
	for _, plugin := range []string{mysql.AUTH_NATIVE_PASSWORD, mysql.AUTH_CACHING_SHA2_PASSWORD} {
		answer := mysql.CalcNativePassword(salt, []byte("pw2"))
		if plugin == mysql.AUTH_CACHING_SHA2_PASSWORD {
			answer = mysql.CalcCachingSha2Password(salt, []byte("pw2"))
		}
		if !passwordMatches(plugin, salt, answer, []string{"pw1", "pw2"}) {
			t.Errorf("%s: the right password did not match", plugin)
		}
		if passwordMatches(plugin, salt, answer, []string{"pw1"}) {
			t.Errorf("%s: a wrong password matched", plugin)
		}
		if passwordMatches(plugin, []byte("another-salt-of-20b!"), answer, []string{"pw2"}) {
			t.Errorf("%s: an answer to another salt matched", plugin)
		}
	}
	// Never a passwordless login: an empty answer matches nothing, an empty
	// password in the list is skipped.
	if passwordMatches(mysql.AUTH_NATIVE_PASSWORD, salt, nil, []string{"", "pw"}) {
		t.Error("an empty answer matched")
	}
	if passwordMatches(mysql.AUTH_SHA256_PASSWORD, salt, []byte("x"), []string{"x"}) {
		t.Error("an unsupported plugin matched")
	}
}

// The standalone shim's handler does not change users: a COM_CHANGE_USER on
// it keeps the answer it had ("not supported").
func TestHandlerIsNotAUserChanger(t *testing.T) {
	if _, ok := any(&Handler{}).(UserChanger); ok {
		t.Fatal("the standalone shim's Handler became a UserChanger")
	}
}
