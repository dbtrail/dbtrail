package metadata

import (
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// Every row was read from MariaDB 11.8 (HEX(col) and the column's text) and
// is pinned here; the integration test checks the same rules against the
// server over many more values.
var measuredMariaDBFixed = []struct {
	dt, hex, text string
}{
	{"uuid", "123E4567E89B12D3A456426614174000", "123e4567-e89b-12d3-a456-426614174000"},
	{"uuid", "00000000000000000000000000000000", "00000000-0000-0000-0000-000000000000"},
	{"uuid", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
	{"inet4", "0A000000", "10.0.0.0"},
	{"inet4", "00000000", "0.0.0.0"},
	{"inet4", "FFFFFFFF", "255.255.255.255"},
	{"inet4", "7C5C0000", "124.92.0.0"},
	{"inet6", "20010DB8000000000000000000000000", "2001:db8::"},
	{"inet6", "00000000000000000000000000000000", "::"},
	{"inet6", "00000000000000000000FFFF01020304", "::ffff:1.2.3.4"},
	{"inet6", "00000000000000000000000001020304", "::1.2.3.4"},
	{"inet6", "00010000000000000000000000000000", "1::"},
	{"inet6", "00000000000000000000000000000001", "::1"},
	{"inet6", "00010000000000020000000000000003", "1:0:0:2::3"},
	{"inet6", "FE800000000000000001000000000000", "fe80::1:0:0:0"},
	{"inet6", "00010002000300040005000600070008", "1:2:3:4:5:6:7:8"},
	{"inet6", "00000000000000000000FFFF00000000", "::ffff:0.0.0.0"},
	{"inet6", "00000000000000000000000000000100", "::100"},
	{"inet6", "00010000000000000002000000000000", "1::2:0:0:0"},
	{"inet6", "00000000000100000000000000000000", "0:0:1::"},
	{"inet6", "00000000000000000000FFFE01020304", "::fffe:102:304"},
	{"inet6", "00000000000000000000000000000002", "::2"},
	{"inet6", "00010000000200030004000500060007", "1::2:3:4:5:6:7"},
	{"inet6", "00000000000000000000000000010203", "::0.1.2.3"},
	{"inet6", "00010002000300040005000600000000", "1:2:3:4:5:6::"},
	{"inet6", "00000000000000000000000000010000", "::0.1.0.0"},
	{"inet6", "00000000000000000000FFFF00010000", "::ffff:0.1.0.0"},
	{"inet6", "00000000000000000001FFFF00000000", "::1:ffff:0:0"},
	{"inet6", "00010000000000010000000000010001", "1::1:0:0:1:1"},
	{"inet6", "00000000000000000000FFFFFFFFFFFF", "::ffff:255.255.255.255"},
}

func TestFormatMariaDBFixed_measured(t *testing.T) {
	for _, c := range measuredMariaDBFixed {
		b := mustHex(t, c.hex)
		got, ok := FormatMariaDBFixed(c.dt, b)
		if !ok || got != c.text {
			t.Errorf("FormatMariaDBFixed(%s, %s) = %q, %v; want %q", c.dt, c.hex, got, ok, c.text)
		}
		back, err := ParseMariaDBFixed(c.dt, c.text)
		if err != nil || hex.EncodeToString(back) != hex.EncodeToString(b) {
			t.Errorf("ParseMariaDBFixed(%s, %q) = %x, %v; want %x", c.dt, c.text, back, err, b)
		}
		if !IsMariaDBFixedText(c.dt, c.text) {
			t.Errorf("IsMariaDBFixedText(%s, %q) = false", c.dt, c.text)
		}
	}
}

func TestFormatMariaDBFixed_wrongWidthOrType(t *testing.T) {
	for _, c := range []struct {
		dt string
		b  []byte
	}{
		{"uuid", make([]byte, 15)},
		{"uuid", make([]byte, 17)},
		{"inet4", make([]byte, 3)},
		{"inet6", make([]byte, 4)},
		{"binary", make([]byte, 16)},
		{"", make([]byte, 16)},
	} {
		if s, ok := FormatMariaDBFixed(c.dt, c.b); ok {
			t.Errorf("FormatMariaDBFixed(%q, %d bytes) = %q, want refused", c.dt, len(c.b), s)
		}
	}
	if s, ok := FormatMariaDBFixed(" UUID ", make([]byte, 16)); !ok || s != "00000000-0000-0000-0000-000000000000" {
		t.Errorf("data type case/spaces: got %q, %v", s, ok)
	}
}

// What MariaDB 11.8 accepts for CAST('…' AS UUID/INET4/INET6), measured:
// case does not matter, UUID dashes may sit anywhere inside but not at either
// end. Anything the parser refuses is refused out loud, never guessed.
func TestParseMariaDBFixed_inputs(t *testing.T) {
	uuid := "123e4567e89b12d3a456426614174000"
	for _, in := range []string{
		"123E4567-E89B-12D3-A456-426614174000",
		"123e4567e89b12d3a456426614174000",
		"123e-4567-e89b-12d3-a456-4266-1417-4000",
		"123e4567--e89b12d3a456426614174000",
		"1-23e4567e89b12d3a456426614174000",
	} {
		b, err := ParseMariaDBFixed("uuid", in)
		if err != nil || hex.EncodeToString(b) != uuid {
			t.Errorf("uuid %q: %x, %v", in, b, err)
		}
	}
	for _, in := range []string{
		"", "-123e4567e89b12d3a456426614174000", "123e4567e89b12d3a456426614174000-",
		"{123e4567-e89b-12d3-a456-426614174000}", " 123e4567-e89b-12d3-a456-426614174000",
		"123e4567e89b12d3a45642661417400", "123e4567e89b12d3a45642661417400000", "zz3e4567e89b12d3a456426614174000",
	} {
		if b, err := ParseMariaDBFixed("uuid", in); err == nil {
			t.Errorf("uuid %q accepted as %x, want refused", in, b)
		}
	}

	if b, err := ParseMariaDBFixed("inet4", "010.0.0.1"); err != nil || hex.EncodeToString(b) != "0a000001" {
		t.Errorf("inet4 leading zero: %x, %v (MariaDB reads 010 as 10)", b, err)
	}
	for _, in := range []string{"", "1.2.3", "1.2.3.4.5", "256.0.0.0", "10.0.0.1 ", "1..2.3", "0010.0.0.1", "a.b.c.d", "-1.0.0.0"} {
		if b, err := ParseMariaDBFixed("inet4", in); err == nil {
			t.Errorf("inet4 %q accepted as %x, want refused", in, b)
		}
	}

	for in, want := range map[string]string{
		"::FFFF:1.2.3.4":  "00000000000000000000ffff01020304",
		"0:0:0:0:0:0:0:1": "00000000000000000000000000000001",
		"1.2.3.4":         "00000000000000000000ffff01020304",
		"2001:DB8::":      "20010db8000000000000000000000000",
	} {
		if b, err := ParseMariaDBFixed("inet6", in); err != nil || hex.EncodeToString(b) != want {
			t.Errorf("inet6 %q: %x, %v; want %s", in, b, err, want)
		}
	}
	for _, in := range []string{"", "::1%eth0", "1:2:3", ":::", "::1 "} {
		if b, err := ParseMariaDBFixed("inet6", in); err == nil {
			t.Errorf("inet6 %q accepted as %x, want refused", in, b)
		}
	}
	if _, err := ParseMariaDBFixed("varchar", "x"); err == nil {
		t.Error("a non-MariaDB type must be refused")
	}
}

func TestRenderStoredMariaDBFixed(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString
	for _, c := range []struct {
		dt   string
		in   any
		want any
		ok   bool
	}{
		{"uuid", enc(mustHex(t, "123E4567E89B12D3A456426614174000")), "123e4567-e89b-12d3-a456-426614174000", true},
		{"uuid", enc(make([]byte, 16)), "00000000-0000-0000-0000-000000000000", true},
		{"inet4", enc(mustHex(t, "0A000000")), "10.0.0.0", true},
		{"inet6", enc(mustHex(t, "00000000000000000000FFFF01020304")), "::ffff:1.2.3.4", true},
		// Not the stored form of a full-width value: left as it is.
		{"uuid", nil, nil, false},
		{"uuid", 42.0, 42.0, false},
		{"inet4", enc([]byte{10}), enc([]byte{10}), false},                      // short (pre-padding)
		{"uuid", enc(make([]byte, 12)), enc(make([]byte, 12)), false},           // 12 bytes, 16 chars
		{"uuid", "\x12\x3e�g�", "\x12\x3e�g�", false},                           // pre-#1944 raw text
		{"inet4", "\n\x00\x00\x00", "\n\x00\x00\x00", false},                    // pre-#1944 raw bytes
		{"uuid", "!!!!!!!!!!!!!!!!!!!!!!!!", "!!!!!!!!!!!!!!!!!!!!!!!!", false}, // right length, not base64
		{"varchar", enc(make([]byte, 16)), enc(make([]byte, 16)), false},
	} {
		got, ok := RenderStoredMariaDBFixed(c.dt, c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("RenderStoredMariaDBFixed(%s, %#v) = %#v, %v; want %#v, %v", c.dt, c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestIsMariaDBFixedText_rejectsOtherSpellings(t *testing.T) {
	for _, c := range []struct{ dt, s string }{
		{"uuid", "123E4567-E89B-12D3-A456-426614174000"}, // upper case: the server prints lower
		{"uuid", "123e4567e89b12d3a456426614174000"},
		{"inet6", "0:0:0:0:0:0:0:1"},
		{"inet6", "::ffff:0102:0304"},
		{"inet4", "010.0.0.1"},
		{"uuid", "AAAAAAAAAAAAAAAAAAAAAA=="}, // still base64
	} {
		if IsMariaDBFixedText(c.dt, c.s) {
			t.Errorf("IsMariaDBFixedText(%s, %q) = true, want false", c.dt, c.s)
		}
	}
}
