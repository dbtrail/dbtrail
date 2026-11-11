package latin1

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// MySQL 8.0.46, measured for #2634:
//
//	HEX(CONVERT(_latin1 0x818D8F909D USING utf8mb4)) = C281C28DC28FC290C29D
//	HEX(CONVERT(_latin1 0x80E9FF9F   USING utf8mb4)) = E282ACC3A9C3BFC5B8
func TestDecodeMatchesMySQL(t *testing.T) {
	for in, want := range map[string]string{
		"\x81\x8d\x8f\x90\x9d": "\u0081\u008d\u008f\u0090\u009d",
		"\x80\xe9\xff\x9f":     "€éÿŸ",
		"caf\xe9":              "café",
		"plain ascii":          "plain ascii",
		"":                     "",
		"\x00":                 "\x00",
		// Bytes that also happen to be valid UTF-8 are still latin1: C3 A9 is
		// "Ã©", never "é", when the column is latin1.
		"\xc3\xa9": "Ã©",
	} {
		if got := Decode(in); got != want {
			t.Errorf("Decode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEveryByteRoundTrips(t *testing.T) {
	var all strings.Builder
	for b := 0; b < 256; b++ {
		all.WriteByte(byte(b))
	}
	dec := Decode(all.String())
	if !utf8.ValidString(dec) || strings.ContainsRune(dec, utf8.RuneError) {
		t.Fatalf("decoding all 256 bytes gave invalid UTF-8 or U+FFFD: %q", dec)
	}
	back, err := Encode(dec)
	if err != nil || back != all.String() {
		t.Fatalf("Encode(Decode(all bytes)) = %q, %v", back, err)
	}
}

func TestEncodeRefusesWhatLatin1CannotHold(t *testing.T) {
	for _, s := range []string{"日本", "café ☕", "\xff\xfe"} {
		if _, err := Encode(s); err == nil {
			t.Errorf("Encode(%q): want an error, latin1 has no byte for it", s)
		}
	}
}

func TestIsLatin1(t *testing.T) {
	for cs, want := range map[string]bool{"latin1": true, "LATIN1": true, " latin1 ": true, "utf8mb4": false, "": false, "latin2": false} {
		if got := IsLatin1(cs); got != want {
			t.Errorf("IsLatin1(%q) = %v, want %v", cs, got, want)
		}
	}
}

func TestToStored(t *testing.T) {
	for in, want := range map[string]string{"café": "caf\xe9", "caf\xe9": "caf\xe9", "plain": "plain", "": ""} {
		if got, err := ToStored(in); err != nil || got != want {
			t.Errorf("ToStored(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := ToStored("日本"); err == nil {
		t.Error("ToStored(日本): want an error")
	}
}
