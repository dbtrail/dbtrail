package pgshim

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Findings of a review of the PostgreSQL port, each pinned by the case that
// showed it.

// A binary timestamp far from 2000 is decoded to its own date, not to one a
// time.Duration wrapped around to (it holds about 292 years).
func TestDecodeBinaryTimestampFarFromTheEpoch(t *testing.T) {
	be := func(us int64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(us))
		return b
	}
	for _, c := range []struct {
		us   int64
		want string
	}{
		{0, "2000-01-01 00:00:00"},
		{-1, "1999-12-31 23:59:59.999999"},
		{1_500_000, "2000-01-01 00:00:01.5"},
		{-1_500_000, "1999-12-31 23:59:58.5"},
		// 10^16 microseconds is about 317 years: 1683 and 2316.
		{-10_000_000_000_000_000, "1683-02-07 14:13:20"},
		{-10_000_000_000_000_001, "1683-02-07 14:13:19.999999"},
		{10_000_000_000_000_000, "2316-11-24 09:46:40"},
		// The last microsecond of 9999 and the first of year 1.
		{252_455_615_999_999_999, "9999-12-31 23:59:59.999999"},
		{-63_082_281_600_000_000, "0001-01-01 00:00:00"},
	} {
		a, perr := decodeBinaryArg(0, oidTimestamp, be(c.us))
		if perr != nil {
			t.Errorf("us=%d: %s %s", c.us, perr.code, perr.msg)
			continue
		}
		if a.text != c.want {
			t.Errorf("us=%d: got %q, want %q", c.us, a.text, c.want)
		}
		// The same instant, read back the long way.
		if got, err := time.Parse("2006-01-02 15:04:05.999999", a.text); err == nil {
			back := (got.Unix()-pgEpoch.Unix())*1_000_000 + int64(got.Nanosecond()/1000)
			if back != c.us {
				t.Errorf("us=%d: %q reads back as %d", c.us, a.text, back)
			}
		}
	}
	for _, us := range []int64{252_455_616_000_000_000, -63_082_281_600_000_001} {
		if _, perr := decodeBinaryArg(0, oidTimestamp, be(us)); perr == nil || perr.code != "22008" {
			t.Errorf("us=%d: got %v, want 22008", us, perr)
		}
	}
	a, perr := decodeBinaryArg(0, oidTimestamptz, be(-10_000_000_000_000_000))
	if perr != nil || a.text != "1683-02-07 14:13:20+00" {
		t.Errorf("timestamptz: got %q %v", a.text, perr)
	}
}

// LIKE patterns are read by character: a letter written in more than one
// byte matches itself, after a backslash too.
func TestLikeRegexpMultiByte(t *testing.T) {
	for _, c := range []struct {
		pattern, s string
		want       bool
	}{
		{"café", "café", true},
		{"caf_", "café", true},
		{"caf__", "café", false},
		{`caf\é`, "café", true},
		{"%é%", "réservé", true},
		{"日本_", "日本語", true},
	} {
		re, perr := likeRegexp(c.pattern)
		if perr != nil {
			t.Fatalf("%q: %s", c.pattern, perr.msg)
		}
		if got := re.MatchString(c.s); got != c.want {
			t.Errorf("%q LIKE %q = %v, want %v", c.s, c.pattern, got, c.want)
		}
	}
}

// U& is a Unicode escape only where it starts a token and a quote follows
// it, as PostgreSQL's scanner reads it.
func TestForwardRefusalUnicodeEscapeIsAToken(t *testing.T) {
	for _, c := range []struct {
		stmt    string
		refused bool
	}{
		{"SELECT menu&4 FROM t", false},
		{"SELECT flags_mu&1 FROM t", false},
		{"SELECT u&x FROM t", false},
		{"SELECT u & 'x' FROM t", false},
		{"SELECT 'U&''x' FROM t", false},
		{`SELECT "U&'x" FROM t`, false},
		{"SELECT 1 -- U&'x'", false},
		{"SELECT 1 /* /* U&'x' */ */", false},
		{"SELECT $$U&'x'$$", false},
		{`SELECT U&'d\0061t'`, true},
		{`SELECT u&'x'`, true},
		{`SELECT * FROM U&"d\0061t"`, true},
		{`SELECT a&U&'x' FROM t`, true},
		{`SELECT (U&"x")`, true},
	} {
		perr := forwardRefusal(c.stmt)
		refused := perr != nil && strings.Contains(perr.msg, "Unicode escapes")
		if refused != c.refused {
			t.Errorf("%s: refused = %v (%v), want %v", c.stmt, refused, perr, c.refused)
		}
	}
}

// The options start-up parameter is read as PostgreSQL reads it: a dash in
// a name is an underscore after -c too, and --name with no value is an
// error, not an empty value.
func TestParseOptionsAsPostgreSQL(t *testing.T) {
	got, perr := parseOptions("-c statement-timeout=5s -clock-timeout=1s --search-path=a")
	if perr != nil {
		t.Fatal(perr.msg)
	}
	want := [][2]string{{"statement_timeout", "5s"}, {"lock_timeout", "1s"}, {"search_path", "a"}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("option %d: got %v, want %v", i, got[i], want[i])
		}
	}
	for _, v := range []string{"--search_path", "-c search_path", "--=x"} {
		if _, perr := parseOptions(v); perr == nil || perr.code != "42601" {
			t.Errorf("%q: got %v, want 42601", v, perr)
		}
	}
}

// An Execute that ends a SHOW sends SHOW as its tag, as PostgreSQL does and
// as the simple protocol here does, not SHOW 1.
func TestExtendedShowTagHasNoCount(t *testing.T) {
	e := &echoLogin{}
	r := dialRaw(t, e.login)
	tagOf := func(got []string) string {
		for _, m := range got {
			if strings.HasPrefix(m, "CommandComplete ") {
				return m
			}
		}
		return ""
	}
	simple := tagOf(r.send(&pgproto3.Query{String: "SHOW application_name"}))
	ext := tagOf(r.send(&pgproto3.Parse{Query: "SHOW application_name"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}))
	if simple != "CommandComplete SHOW" || ext != simple {
		t.Fatalf("simple %q, extended %q; want both %q", simple, ext, "CommandComplete SHOW")
	}
	// A SELECT still carries this Execute's count.
	if got := tagOf(r.send(&pgproto3.Parse{Query: "SELECT stmt FROM t"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})); got != "CommandComplete SELECT 1" {
		t.Fatalf("SELECT: %q", got)
	}
}
