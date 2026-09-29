package parser

import (
	"strings"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

func TestParseMariaDBPosition(t *testing.T) {
	g := func(d, s uint32, n uint64) gomysql.MariadbGTID {
		return gomysql.MariadbGTID{DomainID: d, ServerID: s, SequenceNumber: n}
	}
	ok := []struct {
		in   string
		want map[uint32]gomysql.MariadbGTID
	}{
		{"0-1-100", map[uint32]gomysql.MariadbGTID{0: g(0, 1, 100)}},
		{"0-1-100,1-2-7", map[uint32]gomysql.MariadbGTID{0: g(0, 1, 100), 1: g(1, 2, 7)}},
		{" 0-1-100 ,\n1-2-7\n", map[uint32]gomysql.MariadbGTID{0: g(0, 1, 100), 1: g(1, 2, 7)}},
		{"4294967295-4294967295-18446744073709551615", map[uint32]gomysql.MariadbGTID{4294967295: g(4294967295, 4294967295, 18446744073709551615)}},
	}
	for _, tc := range ok {
		got, err := ParseMariaDBPosition(tc.in)
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("%q: got %v, want %v", tc.in, got, tc.want)
			continue
		}
		for d, w := range tc.want {
			if got[d] != w {
				t.Errorf("%q: domain %d = %v, want %v", tc.in, d, got[d], w)
			}
		}
	}

	bad := []struct{ in, want string }{
		{"", "empty MariaDB GTID position"},
		{" \n\t", "empty MariaDB GTID position"},
		{"garbage", "invalid"},
		{"0-1-", "invalid"},
		{"0-1", "invalid"},
		{"-1-1-1", "invalid"},
		{"0-1-5,,1-2-3", "empty entry"},
		{"0-1-5,", "empty entry"},
		{",0-1-5", "empty entry"},
		{"3e11fa47-bee9-11e4-9716-8f2e7c74b0e5:1-5", "invalid"},
		{"0-1-100,0-2-90", "twice"},
		{"0-2-90,0-1-100", "twice"},
	}
	for _, tc := range bad {
		got, err := ParseMariaDBPosition(tc.in)
		if err == nil {
			t.Errorf("%q: parsed to %v, want an error", tc.in, got)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: error %q, want it to mention %q", tc.in, err, tc.want)
		}
	}
}
