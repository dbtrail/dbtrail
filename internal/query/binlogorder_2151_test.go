package query

import "testing"

func TestLaterInBinlog(t *testing.T) {
	at := func(file string, pos uint64) *ResultRow { return &ResultRow{BinlogFile: file, StartPos: pos} }
	for _, tc := range []struct {
		name string
		a, b *ResultRow
		want bool
	}{
		{"higher position in one file", at("binlog.000007", 300), at("binlog.000007", 100), true},
		{"lower position in one file", at("binlog.000007", 100), at("binlog.000007", 300), false},
		{"the same position is not later", at("binlog.000007", 100), at("binlog.000007", 100), false},
		{"next file, lower position", at("binlog.000008", 4), at("binlog.000007", 900), true},
		{"previous file, higher position", at("binlog.000007", 900), at("binlog.000008", 4), false},
		{"suffix grew a digit", at("binlog.1000000", 4), at("binlog.999999", 900), true},
		{"suffix grew a digit, reversed", at("binlog.999999", 900), at("binlog.1000000", 4), false},
		{"no coordinates on either", at("", 0), at("", 0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := LaterInBinlog(tc.a, tc.b); got != tc.want {
				t.Fatalf("LaterInBinlog(%s:%d, %s:%d) = %v, want %v",
					tc.a.BinlogFile, tc.a.StartPos, tc.b.BinlogFile, tc.b.StartPos, got, tc.want)
			}
		})
	}
	// The time and the id play no part.
	a, b := at("binlog.000007", 300), at("binlog.000007", 100)
	a.EventID, b.EventID = 1, 2
	b.EventTimestamp = b.EventTimestamp.AddDate(1, 0, 0)
	if !LaterInBinlog(a, b) {
		t.Fatal("a later time or a higher id on b made it later than a")
	}
}
